package namesys

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-net"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub-router/pb"
	record "github.com/libp2p/go-libp2p-record"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("pubsub-valuestore")

const OnJoinProto = protocol.ID("/psOnJoin/0.0.1")

type watchGroup struct {
	// Note: this chan must be buffered, see notifyWatchers
	listeners map[chan []byte]struct{}
}

type PubsubValueStore struct {
	ctx context.Context
	ds  ds.Datastore
	ps  *pubsub.PubSub

	host host.Host

	rebroadcastInitialDelay time.Duration
	rebroadcastInterval     time.Duration

	// Map of keys to subscriptions.
	//
	// If a key is present but the subscription is nil, we've bootstrapped
	// but haven't subscribed.
	mx   sync.Mutex
	subs map[string]*pubsub.Subscription

	watchLk  sync.Mutex
	watching map[string]*watchGroup

	Validator record.Validator
}

// KeyToTopic converts a binary record key to a pubsub topic key.
func KeyToTopic(key string) string {
	// Record-store keys are arbitrary binary. However, pubsub requires UTF-8 string topic IDs.
	// Encodes to "/record/base64url(key)"
	return "/record/" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

// NewPubsubPublisher constructs a new Publisher that publishes IPNS records through pubsub.
// The constructor interface is complicated by the need to bootstrap the pubsub topic.
// This could be greatly simplified if the pubsub implementation handled bootstrap itself
func NewPubsubValueStore(ctx context.Context, host host.Host, cr routing.ContentRouting, ps *pubsub.PubSub, validator record.Validator) *PubsubValueStore {
	psValueStore := &PubsubValueStore{
		ctx: ctx,

		ds:                      dssync.MutexWrap(ds.NewMapDatastore()),
		ps:                      ps,
		host:                    host,
		rebroadcastInitialDelay: 100 * time.Millisecond,
		rebroadcastInterval:     time.Second,

		subs:     make(map[string]*pubsub.Subscription),
		watching: make(map[string]*watchGroup),

		Validator: validator,
	}

	host.SetStreamHandler(OnJoinProto, func(s net.Stream) {
		defer net.FullClose(s)

		msgData, err := readBytes(s)
		if err != nil {
			return
		}

		msg := pb.RequestLatest{}
		if msg.Unmarshal(msgData) != nil {
			return
		}

		response, err := psValueStore.getLocal(*msg.Identifier)
		if err != nil {
			return
		}

		if writeBytes(s, response) != nil {
			return
		}
	})
	return psValueStore
}

// Publish publishes an IPNS record through pubsub with default TTL
func (p *PubsubValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	// Record-store keys are arbitrary binary. However, pubsub requires UTF-8 string topic IDs.
	// Encode to "/record/base64url(key)"
	topic := KeyToTopic(key)

	if err := p.Subscribe(key); err != nil {
		return err
	}

	log.Debugf("PubsubPublish: publish value for key", key)
	done := make(chan error, 1)
	go func() {
		done <- p.ps.Publish(topic, value)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PubsubValueStore) isBetter(key string, val []byte) (isBetter bool, isEqual bool) {
	if p.Validator.Validate(key, val) != nil {
		return false, false
	}

	old, err := p.getLocal(key)
	if err != nil {
		// If the old one is invalid, the new one is *always* better.
		return true, false
	}

	// Same record is not better
	if old != nil && bytes.Equal(old, val) {
		return false, true
	}

	i, err := p.Validator.Select(key, [][]byte{val, old})
	return err == nil && i == 0, false
}

func (p *PubsubValueStore) Subscribe(key string) error {
	p.mx.Lock()
	// see if we already have a pubsub subscription; if not, subscribe
	sub := p.subs[key]
	p.mx.Unlock()

	if sub != nil {
		return nil
	}

	topic := KeyToTopic(key)

	// Ignore the error. We have to check again anyways to make sure the
	// record hasn't expired.
	//
	// Also, make sure to do this *before* subscribing.
	myID := p.host.ID()
	_ = p.ps.RegisterTopicValidator(topic, func(ctx context.Context, src peer.ID, msg *pubsub.Message) bool {
		isBetter, isEqual := p.isBetter(key, msg.GetData())

		if src == myID && isEqual {
			return true
		}
		return isBetter
	})

	sub, err := p.ps.Subscribe(topic)
	if err != nil {
		p.mx.Unlock()
		return err
	}

	p.mx.Lock()
	existingSub, _ := p.subs[key]
	if existingSub != nil {
		p.mx.Unlock()
		sub.Cancel()
		return nil
	}

	p.subs[key] = sub
	go p.handleSubscription(sub, key)
	p.mx.Unlock()

	go p.rebroadcast(key)
	log.Debugf("PubsubResolve: subscribed to %s", key)

	return nil
}

func (p *PubsubValueStore) rebroadcast(key string) {
	topic := KeyToTopic(key)
	time.Sleep(p.rebroadcastInitialDelay)

	ticker := time.NewTicker(p.rebroadcastInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			val, err := p.getLocal(key)
			if err == nil {
				_ = p.ps.Publish(topic, val)
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PubsubValueStore) getLocal(key string) ([]byte, error) {
	val, err := p.ds.Get(dshelp.NewKeyFromBinary([]byte(key)))
	if err != nil {
		// Don't invalidate due to ds errors.
		if err == ds.ErrNotFound {
			err = routing.ErrNotFound
		}
		return nil, err
	}

	// If the old one is invalid, the new one is *always* better.
	if err := p.Validator.Validate(key, val); err != nil {
		return nil, err
	}
	return val, nil
}

func (p *PubsubValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if err := p.Subscribe(key); err != nil {
		return nil, err
	}

	return p.getLocal(key)
}

func (p *PubsubValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if err := p.Subscribe(key); err != nil {
		return nil, err
	}

	p.watchLk.Lock()
	defer p.watchLk.Unlock()

	out := make(chan []byte, 1)
	lv, err := p.getLocal(key)
	if err == nil {
		out <- lv
		close(out)
		return out, nil
	}

	wg, ok := p.watching[key]
	if !ok {
		wg = &watchGroup{
			listeners: map[chan []byte]struct{}{},
		}
		p.watching[key] = wg
	}

	proxy := make(chan []byte, 1)

	ctx, cancel := context.WithCancel(ctx)
	wg.listeners[proxy] = struct{}{}

	go func() {
		defer func() {
			cancel()

			p.watchLk.Lock()
			delete(wg.listeners, proxy)

			if _, ok := p.watching[key]; len(wg.listeners) == 0 && ok {
				delete(p.watching, key)
			}
			p.watchLk.Unlock()

			close(out)
		}()

		for {
			select {
			case val, ok := <-proxy:
				if !ok {
					return
				}

				// outCh is buffered, so we just put the value or swap it for the newer one
				select {
				case out <- val:
				case <-out:
					out <- val
				}

				// 1 is good enough
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// GetSubscriptions retrieves a list of active topic subscriptions
func (p *PubsubValueStore) GetSubscriptions() []string {
	p.mx.Lock()
	defer p.mx.Unlock()

	var res []string
	for sub := range p.subs {
		res = append(res, sub)
	}

	return res
}

// Cancel cancels a topic subscription; returns true if an active
// subscription was canceled
func (p *PubsubValueStore) Cancel(name string) (bool, error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.watchLk.Lock()
	if _, wok := p.watching[name]; wok {
		p.watchLk.Unlock()
		return false, fmt.Errorf("key has active subscriptions")
	}
	p.watchLk.Unlock()

	sub, ok := p.subs[name]
	if ok {
		sub.Cancel()
		delete(p.subs, name)
	}

	return ok, nil
}

func (p *PubsubValueStore) handleSubscription(sub *pubsub.Subscription, key string) {
	defer sub.Cancel()

	newMsg := make(chan []byte)
	go func() {
		for {
			data, err := p.handleNewMsgs(sub, key)
			if err != nil {
				close(newMsg)
				return
			}
			newMsg <- data
		}
	}()

	newPeerData := make(chan []byte)
	go func() {
		for {
			data, err := p.handleNewPeer(sub, key)
			if err != nil {
				close(newPeerData)
				return
			}
			newPeerData <- data
		}
	}()

	for {
		var data []byte
		select {
		case data = <-newMsg:
		case data = <-newPeerData:
		}

		if isBetter, _ := p.isBetter(key, data); isBetter {
			err := p.ds.Put(dshelp.NewKeyFromBinary([]byte(key)), data)
			if err != nil {
				log.Warningf("PubsubResolve: error writing update for %s: %s", key, err)
			}
			p.notifyWatchers(key, data)
		}
	}
}

func (p *PubsubValueStore) handleNewMsgs(sub *pubsub.Subscription, key string) ([]byte, error) {
	msg, err := sub.Next(p.ctx)
	if err != nil {
		if err != context.Canceled {
			log.Warningf("PubsubResolve: subscription error in %s: %s", key, err.Error())
		}
		return nil, err
	}
	return msg.GetData(), nil
}

func (p *PubsubValueStore) handleNewPeer(sub *pubsub.Subscription, key string) ([]byte, error) {
	peerEvt, err := sub.NextPeerEvent(p.ctx)
	for {
		if err != nil {
			if err != context.Canceled {
				log.Warningf("PubsubNewPeer: subscription error in %s: %s", key, err.Error())
			}
			return nil, err
		}
		if peerEvt.Type == pubsub.PEER_JOIN {
			break
		}
	}

	peerCtx, _ := context.WithTimeout(p.ctx, time.Second*10)
	s, err := p.host.NewStream(peerCtx, peerEvt.Peer, OnJoinProto)
	if err != nil {
		return nil, err
	}
	defer helpers.FullClose(s)

	msg := pb.RequestLatest{Identifier: &key}
	msgData, err := msg.Marshal()
	if err != nil {
		return nil, err
	}

	if writeBytes(s, msgData) != nil {
		return nil, err
	}

	s.Close()

	response, err := readBytes(s)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (p *PubsubValueStore) notifyWatchers(key string, data []byte) {
	p.watchLk.Lock()
	defer p.watchLk.Unlock()
	sg, ok := p.watching[key]
	if !ok {
		return
	}

	for watcher := range sg.listeners {
		select {
		case <-watcher:
			watcher <- data
		case watcher <- data:
		}
	}
}

const sizeLengthBytes = 8

// readNumBytesFromReader reads a specific number of bytes from a Reader, or returns an error
func readNumBytesFromReader(r io.Reader, numBytes uint64) ([]byte, error) {
	data := make([]byte, numBytes)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return data, err
	} else if uint64(n) != numBytes {
		return data, fmt.Errorf("Could not read full length from stream")
	}
	return data, nil
}

func readBytes(r io.Reader) ([]byte, error) {
	// Protocol: uint64 MessageLength followed by byte[] MarshalledMessage

	sizeData, err := readNumBytesFromReader(r, sizeLengthBytes)
	if err != nil {
		return nil, err
	}

	size := binary.LittleEndian.Uint64(sizeData)
	data, err := readNumBytesFromReader(r, size)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func writeBytes(w io.Writer, data []byte) error {
	size := len(data)

	// Protocol: uint64 MessageLength followed by byte[] MarshalledMessage
	sizeData := make([]byte, sizeLengthBytes)
	binary.LittleEndian.PutUint64(sizeData, uint64(size))

	_, err := w.Write(sizeData)
	if err != nil {
		return err
	}

	_, err = w.Write(data)

	return err
}
