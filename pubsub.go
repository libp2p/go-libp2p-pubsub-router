package namesys

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/ipfs/go-cid"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	record "github.com/libp2p/go-libp2p-record"

	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"
)

var log = logging.Logger("pubsub-valuestore")

const PSGetLatestProto = protocol.ID("/pubsub-get-latest/0.0.1")

type watchGroup struct {
	// Note: this chan must be buffered, see notifyWatchers
	listeners map[chan []byte]struct{}
}

type PubsubValueStore struct {
	ctx context.Context
	ds  ds.Datastore
	cr  routing.ContentRouting
	ps  *pubsub.PubSub

	host      host.Host
	getLatest *getLatestProtocol

	rebroadcastInitialDelay time.Duration
	rebroadcastInterval     time.Duration

	// Map of keys to subscriptions.
	//
	// If a key is present but the subscription is nil, we've bootstrapped
	// but haven't subscribed.
	mx   sync.Mutex
	subs map[string]*pubsub.Subscription

	cancels map[string]context.CancelFunc

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

		cr: cr, // needed for pubsub bootstrap

		ds:                      dssync.MutexWrap(ds.NewMapDatastore()),
		ps:                      ps,
		host:                    host,
		rebroadcastInitialDelay: 100 * time.Millisecond,
		rebroadcastInterval:     time.Minute * 10,

		subs:     make(map[string]*pubsub.Subscription),
		cancels:  make(map[string]context.CancelFunc),
		watching: make(map[string]*watchGroup),

		Validator: validator,
	}

	psValueStore.getLatest = newGetLatestProtocol(ctx, host, psValueStore.getLocal)

	go psValueStore.rebroadcast(ctx)

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

	select {
	case err := <-p.psPublishChannel(topic, value):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// compare compares the input value with the current value.
// Returns 0 if equal, greater than 0 if better, less than 0 if worse
func (p *PubsubValueStore) compare(key string, val []byte) int {
	if p.Validator.Validate(key, val) != nil {
		return -1
	}

	old, err := p.getLocal(key)
	if err != nil {
		// If the old one is invalid, the new one is *always* better.
		return 1
	}

	// Same record is not better
	if old != nil && bytes.Equal(old, val) {
		return 0
	}

	i, err := p.Validator.Select(key, [][]byte{val, old})
	if err == nil && i == 0 {
		return 1
	} else {
		return -1
	}
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
		cmp := p.compare(key, msg.GetData())

		return cmp > 0 || cmp == 0 && src == myID
	})

	sub, err := p.ps.Subscribe(topic)
	if err != nil {
		return err
	}

	p.mx.Lock()
	existingSub, bootstrapped := p.subs[key]
	if existingSub != nil {
		p.mx.Unlock()
		sub.Cancel()
		return nil
	}

	ctx, cancel := context.WithCancel(p.ctx)
	p.cancels[key] = cancel

	p.subs[key] = sub
	go p.handleSubscription(ctx, sub, key, cancel)
	p.mx.Unlock()

	log.Debugf("PubsubResolve: subscribed to %s", key)

	if !bootstrapped {
		// TODO: Deal with publish then resolve case? Cancel behaviour changes.
		go bootstrapPubsub(ctx, p.cr, p.host, topic)
	}

	return nil
}

func (p *PubsubValueStore) rebroadcast(ctx context.Context) {
	time.Sleep(p.rebroadcastInitialDelay)

	ticker := time.NewTicker(p.rebroadcastInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var keys []string
			p.mx.Lock()
			keys = make([]string, 0, len(p.subs))
			for p, _ := range p.subs {
				keys = append(keys, p)
			}
			p.mx.Unlock()
			if len(keys) > 0 {
				for _, k := range keys {
					val, err := p.getLocal(k)
					if err == nil {
						topic := KeyToTopic(k)
						select {
						case <-p.psPublishChannel(topic, val):
						case <-ctx.Done():
							return
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *PubsubValueStore) psPublishChannel(topic string, value []byte) chan error {
	done := make(chan error, 1)
	go func() {
		done <- p.ps.Publish(topic, value)
	}()
	return done
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

	if cancel, ok := p.cancels[name]; ok {
		cancel()
		delete(p.cancels, name)
	}

	return ok, nil
}

func (p *PubsubValueStore) handleSubscription(ctx context.Context, sub *pubsub.Subscription, key string, cancel func()) {
	defer sub.Cancel()
	defer cancel()

	newMsg := make(chan []byte)
	go func() {
		defer close(newMsg)
		for {
			data, err := p.handleNewMsgs(ctx, sub, key)
			if err != nil {
				return
			}
			select {
			case newMsg <- data:
			case <-ctx.Done():
				return
			}
		}
	}()

	newPeerData := make(chan []byte)
	go func() {
		defer close(newPeerData)
		for {
			data, err := p.handleNewPeer(ctx, sub, key)
			if err == nil {
				if data != nil {
					select {
					case newPeerData <- data:
					case <-ctx.Done():
						return
					}
				}
			} else {
				select {
				case <-ctx.Done():
					return
				default:
					log.Errorf("PubsubPeerJoin: error interacting with new peer", err)
				}
			}
		}
	}()

	for {
		var data []byte
		var ok bool
		select {
		case data, ok = <-newMsg:
			if !ok {
				return
			}
		case data, ok = <-newPeerData:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		if p.compare(key, data) > 0 {
			err := p.ds.Put(dshelp.NewKeyFromBinary([]byte(key)), data)
			if err != nil {
				log.Warningf("PubsubResolve: error writing update for %s: %s", key, err)
			}
			p.notifyWatchers(key, data)
		}
	}
}

func (p *PubsubValueStore) handleNewMsgs(ctx context.Context, sub *pubsub.Subscription, key string) ([]byte, error) {
	msg, err := sub.Next(ctx)
	if err != nil {
		if err != context.Canceled {
			log.Warningf("PubsubResolve: subscription error in %s: %s", key, err.Error())
		}
		return nil, err
	}
	return msg.GetData(), nil
}

func (p *PubsubValueStore) handleNewPeer(ctx context.Context, sub *pubsub.Subscription, key string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var pid peer.ID

	for {
		peerEvt, err := sub.NextPeerEvent(ctx)
		if err != nil {
			if err != context.Canceled {
				log.Warningf("PubsubNewPeer: subscription error in %s: %s", key, err.Error())
			}
			return nil, err
		}
		if peerEvt.Type == pubsub.PeerJoin {
			pid = peerEvt.Peer
			break
		}
	}

	return p.getLatest.Get(ctx, pid, key)
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

// rendezvous with peers in the name topic through provider records
// Note: rendezvous/boostrap should really be handled by the pubsub implementation itself!
func bootstrapPubsub(ctx context.Context, cr routing.ContentRouting, host host.Host, name string) {
	// TODO: consider changing this to `pubsub:...`
	topic := "floodsub:" + name
	hash := u.Hash([]byte(topic))
	rz := cid.NewCidV1(cid.Raw, hash)

	go func() {
		err := cr.Provide(ctx, rz, true)
		if err != nil {
			log.Warningf("bootstrapPubsub: error providing rendezvous for %s: %s", topic, err.Error())
		}

		for {
			select {
			case <-time.After(8 * time.Hour):
				err := cr.Provide(ctx, rz, true)
				if err != nil {
					log.Warningf("bootstrapPubsub: error providing rendezvous for %s: %s", topic, err.Error())
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	rzctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	wg := &sync.WaitGroup{}
	for pi := range cr.FindProvidersAsync(rzctx, rz, 10) {
		if pi.ID == host.ID() {
			continue
		}
		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()

			err := host.Connect(ctx, pi)
			if err != nil {
				log.Debugf("Error connecting to pubsub peer %s: %s", pi.ID, err.Error())
				return
			}

			// delay to let pubsub perform its handshake
			time.Sleep(time.Millisecond * 250)

			log.Debugf("Connected to pubsub peer %s", pi.ID)
		}(pi)
	}

	wg.Wait()
}
