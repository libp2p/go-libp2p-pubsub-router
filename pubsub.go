package namesys

import (
	"bytes"
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"
	floodsub "github.com/libp2p/go-floodsub"
	p2phost "github.com/libp2p/go-libp2p-host"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	record "github.com/libp2p/go-libp2p-record"
	routing "github.com/libp2p/go-libp2p-routing"
	ropts "github.com/libp2p/go-libp2p-routing/options"
)

var log = logging.Logger("pubsub-valuestore")

type PubsubValueStore struct {
	ctx  context.Context
	ds   ds.Datastore
	host p2phost.Host
	cr   routing.ContentRouting
	ps   *floodsub.PubSub

	// Map of keys to subscriptions.
	//
	// If a key is present but the subscription is nil, we've bootstrapped
	// but haven't subscribed.
	mx   sync.Mutex
	subs map[string]*floodsub.Subscription

	watchMux sync.RWMutex
	watch    map[string]*watchChannels

	Validator record.Validator
}

type watchChannels struct {
	mux      sync.RWMutex
	channels []chan []byte
}

// NewPubsubPublisher constructs a new Publisher that publishes IPNS records through pubsub.
// The constructor interface is complicated by the need to bootstrap the pubsub topic.
// This could be greatly simplified if the pubsub implementation handled bootstrap itself
func NewPubsubValueStore(ctx context.Context, host p2phost.Host, cr routing.ContentRouting, ps *floodsub.PubSub, validator record.Validator) *PubsubValueStore {
	return &PubsubValueStore{
		ctx:       ctx,
		ds:        dssync.MutexWrap(ds.NewMapDatastore()),
		host:      host, // needed for pubsub bootstrap
		cr:        cr,   // needed for pubsub bootstrap
		ps:        ps,
		Validator: validator,
		subs:      make(map[string]*floodsub.Subscription),
		watch:     make(map[string]*watchChannels),
	}
}

// Publish publishes an IPNS record through pubsub with default TTL
func (p *PubsubValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...ropts.Option) error {
	p.mx.Lock()
	_, bootstraped := p.subs[key]

	if !bootstraped {
		p.subs[key] = nil
		p.mx.Unlock()

		bootstrapPubsub(p.ctx, p.cr, p.host, key)
	} else {
		p.mx.Unlock()
	}

	log.Debugf("PubsubPublish: publish value for key", key)
	return p.ps.Publish(key, value)
}

func (p *PubsubValueStore) isBetter(key string, val []byte) bool {
	if p.Validator.Validate(key, val) != nil {
		return false
	}

	old, err := p.getLocal(key)
	if err != nil {
		// If the old one is invalid, the new one is *always* better.
		return true
	}

	// Same record. Possible DoS vector, should consider failing?
	if bytes.Equal(old, val) {
		return true
	}

	i, err := p.Validator.Select(key, [][]byte{val, old})
	return err == nil && i == 0
}

func (p *PubsubValueStore) Subscribe(key string) error {
	p.mx.Lock()
	// see if we already have a pubsub subscription; if not, subscribe
	sub := p.subs[key]
	p.mx.Unlock()

	if sub != nil {
		return nil
	}

	// Ignore the error. We have to check again anyways to make sure the
	// record hasn't expired.
	//
	// Also, make sure to do this *before* subscribing.
	p.ps.RegisterTopicValidator(key, func(ctx context.Context, msg *floodsub.Message) bool {
		return p.isBetter(key, msg.GetData())
	})

	sub, err := p.ps.Subscribe(key)
	if err != nil {
		p.mx.Unlock()
		return err
	}

	p.mx.Lock()
	existingSub, bootstraped := p.subs[key]
	if existingSub != nil {
		p.mx.Unlock()
		sub.Cancel()
		return nil
	}

	p.subs[key] = sub
	ctx, cancel := context.WithCancel(p.ctx)
	go p.handleSubscription(sub, key, cancel)
	p.mx.Unlock()

	log.Debugf("PubsubResolve: subscribed to %s", key)

	if !bootstraped {
		// TODO: Deal with publish then resolve case? Cancel behaviour changes.
		go bootstrapPubsub(ctx, p.cr, p.host, key)
	}
	return nil
}

func (p *PubsubValueStore) Watch(key string) <-chan []byte {
	p.watchMux.Lock()
	defer p.watchMux.Unlock()
	wChs, ok := p.watch[key]
	if !ok {
		wChs = &watchChannels{
			channels: make([]chan []byte, 0),
		}
		p.watch[key] = wChs
	}
	newCh := make(chan []byte)
	wChs.mux.Lock()
	wChs.channels = append(wChs.channels, newCh)
	wChs.mux.Unlock()
	return newCh
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

func (p *PubsubValueStore) GetValue(ctx context.Context, key string, opts ...ropts.Option) ([]byte, error) {
	if err := p.Subscribe(key); err != nil {
		return nil, err
	}

	return p.getLocal(key)
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
func (p *PubsubValueStore) Cancel(name string) bool {
	p.mx.Lock()
	defer p.mx.Unlock()

	sub, ok := p.subs[name]
	if ok {
		sub.Cancel()
		delete(p.subs, name)
	}

	p.cancelWatchers(name)

	return ok
}

func (p *PubsubValueStore) handleSubscription(sub *floodsub.Subscription, key string, cancel func()) {
	defer sub.Cancel()
	defer cancel()

	for {
		msg, err := sub.Next(p.ctx)
		if err != nil {
			if err != context.Canceled {
				log.Warningf("PubsubResolve: subscription error in %s: %s", key, err.Error())
			}
			return
		}
		if p.isBetter(key, msg.GetData()) {
			err := p.ds.Put(dshelp.NewKeyFromBinary([]byte(key)), msg.GetData())
			if err != nil {
				log.Warningf("PubsubResolve: error writing update for %s: %s", key, err)
			}
			p.notifyWatchers(key, msg.GetData())
		}
	}
}

func (p *PubsubValueStore) notifyWatchers(key string, data []byte) {
	p.watchMux.RLock()
	watchChannels, ok := p.watch[key]
	if !ok {
		p.watchMux.RUnlock()
		return
	}
	watchChannels.mux.RLock()
	p.watchMux.RUnlock()

	defer watchChannels.mux.RUnlock()
	for _, ch := range watchChannels.channels {
		select {
		case ch <- data:
		default:
		}
	}
}

func (p *PubsubValueStore) cancelWatchers(key string) {
	p.watchMux.Lock()
	defer p.watchMux.Unlock()
	watchChannels, ok := p.watch[key]
	if !ok {
		return
	}

	watchChannels.mux.Lock()
	for _, ch := range watchChannels.channels {
		close(ch)
	}
	watchChannels.mux.Unlock()
	delete(p.watch, key)
}

// rendezvous with peers in the name topic through provider records
// Note: rendezvous/boostrap should really be handled by the pubsub implementation itself!
func bootstrapPubsub(ctx context.Context, cr routing.ContentRouting, host p2phost.Host, name string) {
	topic := "floodsub:" + name
	hash := u.Hash([]byte(topic))
	rz := cid.NewCidV1(cid.Raw, hash)

	err := cr.Provide(ctx, rz, true)
	if err != nil {
		log.Warningf("bootstrapPubsub: error providing rendezvous for %s: %s", topic, err.Error())
	}

	go func() {
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
		go func(pi pstore.PeerInfo) {
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
