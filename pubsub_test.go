package namesys

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	floodsub "github.com/libp2p/go-floodsub"
	bhost "github.com/libp2p/go-libp2p-blankhost"
	p2phost "github.com/libp2p/go-libp2p-host"
	record "github.com/libp2p/go-libp2p-record"
	routing "github.com/libp2p/go-libp2p-routing"
	rhelper "github.com/libp2p/go-libp2p-routing-helpers"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
)

func newNetHost(ctx context.Context, t *testing.T) p2phost.Host {
	netw := swarmt.GenSwarm(t, ctx)
	return bhost.NewBlankHost(netw)
}

func newNetHosts(ctx context.Context, t *testing.T, n int) []p2phost.Host {
	var out []p2phost.Host

	for i := 0; i < n; i++ {
		h := newNetHost(ctx, t)
		out = append(out, h)
	}

	return out
}

type testValidator struct{}

func (testValidator) Validate(key string, value []byte) error {
	ns, k, err := record.SplitKey(key)
	if err != nil {
		return err
	}
	if ns != "namespace" {
		return record.ErrInvalidRecordType
	}
	if !bytes.Contains(value, []byte(k)) {
		return record.ErrInvalidRecordType
	}
	if bytes.Contains(value, []byte("invalid")) {
		return record.ErrInvalidRecordType
	}
	return nil

}

func (testValidator) Select(key string, vals [][]byte) (int, error) {
	if len(vals) == 0 {
		panic("selector with no values")
	}
	var best []byte
	idx := 0
	for i, val := range vals {
		if bytes.Compare(best, val) < 0 {
			best = val
			idx = i
		}
	}
	return idx, nil
}

// tests
func TestPubsubPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := "/namespace/key"
	key2 := "/namespace/key2"

	hosts := newNetHosts(ctx, t, 5)
	vss := make([]*PubsubValueStore, len(hosts))
	for i := 0; i < len(vss); i++ {

		fs, err := floodsub.NewFloodSub(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}

		vss[i] = NewPubsubValueStore(ctx, hosts[i], rhelper.Null{}, fs, testValidator{})
	}
	pub := vss[0]
	vss = vss[1:]

	pubinfo := hosts[0].Peerstore().PeerInfo(hosts[0].ID())
	for _, h := range hosts[1:] {
		if err := h.Connect(ctx, pubinfo); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Millisecond * 100)
	for i, vs := range vss {
		checkNotFound(ctx, t, i, vs, key)
		// delay to avoid connection storms
		time.Sleep(time.Millisecond * 100)
	}

	// let the bootstrap finish
	time.Sleep(time.Second * 1)

	val := []byte("valid for key 1")
	err := pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	time.Sleep(time.Second * 1)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	val = []byte("valid for key 2")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	time.Sleep(time.Second * 1)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	// Check selector.
	nval := []byte("valid for key 1")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	time.Sleep(time.Second * 1)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	// Check validator.
	nval = []byte("valid for key 9999 invalid")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	time.Sleep(time.Second * 1)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	// Different key?

	// subscribe to the second key
	for i, vs := range vss {
		checkNotFound(ctx, t, i, vs, key2)
	}
	time.Sleep(time.Second * 1)

	// Put to the second key
	nval = []byte("valid for key2")
	err = pub.PutValue(ctx, key2, nval)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	time.Sleep(time.Second * 1)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key2, nval)
		checkValue(ctx, t, i, vs, key, val)
	}

	// cancel subscriptions
	for _, vs := range vss {
		vs.Cancel(key)
	}
	time.Sleep(time.Millisecond * 100)

	nval = []byte("valid for key 3")
	err = pub.PutValue(ctx, key, nval)
	if err != nil {
		t.Fatal(err)
	}

	// check we still have the old value in the vssolver
	time.Sleep(time.Second * 1)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}
}

func TestWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := "/namespace/key"

	hosts := newNetHosts(ctx, t, 5)
	vss := make([]*PubsubValueStore, len(hosts))
	for i := 0; i < len(vss); i++ {

		fs, err := floodsub.NewFloodSub(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}

		vss[i] = NewPubsubValueStore(ctx, hosts[i], rhelper.Null{}, fs, testValidator{})
	}
	pub := vss[0]
	vss = vss[1:]

	pubinfo := hosts[0].Peerstore().PeerInfo(hosts[0].ID())
	for _, h := range hosts[1:] {
		if err := h.Connect(ctx, pubinfo); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Millisecond * 100)
	for i, vs := range vss {
		checkNotFound(ctx, t, i, vs, key)
		// delay to avoid connection storms
		time.Sleep(time.Millisecond * 100)
	}

	// let the bootstrap finish
	time.Sleep(time.Second * 1)

	ch := pub.Watch(key)
	var watched []byte
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		defer wg.Done()
		for v := range ch {
			watched = v
		}
	}()

	err := pub.Subscribe(key)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	val := []byte("valid for key 1")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 1)
	pub.Cancel(key)
	wg.Wait()
	if !bytes.Equal(val, watched) {
		t.Fatal("should have watched the update")
	}
}

func checkNotFound(ctx context.Context, t *testing.T, i int, vs routing.ValueStore, key string) {
	t.Helper()
	_, err := vs.GetValue(ctx, key)
	if err != routing.ErrNotFound {
		t.Fatalf("[vssolver %d] unexpected error: %s", i, err.Error())
	}
}

func checkValue(ctx context.Context, t *testing.T, i int, vs routing.ValueStore, key string, val []byte) {
	t.Helper()
	xval, err := vs.GetValue(ctx, key)
	if err != nil {
		t.Fatalf("[ValueStore %d] vssolve failed: %s", i, err.Error())
	}
	if !bytes.Equal(xval, val) {
		t.Fatalf("[ValueStore %d] unexpected value: expected '%s', got '%s'", i, val, xval)
	}
}
