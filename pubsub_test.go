package namesys

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/routing"

	bhost "github.com/libp2p/go-libp2p-blankhost"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	record "github.com/libp2p/go-libp2p-record"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
)

func newNetHost(ctx context.Context, t *testing.T) host.Host {
	netw := swarmt.GenSwarm(t)
	bh := bhost.NewBlankHost(netw)
	t.Cleanup(func() {
		err := bh.Close()
		if err != nil {
			panic(err)
		}
	})
	return bh
}

func newNetHosts(ctx context.Context, t *testing.T, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		h := newNetHost(ctx, t)
		out = append(out, h)
	}

	time.Sleep(3 * time.Second)
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

func setupTest(ctx context.Context, t *testing.T) (*PubsubValueStore, []*PubsubValueStore) {
	key := "/namespace/key"

	hosts := newNetHosts(ctx, t, 5)
	vss := make([]*PubsubValueStore, len(hosts))
	for i := 0; i < len(vss); i++ {

		fs, err := pubsub.NewFloodSub(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}

		vss[i], err = NewPubsubValueStore(ctx, hosts[i], fs, testValidator{})
		if err != nil {
			t.Fatal(err)
		}
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

	return pub, vss
}

// tests
func TestEarlyPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 5)

	key := "/namespace/key"
	val := []byte("valid for key 1")

	vss := make([]*PubsubValueStore, len(hosts))
	for i := 0; i < len(vss); i++ {
		fs, err := pubsub.NewFloodSub(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}

		vss[i], err = NewPubsubValueStore(ctx, hosts[i], fs, testValidator{})
		if err != nil {
			t.Fatal(err)
		}
	}

	pub := vss[0]
	vss = vss[1:]

	if err := pub.PutValue(ctx, key, val); err != nil {
		t.Fatal(err)
	}

	for i, vs := range vss {
		connect(t, hosts[i], hosts[i+1])
		if err := vs.Subscribe(key); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for Fetch protocol to retrieve data
	waitForPropagation(ctx, t, vss, key)

	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}
}

func TestPubsubPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub, vss := setupTest(ctx, t)
	defer pub.host.Close()

	key := "/namespace/key"
	key2 := "/namespace/key2"

	val := []byte("valid for key 1")
	err := pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	waitForPropagation(ctx, t, vss, key)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	val = []byte("valid for key 2")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	waitForPropagation(ctx, t, vss, key)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	// Check selector.
	nval := []byte("valid for key 1")
	err = pub.PutValue(ctx, key, nval)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	waitForPropagation(ctx, t, vss, key)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, val)
	}

	// Check validator.
	nval = []byte("valid for key 9999 invalid")
	err = pub.PutValue(ctx, key, nval)
	if err != nil {
		t.Fatal(err)
	}

	// let the flood propagate
	waitForPropagation(ctx, t, vss, key)
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
	waitForPropagation(ctx, t, vss, key2)
	waitForPropagation(ctx, t, vss, key)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key2, nval)
		checkValue(ctx, t, i, vs, key, val)
	}

	// cancel subscriptions
	for _, vs := range vss {
		vs.Cancel(key)
	}
	time.Sleep(time.Millisecond * 100)

	// Get missed value?
	nval = []byte("valid for key 3")
	err = pub.PutValue(ctx, key, nval)
	if err != nil {
		t.Fatal(err)
	}

	// resubscribe
	for _, vs := range vss {
		if err := vs.Subscribe(key); err != nil {
			t.Fatal(err)
		}
	}

	// check that we get the new value
	waitForPropagation(ctx, t, vss, key)
	for i, vs := range vss {
		checkValue(ctx, t, i, vs, key, nval)
	}
}

func TestWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub, vss := setupTest(ctx, t)
	defer pub.host.Close()

	key := "/namespace/key"
	key2 := "/namespace/key2"

	ch, err := vss[1].SearchValue(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	val := []byte("valid for key 1")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	v := string(<-ch)
	if v != "valid for key 1" {
		t.Errorf("got unexpected value: %s", v)
	}

	val = []byte("valid for key 2")
	err = pub.PutValue(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	_, err = vss[1].SearchValue(ctx, key2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = vss[1].Cancel(key2)
	if err.Error() != "key has active subscriptions" {
		t.Fatal("cancel should have failed")
	}

	// let the flood propagate
	time.Sleep(time.Second * 1)

	ch, err = vss[1].SearchValue(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	v = string(<-ch)
	if v != "valid for key 2" {
		t.Errorf("got unexpected value: %s", v)
	}
}

func TestPutMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 5)
	vss := make([]*PubsubValueStore, len(hosts))
	for i := 0; i < len(vss); i++ {
		fs, err := pubsub.NewFloodSub(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}

		vss[i], err = NewPubsubValueStore(ctx, hosts[i], fs, testValidator{})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < len(hosts); i++ {
		connect(t, hosts[0], hosts[i])
	}

	const numRuns = 10
	const numRoutines = 1000 // Note: if changing the numRoutines also change the number of digits in the fmtString
	const fmtString = "%s-%04d"
	const baseKey = "/namespace/key"

	for i := 0; i < numRuns; i++ {
		key := fmt.Sprintf("%s/%d", baseKey, i)
		var eg errgroup.Group
		for j := 0; j < numRoutines; j++ {
			rtNum := j
			eg.Go(func() error {
				return vss[0].PutValue(ctx, key, []byte(fmt.Sprintf(fmtString, key, rtNum)))
			})
		}

		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		finalValue := []byte(fmt.Sprintf(fmtString, key, numRoutines-1))
		for j := 0; j < len(hosts); j++ {
			for {
				v, err := vss[j].GetValue(ctx, key)
				if err != routing.ErrNotFound {
					if err != nil {
						t.Fatal(err)
					}
					if bytes.Equal(v, finalValue) {
						break
					}
				}
				time.Sleep(time.Millisecond * 100)
			}
		}
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

func waitForPropagation(ctx context.Context, t *testing.T, vss []*PubsubValueStore, key string) {
	t.Helper()

	condition := func(ctx context.Context) (bool, error) {
		for _, vs := range vss {
			_, err := vs.GetValue(ctx, key)
			if err == nil {
				continue
			} else if errors.Is(err, routing.ErrNotFound) {
				// Not propogated yet
				return false, nil
			} else {
				// Some other error occured
				return false, err
			}
		}

		// No errors, all done
		return true, nil
	}

	err := waitUntil(ctx, condition, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("[ValueStore] vssolve failed: %v", err)
	}
}

func waitUntil(ctx context.Context, condition func(context.Context) (bool, error), interval time.Duration) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			done, err := condition(ctx)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
