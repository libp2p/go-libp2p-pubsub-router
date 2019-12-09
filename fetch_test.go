package namesys

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
)

func connect(t *testing.T, a, b host.Host) {
	pinfo := a.Peerstore().PeerInfo(a.ID())
	err := b.Connect(context.Background(), pinfo)
	if err != nil {
		t.Fatal(err)
	}
}

type datastore struct {
	data map[string][]byte
}

func (d *datastore) Lookup(key string) ([]byte, error) {
	v, ok := d.data[key]
	if !ok {
		return nil, errors.New("key not found")
	}
	return v, nil
}

func TestFetchProtocolTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{map[string][]byte{"key": []byte("value1")}}
	h1 := newFetchProtocol(ctx, hosts[0], d1.Lookup)

	d2 := &datastore{map[string][]byte{"key": []byte("value2")}}
	h2 := newFetchProtocol(ctx, hosts[1], d2.Lookup)

	fetchCheck(ctx, t, h1, h2, "key", []byte("value2"))
	fetchCheck(ctx, t, h2, h1, "key", []byte("value1"))
}

func TestFetchProtocolNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{map[string][]byte{"key": []byte("value1")}}
	h1 := newFetchProtocol(ctx, hosts[0], d1.Lookup)

	d2 := &datastore{make(map[string][]byte)}
	h2 := newFetchProtocol(ctx, hosts[1], d2.Lookup)

	fetchCheck(ctx, t, h1, h2, "key", nil)
	fetchCheck(ctx, t, h2, h1, "key", []byte("value1"))
}

func TestFetchProtocolRepeated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{map[string][]byte{"key": []byte("value1")}}
	h1 := newFetchProtocol(ctx, hosts[0], d1.Lookup)

	d2 := &datastore{make(map[string][]byte)}
	h2 := newFetchProtocol(ctx, hosts[1], d2.Lookup)

	for i := 0; i < 10; i++ {
		fetchCheck(ctx, t, h1, h2, "key", nil)
		fetchCheck(ctx, t, h2, h1, "key", []byte("value1"))
	}
}

func fetchCheck(ctx context.Context, t *testing.T,
	requester *fetchProtocol, responder *fetchProtocol, key string, expected []byte) {
	data, err := requester.Fetch(ctx, responder.host.ID(), key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, expected) {
		t.Fatalf("expected: %v, received: %v", string(expected), string(data))
	}

	if (data == nil && expected != nil) || (data != nil && expected == nil) {
		t.Fatalf("expected []byte{} or nil and received the opposite")
	}
}
