package namesys

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"

	pb "github.com/libp2p/go-libp2p-pubsub-router/pb"
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

func TestGetLatestProtocolTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{map[string][]byte{"key": []byte("value1")}}
	h1 := newGetLatestProtocol(ctx, hosts[0], d1.Lookup)

	d2 := &datastore{map[string][]byte{"key": []byte("value2")}}
	h2 := newGetLatestProtocol(ctx, hosts[1], d2.Lookup)

	getLatest(t, ctx, h1, h2, "key", []byte("value2"))
	getLatest(t, ctx, h2, h1, "key", []byte("value1"))
}

func TestGetLatestProtocolNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{map[string][]byte{"key": []byte("value1")}}
	h1 := newGetLatestProtocol(ctx, hosts[0], d1.Lookup)

	d2 := &datastore{make(map[string][]byte)}
	h2 := newGetLatestProtocol(ctx, hosts[1], d2.Lookup)

	getLatest(t, ctx, h1, h2, "key", nil)
	getLatest(t, ctx, h2, h1, "key", []byte("value1"))
}

func TestGetLatestProtocolErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{make(map[string][]byte)}
	h1 := newGetLatestProtocol(ctx, hosts[0], d1.Lookup)

	// bad send protocol to force an error
	s, err := hosts[1].NewStream(ctx, h1.host.ID(), PSGetLatestProto)
	if err != nil {
		t.Fatal(err)
	}
	defer helpers.FullClose(s)

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, ^uint64(0))
	if _, err := s.Write(buf); err != nil {
		t.Fatal(err)
	}

	response := &pb.RespondLatest{}
	if err := readMsg(ctx, s, response); err != nil {
		t.Fatal(err)
	}

	if response.Status != pb.RespondLatest_ERR {
		t.Fatal("should have received an error")
	}
}

func TestGetLatestProtocolRepeated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := newNetHosts(ctx, t, 2)
	connect(t, hosts[0], hosts[1])

	// wait for hosts to get connected
	time.Sleep(time.Millisecond * 100)

	d1 := &datastore{map[string][]byte{"key": []byte("value1")}}
	h1 := newGetLatestProtocol(ctx, hosts[0], d1.Lookup)

	d2 := &datastore{make(map[string][]byte)}
	h2 := newGetLatestProtocol(ctx, hosts[1], d2.Lookup)

	for i := 0; i < 10; i++ {
		getLatest(t, ctx, h1, h2, "key", nil)
		getLatest(t, ctx, h2, h1, "key", []byte("value1"))
	}
}

func getLatest(t *testing.T, ctx context.Context,
	requester *getLatestProtocol, responder *getLatestProtocol, key string, expected []byte) {
	data, err := requester.Get(ctx, responder.host.ID(), key)
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
