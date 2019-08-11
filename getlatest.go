package namesys

import (
	"context"
	"errors"
	"time"

	ggio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	pb "github.com/libp2p/go-libp2p-pubsub-router/pb"
)

type getLatestProtocol struct {
	ctx  context.Context
	host host.Host
}

func newGetLatestProtocol(ctx context.Context, host host.Host, getLocal func(key string) ([]byte, error)) *getLatestProtocol {
	p := &getLatestProtocol{ctx, host}

	host.SetStreamHandler(PSGetLatestProto, func(s network.Stream) {
		p.receive(s, getLocal)
	})

	return p
}

func (p *getLatestProtocol) receive(s network.Stream, getLocal func(key string) ([]byte, error)) {
	defer helpers.FullClose(s)

	msg := &pb.RequestLatest{}
	if err := readMsg(p.ctx, s, msg); err != nil {
		log.Infof("error reading request from %s: %s", s.Conn().RemotePeer(), err)
		s.Reset()
		return
	}

	response, err := getLocal(msg.Identifier)
	var respProto pb.RespondLatest

	if err != nil {
		respProto = pb.RespondLatest{Status: pb.RespondLatest_NOT_FOUND}
	} else {
		respProto = pb.RespondLatest{Data: response}
	}

	if err := writeMsg(p.ctx, s, &respProto); err != nil {
		return
	}
}

func (p getLatestProtocol) Get(ctx context.Context, pid peer.ID, key string) ([]byte, error) {
	peerCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	s, err := p.host.NewStream(peerCtx, pid, PSGetLatestProto)
	if err != nil {
		return nil, err
	}
	defer helpers.FullClose(s)

	msg := &pb.RequestLatest{Identifier: key}

	if err := writeMsg(ctx, s, msg); err != nil {
		return nil, err
	}
	s.Close()

	response := &pb.RespondLatest{}
	if err := readMsg(ctx, s, response); err != nil {
		return nil, err
	}

	switch response.Status {
	case pb.RespondLatest_SUCCESS:
		return response.Data, nil
	case pb.RespondLatest_NOT_FOUND:
		return nil, nil
	default:
		return nil, errors.New("get-latest: received unknown status code")
	}
}

func writeMsg(ctx context.Context, s network.Stream, msg proto.Message) error {
	done := make(chan error, 1)
	go func() {
		wc := ggio.NewDelimitedWriter(s)

		if err := wc.WriteMsg(msg); err != nil {
			done <- err
			return
		}

		done <- nil
	}()

	var retErr error
	select {
	case retErr = <-done:
	case <-ctx.Done():
		retErr = ctx.Err()
	}

	if retErr != nil {
		s.Reset()
		log.Infof("error writing response to %s: %s", s.Conn().RemotePeer(), retErr)
	}
	return retErr
}

func readMsg(ctx context.Context, s network.Stream, msg proto.Message) error {
	done := make(chan error, 1)
	go func() {
		r := ggio.NewDelimitedReader(s, 1<<20)
		if err := r.ReadMsg(msg); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		s.Reset()
		return ctx.Err()
	}
}
