package namesys

import (
	"bufio"
	"context"
	"io"
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
	host host.Host
}

func newGetLatestProtocol(host host.Host, getLocal func(key string) ([]byte, error)) *getLatestProtocol {
	p := &getLatestProtocol{host}

	host.SetStreamHandler(PSGetLatestProto, func(s network.Stream) {
		p.Receive(s, getLocal)
	})

	return p
}

func (p *getLatestProtocol) Receive(s network.Stream, getLocal func(key string) ([]byte, error)) {
	r := ggio.NewDelimitedReader(s, 1<<20)
	msg := &pb.RequestLatest{}
	if err := r.ReadMsg(msg); err != nil {
		if err != io.EOF {
			s.Reset()
			log.Infof("error reading request from %s: %s", s.Conn().RemotePeer(), err)
		} else {
			// Just be nice. They probably won't read this
			// but it doesn't hurt to send it.
			s.Close()
		}
		return
	}

	response, err := getLocal(*msg.Identifier)
	var respProto pb.RespondLatest

	if err != nil || response == nil {
		nodata := true
		respProto = pb.RespondLatest{Nodata: &nodata}
	} else {
		respProto = pb.RespondLatest{Data: response}
	}

	if err := writeBytes(s, &respProto); err != nil {
		s.Reset()
		log.Infof("error writing response to %s: %s", s.Conn().RemotePeer(), err)
		return
	}
	helpers.FullClose(s)
}

func (p getLatestProtocol) Send(ctx context.Context, pid peer.ID, key string) ([]byte, error) {
	peerCtx, _ := context.WithTimeout(ctx, time.Second*10)
	s, err := p.host.NewStream(peerCtx, pid, PSGetLatestProto)
	if err != nil {
		return nil, err
	}

	if err := s.SetDeadline(time.Now().Add(time.Second * 5)); err != nil {
		return nil, err
	}

	defer helpers.FullClose(s)

	msg := pb.RequestLatest{Identifier: &key}

	if err := writeBytes(s, &msg); err != nil {
		s.Reset()
		return nil, err
	}

	s.Close()

	r := ggio.NewDelimitedReader(s, 1<<20)
	response := &pb.RespondLatest{}
	if err := r.ReadMsg(response); err != nil {
		return nil, err
	}

	return response.Data, nil
}

func writeBytes(w io.Writer, msg proto.Message) error {
	bufw := bufio.NewWriter(w)
	wc := ggio.NewDelimitedWriter(bufw)

	if err := wc.WriteMsg(msg); err != nil {
		return err
	}

	return bufw.Flush()
}
