package treedb

import (
	"errors"
	"net"

	"github.com/kezhuw/treedb/protocol"
	"github.com/kezhuw/treedb/v0"
)

type router struct {
	r *connReader
	w *connWriter
}

func newRouter(cap int) *router {
	ru := &router{}
	ru.r = newConnReader(cap)
	ru.w = newConnWriter(cap)
	return ru
}

func unmarshalPacket(pkt *protocol.Packet) interface{} {
	if pkt.Cmd == protocol.ERROR {
		return errors.New("treedb: TODO error response")
	}
	return pkt.Payload
}

func replyPacket(s *session, seq uint32, cmd uint32, payload []byte) {
	reply := s.Unblock(seq)
	if reply == nil {
		return
	}
	go replyResponse(reply, cmd, payload)
}

func replyResponse(reply chan interface{}, cmd uint32, payload []byte) {
	msg, err := v0.Decoder.Unmarshal(int64(cmd), payload)
	if err != nil {
		reply <- err
		return
	}
	switch msg := msg.(type) {
	case *protocol.OkResponse:
		reply <- nil
	case *protocol.ErrorResponse:
		reply <- &protocol.Error{Code: int(msg.Code), Info: msg.Info}
	default:
		reply <- msg
	}
}

func drainRequests(reqs chan request, err error) {
	for req := range reqs {
		req.Reply <- err
	}
}

func waitResponses(s *session, reads chan protocol.Packet, errors chan uint32, err error) {
	for reads != nil || errors != nil {
		select {
		case pkt, ok := <-reads:
			if !ok {
				reads = nil
				break
			}
			replyPacket(s, pkt.Seq, pkt.Cmd, pkt.Payload)
		case seq, ok := <-errors:
			if !ok {
				errors = nil
				break
			}
			s.Reply(seq, err)
		}
	}
	s.ReplyAll(err)
}

func (ru *router) Serve(conn net.Conn, requests chan request, closed chan interface{}) {
	defer conn.Close()
	go ru.r.Serve(conn)
	go ru.w.Serve(conn)

	r := ru.r
	w := ru.w
	s := newSession(128)
	reads := r.Reads
	errors := w.Errors
	for {
		select {
		case req, ok := <-requests:
			switch {
			case !ok:
				w.Close()
				waitResponses(s, reads, errors, ErrClientClosed)
				return
			case req.Reply == closed:
				req.Reply <- nil
				go drainRequests(requests, ErrClientClosed)
				w.Close()
				waitResponses(s, reads, errors, ErrClientClosed)
				return
			default:
				var pkt protocol.Packet
				pkt.Seq = s.Block(req.Reply)
				pkt.Cmd = req.Cmd
				pkt.Payload = req.Data
				w.Writes <- pkt
			}
		case pkt, ok := <-reads:
			switch {
			case !ok:
				go drainRequests(requests, r.Err)
				go waitResponses(s, nil, errors, r.Err)
				return
			default:
				replyPacket(s, pkt.Seq, pkt.Cmd, pkt.Payload)
			}
		case seq := <-errors:
			s.Reply(seq, w.Err)
			go drainRequests(requests, w.Err)
			w.Close()
			waitResponses(s, reads, errors, w.Err)
			return
		}
	}
}
