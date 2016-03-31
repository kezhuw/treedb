package treedb

import (
	"net"
	"runtime"

	"github.com/golang/protobuf/proto"
	"github.com/kezhuw/treedb/protocol"
)

type request struct {
	Cmd   uint32
	Data  []byte
	Reply chan interface{}
}

type broker struct {
	closed   chan interface{}
	requests chan request
}

func (b *broker) finalize() {
	close(b.requests)
}

func newBroker(cap int, conn net.Conn) *broker {
	b := &broker{
		closed:   make(chan interface{}, 1),
		requests: make(chan request, cap),
	}
	runtime.SetFinalizer(b, (*broker).finalize)

	router := newRouter(cap)
	go router.Serve(conn, b.requests, b.closed)
	return b
}

func (b *broker) Request(reply chan interface{}, cmd uint32, msg interface{}) {
	var data []byte
	switch msg := msg.(type) {
	case nil:
	case []byte:
		data = msg
	case protocol.Marshaler:
		buf, err := msg.Marshal()
		if err != nil {
			reply <- err
			return
		}
		data = buf
	case protocol.Protobuf:
		buf, err := proto.Marshal(msg)
		if err != nil {
			reply <- err
			return
		}
		data = buf
	}
	b.requests <- request{Cmd: cmd, Data: data, Reply: reply}
}

func (b *broker) Close() error {
	b.Request(b.closed, 0, nil)
	if err, ok := (<-b.closed).(error); ok {
		return err
	}
	return nil
}
