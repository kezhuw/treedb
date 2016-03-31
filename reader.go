package treedb

import (
	"bufio"
	"io"
	"net"

	"github.com/kezhuw/treedb/protocol"
)

type connReader struct {
	Err   error
	Reads chan protocol.Packet
}

func newConnReader(cap int) *connReader {
	return &connReader{
		Reads: make(chan protocol.Packet, cap),
	}
}

func (cr *connReader) Serve(conn net.Conn) {
	defer close(cr.Reads)
	r := bufio.NewReader(conn)
	var pkt protocol.Packet
	for {
		err := protocol.ReadPacket(r, &pkt)
		if err != nil {
			switch err {
			case io.EOF:
				cr.Err = ErrServerClosed
			default:
				cr.Err = err
			}
			break
		}
		cr.Reads <- pkt
		pkt.Payload = nil
	}
}
