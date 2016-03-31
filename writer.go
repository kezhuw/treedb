package treedb

import (
	"bufio"
	"io"
	"net"

	"github.com/kezhuw/treedb/protocol"
)

type connWriter struct {
	Err    error
	Errors chan uint32
	Writes chan protocol.Packet
}

func newConnWriter(cap int) *connWriter {
	return &connWriter{
		Errors: make(chan uint32, 16),
		Writes: make(chan protocol.Packet, cap),
	}
}

type writeCloser interface {
	CloseWrite() error
}

func closeWrite(w io.Writer) error {
	if closer, ok := w.(writeCloser); ok {
		closer.CloseWrite()
	}
	return nil
}

func (cw *connWriter) Serve(conn net.Conn) {
	defer close(cw.Errors)
	defer closeWrite(conn)

	w := bufio.NewWriter(conn)
	for pkt := range cw.Writes {
		protocol.WritePacket(w, &pkt)
		err := w.Flush()
		if err != nil {
			cw.Err = err
			cw.Errors <- pkt.Seq
			for pkt := range cw.Writes {
				cw.Errors <- pkt.Seq
			}
			break
		}
	}
}

func (cw *connWriter) Close() {
	close(cw.Writes)
}
