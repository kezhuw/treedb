package protocol

import (
	"encoding/binary"
	"io"
)

const (
	PacketHeadSize = 16
)

type Packet struct {
	Seq     uint32
	Cmd     uint32
	Payload []byte
}

func ReadPacket(r io.Reader, pkt *Packet) error {
	var head [PacketHeadSize]byte
	n, err := io.ReadFull(r, head[:])
	if err != nil {
		if err == io.ErrUnexpectedEOF && n == 0 {
			err = io.EOF
		}
		return err
	}
	var payload []byte
	if len := binary.LittleEndian.Uint64(head[8:]); len != 0 {
		payload = make([]byte, len)
		_, err := io.ReadFull(r, payload)
		if err != nil {
			return err
		}
		pkt.Payload = payload
	}
	pkt.Seq = binary.LittleEndian.Uint32(head[:4])
	pkt.Cmd = binary.LittleEndian.Uint32(head[4:])
	return nil
}

func WritePacket(w io.Writer, pkt *Packet) error {
	var head [PacketHeadSize]byte
	binary.LittleEndian.PutUint32(head[:4], uint32(pkt.Seq))
	binary.LittleEndian.PutUint32(head[4:], uint32(pkt.Cmd))
	n := len(pkt.Payload)
	binary.LittleEndian.PutUint64(head[8:], uint64(n))
	_, err := w.Write(head[:])
	if err != nil {
		return err
	}
	if n != 0 {
		_, err = w.Write(pkt.Payload)
	}
	return err
}
