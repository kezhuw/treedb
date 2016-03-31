package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var (
	errBufTooLong      = errors.New("treedb/protocol.HandshakeRequest: buf too long")
	errTooManyVersions = errors.New("treedb/protocol.HandshakeRequest: too many versions")
)

type HandshakeRequest struct {
	Versions []uint32
}

type HandshakeResponse struct {
	Version uint32
}

func (msg *HandshakeRequest) Marshal() ([]byte, error) {
	n := len(msg.Versions)
	if n == 0 || n >= 255 {
		return nil, errTooManyVersions
	}
	var buf bytes.Buffer
	buf.WriteByte(byte(n))
	binary.Write(&buf, binary.LittleEndian, msg.Versions)
	return buf.Bytes(), nil
}

func (msg *HandshakeRequest) Unmarshal(buf []byte) error {
	r := bytes.NewReader(buf)
	n, err := r.ReadByte()
	if err != nil {
		return nil
	}
	versions := make([]uint32, n)
	if binary.Read(r, binary.LittleEndian, versions) != nil {
		return nil
	}
	msg.Versions = versions
	return nil
}

func (msg *HandshakeResponse) Marshal() ([]byte, error) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], msg.Version)
	return buf[:], nil
}

func (msg *HandshakeResponse) Unmarshal(buf []byte) error {
	n := len(buf)
	switch {
	case n < 4:
		return io.ErrUnexpectedEOF
	case n > 4:
		return errBufTooLong
	}
	msg.Version = binary.LittleEndian.Uint32(buf)
	return nil
}
