package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	ErrExtraBytes        = errors.New("treedb/protocol: extra bytes remains")
	ErrInvalidUnreadByte = errors.New("treedb/protocol: invalid use of UnreadByte")
)

type Reader struct {
	off int
	buf []byte
}

func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf}
}

func (r *Reader) Bytes() []byte {
	return r.buf[r.off:]
}

func (r *Reader) ReadByte() (byte, error) {
	if r.off >= len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	c := r.buf[r.off]
	r.off++
	return c, nil
}

func (r *Reader) UnreadByte() error {
	r.off--
	return nil
}

func (r *Reader) ReadVarint() (int64, error) {
	return binary.ReadVarint(r)
}

func (r *Reader) ReadUvarint() (uint64, error) {
	return binary.ReadUvarint(r)
}

func (r *Reader) ReadBytes() ([]byte, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	pos := r.off
	if pos+int(n) > len(r.buf) {
		return r.buf[pos:], io.ErrUnexpectedEOF
	}
	r.off += int(n)
	return r.buf[pos:r.off], nil
}

func (r *Reader) ReadString() (string, error) {
	b, err := r.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *Reader) readValue() (interface{}, error) {
	typ, err := r.ReadByte()
	switch {
	case err != nil:
		return nil, err
	case typ == FieldTree:
		return r.readTree()
	case typ == FieldBinary:
		return r.ReadBytes()
	default:
		return nil, fmt.Errorf("treedb/protocol: invalid value type: %d", typ)
	}
}

func (r *Reader) readTree() (map[string]interface{}, error) {
	n, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}
	var fields map[string]interface{}
	switch {
	case n > 0:
		fields = make(map[string]interface{}, n)
	default:
		fields = make(map[string]interface{})
	}
	for {
		key, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		if len(key) == 0 {
			break
		}
		val, err := r.readValue()
		if err != nil {
			return nil, err
		}
		fields[string(key)] = val
	}
	return fields, nil
}

func Unmarshal(buf []byte) (interface{}, error) {
	r := Reader{buf: buf}
	val, err := r.readValue()
	if err != nil {
		return nil, err
	}
	switch len(r.Bytes()) {
	case 0:
		return val, nil
	default:
		return val, ErrExtraBytes
	}
}
