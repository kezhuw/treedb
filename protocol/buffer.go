package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
)

var (
	ErrNil         = errors.New("treedb/protocol: nil value/field")
	ErrInvalidType = errors.New("treedb/protocol: invalid value/field type")
)

type Buffer struct {
	bytes.Buffer
	buf [binary.MaxVarintLen64]byte
}

func (w *Buffer) WriteUvarint(x uint64) error {
	n := binary.PutUvarint(w.buf[:], x)
	w.Write(w.buf[:n])
	return nil
}

func (w *Buffer) WriteVarint(x int64) error {
	n := binary.PutVarint(w.buf[:], x)
	w.Write(w.buf[:n])
	return nil
}

func (w *Buffer) WriteString(s string) error {
	w.WriteUvarint(uint64(len(s)))
	w.Buffer.WriteString(s)
	return nil
}

func (w *Buffer) WriteBytes(b []byte) (int, error) {
	w.WriteUvarint(uint64(len(b)))
	w.Write(b)
	return len(b), nil
}

func (w *Buffer) WriteValue(value interface{}) error {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Invalid:
		return ErrNil
	case reflect.Array, reflect.Slice:
		if rv.Type().Elem().Kind() != reflect.Uint8 {
			break
		}
		w.WriteByte(FieldBinary)
		w.WriteBytes(rv.Bytes())
		return nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			break
		}
		keys := rv.MapKeys()
		w.WriteByte(FieldTree)
		w.WriteVarint(int64(len(keys)))
		for _, key := range keys {
			w.WriteString(key.String())
			err := w.WriteValue(rv.MapIndex(key).Interface())
			if err != nil {
				return err
			}
		}
		w.WriteUvarint(0)
		return nil
	}
	return ErrInvalidType
}

func Marshal(value interface{}) ([]byte, error) {
	var buf Buffer
	err := buf.WriteValue(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
