package protocol

import (
	"errors"
	"reflect"

	"github.com/golang/protobuf/proto"
)

var ErrNoUnmarshaler = errors.New("treedb/protocol.Decoder: no unmarshaler registered")

type decodeEntry struct {
	Unmarshaler reflect.Type
	Protobuf    reflect.Type
}

type Unmarshaler interface {
	Unmarshal([]byte) error
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Protobuf interface {
	proto.Message
}

type Decoder struct {
	msgs map[int64]decodeEntry
}

func (d *Decoder) Register(id int64, msg interface{}) {
	if msg == nil {
		panic("treedb/protocol.Decoder: nil prototype message value")
	}
	if _, ok := d.msgs[id]; ok {
		panic("treedb/protocol.Decoder: duplicated protocol registered")
	}
	if d.msgs == nil {
		d.msgs = make(map[int64]decodeEntry)
	}
	typ := reflect.TypeOf(msg)
	if typ.Kind() != reflect.Ptr {
		panic("treedb/protocol.Decoder: no pointer type registered")
	}
	switch msg.(type) {
	case Unmarshaler:
		d.msgs[id] = decodeEntry{Unmarshaler: typ.Elem()}
	case Protobuf:
		d.msgs[id] = decodeEntry{Protobuf: typ.Elem()}
	default:
		panic("treedb/protocol.Decoder: unsupported type")
	}
}

func (d *Decoder) Unmarshal(id int64, buf []byte) (interface{}, error) {
	entry, ok := d.msgs[id]
	if !ok {
		return nil, ErrNoUnmarshaler
	}
	switch {
	case entry.Unmarshaler != nil:
		msg := reflect.New(entry.Unmarshaler).Interface().(Unmarshaler)
		err := msg.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		return msg, nil
	default:
		msg := reflect.New(entry.Protobuf).Interface().(proto.Message)
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			return nil, err
		}
		return msg, nil
	}
}
