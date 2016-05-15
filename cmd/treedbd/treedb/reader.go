package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/kezhuw/leveldb"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/cache"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type reader struct {
	Now  time.Time
	kbuf bytes.Buffer
}

type keyField struct {
	Key   string
	Field *tree.Field
}

func (r *reader) kprintf(format string, args ...interface{}) []byte {
	r.kbuf.Reset()
	fmt.Fprintf(&r.kbuf, format, args...)
	return r.kbuf.Bytes()
}

func (r *reader) readFullTree(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node) (map[string]interface{}, error) {
	n := len(t.Fields)
	keys := make([]keyField, 0, n)
	for k, f := range t.Fields {
		f.RLock()
		keys = append(keys, keyField{k, f})
	}
	t.Unlock()
	m := make(map[string]interface{}, n)
	for i, kf := range keys {
		value, err := r.readField(ss, t.ID, kf.Key, kf.Field, c.Field(kf.Key))
		if err != nil {
			for _, kf := range keys[i+1:] {
				kf.Field.RUnlock()
			}
			return nil, err
		}
		if value != nil {
			m[kf.Key] = value
		}
	}
	return m, nil
}

func (r *reader) readField(ss *leveldb.Snapshot, id tree.ID, k string, f *tree.Field, c cache.Node) (interface{}, error) {
	if f.Snapshot != nil {
		ss = f.Snapshot.Dup()
		defer ss.Release()
	}
	switch v := f.Value.(type) {
	case nil:
		f.RUnlock()
		return nil, nil
	case error:
		f.RUnlock()
		return nil, v
	case cache.CollectedType:
		f.RUnlock()
		return r.readDiskField(ss, id, k)
	case []byte:
		f.RUnlock()
		return v, nil
	case *tree.Tree:
		v.Lock()
		f.RUnlock()
		return r.ReadTree(ss, v, c)
	default:
		f.RUnlock()
		return nil, fmt.Errorf("unexpected field value type: %s", reflect.TypeOf(v))
	}
}

func (r *reader) readDiskField(ss *leveldb.Snapshot, id tree.ID, key string) (interface{}, error) {
	value, err := ss.Get(r.kprintf("/tree/%d/%s", id, key), nil)
	switch {
	case err == leveldb.ErrNotFound:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		return r.readDiskValue(ss, value)
	}
}

func (r *reader) readDiskValue(ss *leveldb.Snapshot, value []byte) (interface{}, error) {
	switch {
	case len(value) == 0:
		return nil, ErrCorruptedData
	case value[0] == FieldBinary:
		return dupBytes(value[1:]), nil
	case value[0] == FieldTree:
		id, n := binary.Uvarint(value[1:])
		if n <= 0 {
			return nil, ErrCorruptedData
		}
		return r.readDiskTree(ss, tree.ID(id))
	default:
		return nil, ErrCorruptedData
	}
}

func (r *reader) readDiskTree(ss *leveldb.Snapshot, id tree.ID) (map[string]interface{}, error) {
	prefix := r.kprintf("/tree/%d/", id)
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	prefixLen := len(prefix)
	m := make(map[string]interface{})
	for it.Next() {
		value, err := r.readDiskValue(ss, it.Value())
		if err != nil {
			return nil, err
		}
		m[string(it.Key()[prefixLen:])] = value
	}
	return m, it.Err()
}

func (r *reader) readPartialTree(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node) (map[string]interface{}, error) {
	n := len(t.Fields)
	keys := make(map[string]*tree.Field, n)
	defer unlockReadingFields(keys)
	for k, f := range t.Fields {
		f.RLock()
		keys[k] = f
	}
	t.Unlock()

	prefix := r.kprintf("/tree/%d/", t.ID)
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	prefixLen := len(prefix)

	var err error
	var value interface{}
	m := make(map[string]interface{}, n)
	for it.Next() {
		k := string(it.Key()[prefixLen:])
		f, ok := keys[k]
		switch ok {
		case true:
			delete(keys, k)
			value, err = r.readField(ss, t.ID, k, f, c.Field(k))
			if err != nil {
				return nil, err
			}
			if value != nil {
				m[k] = value
			}
		default:
			value, err = r.readDiskValue(ss, it.Value())
			if err != nil {
				return nil, err
			}
			m[k] = value
		}
	}
	return m, it.Err()
}

func unlockReadingFields(keys map[string]*tree.Field) {
	for _, f := range keys {
		f.RUnlock()
	}
}

func (r *reader) ReadTree(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node) (map[string]interface{}, error) {
	switch {
	case t.Full:
		return r.readFullTree(ss, t, c)
	case len(t.Fields) == 0:
		t.Unlock()
		return r.readDiskTree(ss, t.ID)
	default:
		return r.readPartialTree(ss, t, c)
	}
}
