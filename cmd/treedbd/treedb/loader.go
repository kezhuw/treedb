package treedb

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/cache"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type loader struct {
	Now    time.Time
	Wait   *sync.WaitGroup
	Memory *int64
}

func (l *loader) LoadEmptyTree(ss leveldb.Snapshot, c cache.Node, t *tree.Tree) {
	defer l.Wait.Done()
	defer ss.Close()
	defer t.Unlock()
	prefix := bprintf("/tree/%d/", t.ID)
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	memory := 0
	for it.Next() {
		k := string(it.Key()[len(prefix):])
		f := &tree.Field{Touch: l.Now}
		memory += tree.FieldSize
		value, bytes, err := l.LoadFieldValue(ss, c.Field(k), it.Value())
		switch err {
		case nil:
			f.Value = value
			memory += bytes
		default:
			f.Value = err
		}
		t.Fields[k] = f
	}
	t.Full = true
	atomic.AddInt64(l.Memory, int64(memory))
}

func (l *loader) LoadPartialTree(ss leveldb.Snapshot, c cache.Node, t *tree.Tree) {
	defer l.Wait.Done()
	defer ss.Close()
	defer t.Unlock()
	prefix := bprintf("/tree/%d/", t.ID)
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	memory := 0
	for it.Next() {
		k := string(it.Key()[len(prefix):])
		f, ok := t.Fields[k]
		switch {
		case ok == false:
			f = &tree.Field{Touch: l.Now}
			f.Lock()
			t.Fields[k] = f
		default:
			f.Touch = l.Now
			if f.Value == cache.CollectedObject {
				f.Lock()
				break
			}
			continue
		}
		value, bytes, err := l.LoadFieldValue(ss, c.Field(k), it.Value())
		switch err {
		case nil:
			f.Value = value
			memory += bytes
		default:
			f.Value = err
		}
		f.Unlock()
	}
	t.Full = true
	atomic.AddInt64(l.Memory, int64(memory))
}

func (l *loader) LoadFieldValue(ss leveldb.Snapshot, c cache.Node, value []byte) (interface{}, int, error) {
	switch {
	case len(value) == 0:
		return nil, 0, ErrCorruptedData
	case value[0] == FieldTree:
		id, n := binary.Uvarint(value[1:])
		if n <= 0 {
			return nil, 0, ErrCorruptedData
		}
		t := tree.NewTree(tree.ID(id))
		if c.Duration() != 0 {
			t.Lock()
			l.Wait.Add(1)
			go l.LoadEmptyTree(ss.Dup(), c, t)
		}
		return t, tree.TreeSize, nil
	case value[0] == FieldBinary:
		return dupBytes(value[1:]), cache.SizeofBytes(value[1:]), nil
	default:
		return nil, 0, ErrCorruptedData
	}
}
