package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kezhuw/leveldb"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/cache"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type walker struct {
	DB           *DB
	Now          time.Time
	Reply        chan interface{}
	buf          bytes.Buffer
	wait         sync.WaitGroup
	result       interface{}
	memory       int
	memoryAtomic int64
	loader       loader
}

func (w *walker) Start(callback reflect.Value, reading bool, cmd Command) {
	db := w.DB
	switch reading {
	case true:
		db.reads.Add(1)
	default:
		db.writes.Add(1)
	}
	_, ss := db.latestVersion()
	db.root.Lock()
	go w.handle(callback, reading, ss, db.root, db.getCacheRoot(), cmd)
}

func (w *walker) handle(callback reflect.Value, reading bool, ss *leveldb.Snapshot, t *tree.Tree, c cache.Node, cmd Command) {
	w.loader.Now = w.Now
	w.loader.Wait = &w.wait
	w.loader.Memory = &w.memoryAtomic
	callback.Call([]reflect.Value{
		reflect.ValueOf(w),
		reflect.ValueOf(ss),
		reflect.ValueOf(t),
		reflect.ValueOf(c),
		reflect.ValueOf(cmd),
	})
	w.wait.Wait()
	if reply := w.Reply; reply != nil {
		reply <- w.result
	}
	db := w.DB
	if memory := w.memory + int(w.memoryAtomic); memory != 0 {
		db.addMemory(memory)
	}
	switch reading {
	case true:
		db.reads.Done()
	default:
		db.writes.Done()
	}
}

func (w *walker) getField(ss *leveldb.Snapshot, f *tree.Field, c cache.Node, cmd *GetCommand) {
	switch v := f.Value.(type) {
	case nil:
		f.Unlock()
		w.result = &NotFoundError{cmd.Path.Full}
	case error:
		f.Unlock()
		w.result = v
	case []byte:
		f.Unlock()
		switch cmd.Type {
		case FieldAny, FieldBinary:
			w.result = v
		default:
			w.result = ErrMismatchedType
		}
	case *tree.Tree:
		v.Lock()
		f.Unlock()
		if cmd.Type != FieldAny && cmd.Type != FieldTree {
			v.Unlock()
			w.result = ErrMismatchedType
			break
		}
		var r reader
		r.Now = w.Now
		m, err := r.ReadTree(ss, v, c)
		if err != nil {
			w.result = err
			return
		}
		w.result = m
	}
}

func (w *walker) setField(t *tree.Tree, i int, f *tree.Field, cmd *SetCommand) {
	var b writer
	switch v := f.Value.(type) {
	case error:
		f.Unlock()
		w.result = v
		return
	case *tree.Tree:
		b.DeleteTree(v)
	case []byte:
		b.RewriteBinary(v)
	}
	b.Now = w.Now
	value, err := b.WriteValue(w.DB, t.ID, cmd.Path.Segs[i], cmd.Path.Segs[i+1:], cmd.Value)
	if err != nil {
		f.Unlock()
		w.result = err
		return
	}
	if err := w.DB.commit(&b); err != nil {
		f.Unlock()
		w.result = err
		return
	}
	f.Value = value
	f.Version, f.Snapshot = w.DB.commitVersion()
	f.Unlock()
	w.result = nil
}

func (w *walker) deleteField(ss *leveldb.Snapshot, t *tree.Tree, k string, f *tree.Field, cmd *DeleteCommand) {
	var batch writer
	switch v := f.Value.(type) {
	default:
		f.Unlock()
		w.result = v
		return
	case []byte:
		batch.DeleteBinary(t.ID, k, v)
	case *tree.Tree:
		batch.DeleteTree(t)
	}
	err := w.DB.commit(&batch)
	if err != nil {
		f.Unlock()
		w.result = err
		return
	}
	f.Value = nil
	f.Version, f.Snapshot = w.DB.commitVersion()
	f.Unlock()
	w.result = nil
}

func (w *walker) loadTree(ss *leveldb.Snapshot, t *tree.Tree) {
	defer w.wait.Done()
	defer ss.Release()
	defer t.Unlock()
	prefix := bprintf("/tree/%d/", t.ID)
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	memory := 0
	for it.Next() {
		f := &tree.Field{Touch: w.Now}
		memory += tree.FieldSize
		value := it.Value()
		switch {
		case len(value) == 0:
			f.Value = errors.New("zero length value ...")
		case value[0] == FieldTree:
			id, n := binary.Uvarint(value[1:])
			if n <= 0 {
				f.Value = fmt.Errorf("unable to read tree id from blob: 0x%X", value)
				break
			}
			subTree := tree.NewTree(tree.ID(id))
			w.wait.Add(1)
			subTree.Lock()
			go w.loadTree(ss.Dup(), subTree)
			f.Value = subTree
			memory += tree.TreeSize
		case value[0] == FieldBinary:
			f.Value = dupBytes(value[1:])
			memory += cache.SizeofBytes(value)
		default:
			f.Value = fmt.Errorf("invalid blob: 0x%X", value)
		}
		k := string(it.Key()[len(prefix):])
		t.Fields[k] = f
	}
	t.Full = true
	atomic.AddInt64(&w.memoryAtomic, int64(memory))
}

func (w *walker) loadField(ss *leveldb.Snapshot, id tree.ID, k string, f *tree.Field, c cache.Node) {
	w.buf.Reset()
	fmt.Fprintf(&w.buf, "/tree/%d/%s", id, k)
	value, err := ss.Get(w.buf.Bytes(), nil)
	switch {
	case err == leveldb.ErrNotFound:
		f.Value = nil
	case err != nil:
		f.Value = err
	default:
		value, memory, err := w.loader.LoadFieldValue(ss, c.Field(k), value)
		if err != nil {
			f.Value = err
			break
		}
		f.Value = value
		w.memory += memory
	}

}

func (w *walker) handleGetCommand(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node, cmd *GetCommand) {
	defer ss.Release()
	for i, k := range cmd.Path.Segs {
		c = c.Field(k)
		f := t.Fields[k]
		switch f {
		case nil:
			if t.Full {
				t.Unlock()
				w.result = &NotFoundError{cmd.Path.Sub(i)}
				return
			}
			f = t.EnterNewField(k)
			w.memory += tree.FieldSize
			w.loadField(ss, t.ID, k, f, c)
		default:
			f.Lock()
			t.Unlock()
			if f.Snapshot != nil {
				ss = f.Snapshot.Dup()
				defer ss.Release()
			}
			if f.Value == cache.CollectedObject {
				w.loadField(ss, t.ID, k, f, c)
			}
		}
		f.Touch = w.Now
		if i == len(cmd.Path.Segs)-1 {
			w.getField(ss, f, c, cmd)
			return
		}
		switch v := f.Value.(type) {
		case *tree.Tree:
			t = v
			t.Lock()
			f.Unlock()
		case error:
			f.Unlock()
			w.result = v
			return
		case nil:
			f.Unlock()
			w.result = &NotFoundError{cmd.Path.Sub(i)}
			return
		case []byte:
			f.Unlock()
			w.result = &NotTreeError{cmd.Path.Sub(i)}
			return
		}
	}
}

func (w *walker) handleSetCommand(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node, cmd *SetCommand) {
	defer ss.Release()
	for i, k := range cmd.Path.Segs {
		c = c.Field(k)
		f := t.Fields[k]
		switch f {
		case nil:
			f = t.EnterNewField(k)
			if !t.Full {
				w.loadField(ss, t.ID, k, f, c)
			}
		default:
			f.Lock()
			t.Unlock()
			if f.Snapshot != nil {
				ss = f.Snapshot.Dup()
				defer ss.Release()
			}
			if f.Value == cache.CollectedObject {
				w.loadField(ss, t.ID, k, f, c)
			}
		}
		f.Touch = w.Now
		if i == len(cmd.Path.Segs)-1 {
			w.setField(t, i, f, cmd)
			return
		}
		switch v := f.Value.(type) {
		case *tree.Tree:
			t = v
			t.Lock()
			f.Unlock()
		case error:
			f.Unlock()
			w.result = v
			return
		case []byte:
			f.Unlock()
			w.result = &NotTreeError{cmd.Path.Sub(i)}
			return
		case nil:
			w.setField(t, i, f, cmd)
			return
		}
	}
}

func (w *walker) handleDeleteCommand(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node, cmd *DeleteCommand) {
	defer ss.Release()
	for i, k := range cmd.Path.Segs {
		c = c.Field(k)
		f := t.Fields[k]
		switch f {
		case nil:
			if t.Full {
				t.Unlock()
				w.result = nil
				return
			}
			f = t.EnterNewField(k)
			w.memory += tree.FieldSize
			w.loadField(ss, t.ID, k, f, c)
		default:
			f.Lock()
			t.Unlock()
			if f.Snapshot != nil {
				ss = f.Snapshot.Dup()
				defer ss.Release()
			}
			if f.Value == cache.CollectedObject {
				w.loadField(ss, t.ID, k, f, c)
			}
		}
		if i == len(cmd.Path.Segs)-1 {
			w.deleteField(ss, t, k, f, cmd)
			return
		}
		switch v := f.Value.(type) {
		case *tree.Tree:
			t = v
			t.Lock()
			f.Unlock()
		case error:
			f.Unlock()
			w.result = v
			return
		case nil:
			f.Unlock()
			w.result = &NotFoundError{cmd.Path.Sub(i)}
			return
		case []byte:
			f.Unlock()
			w.result = &NotTreeError{cmd.Path.Sub(i)}
			return
		}
	}
}

func (w *walker) handleTouchCommand(ss *leveldb.Snapshot, t *tree.Tree, c cache.Node, cmd *TouchCommand) {
	ss.Release()
	for _, k := range cmd.Path.Segs {
		f, ok := t.Fields[k]
		if !ok {
			break
		}
		f.Lock()
		t.Unlock()
		f.Touch = w.Now
		switch v := f.Value.(type) {
		case *tree.Tree:
			t = v
			t.Lock()
			f.Unlock()
		default:
			f.Unlock()
			return
		}
	}
	t.Unlock()
}
