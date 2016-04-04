package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/cache"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

type keyValue struct {
	Key   string
	Value interface{}
}

type writer struct {
	leveldb.Batch

	Now       time.Time
	Memory    int
	Snapshots []leveldb.Snapshot

	TreeGC bool

	ubuf [binary.MaxVarintLen64 + 1]byte
	kbuf bytes.Buffer
	vbuf bytes.Buffer
}

func (w *writer) subMemory(n int) {
	w.Memory -= n
}

func (w *writer) addMemory(n int) {
	w.Memory += n
}

func (w *writer) addSnapshot(ss leveldb.Snapshot) {
	w.Snapshots = append(w.Snapshots, ss)
}

func (w *writer) DeleteBinary(id tree.ID, key string, value []byte) {
	w.Delete(w.keyBuf(id, key))
	w.subMemory(cache.SizeofBytes(value))
}

func (w *writer) RewriteBinary(value []byte) {
	w.subMemory(cache.SizeofBytes(value))
}

func (w *writer) DeleteTree(t *tree.Tree) {
	w.TreeGC = true
	w.Put(w.keyfBuf("$gc.tree.%d", t.ID), []byte{})
	t.Lock()
	var pendings []*tree.Tree
	for {
		w.subMemory(tree.TreeSize + len(t.Fields)*tree.FieldSize)
		for k, f := range t.Fields {
			w.subMemory(cache.SizeofKey(k))
			f.Lock()
			if f.Snapshot != nil {
				w.addSnapshot(f.Snapshot)
			}
			switch v := f.Value.(type) {
			case []byte:
				w.subMemory(cache.SizeofBytes(v))
			case *tree.Tree:
				v.Lock()
				pendings = append(pendings, v)
			}
			f.Unlock()
		}
		defer t.Unlock()
		if len(pendings) == 0 {
			break
		}
		t = pendings[len(pendings)-1]
		pendings = pendings[:len(pendings)-1]
	}
}

func (w *writer) treeBuf(id tree.ID) []byte {
	w.ubuf[0] = FieldTree
	n := binary.PutUvarint(w.ubuf[1:], uint64(id))
	return w.ubuf[:n+1]
}

func (w *writer) binaryBuf(b []byte) []byte {
	w.vbuf.Reset()
	w.vbuf.WriteByte(FieldBinary)
	w.vbuf.Write(b)
	return w.vbuf.Bytes()
}

func (w *writer) keyBuf(id tree.ID, key interface{}) []byte {
	w.kbuf.Reset()
	fmt.Fprintf(&w.kbuf, "/tree/%d/%s", id, key)
	return w.kbuf.Bytes()
}

func (w *writer) keyfBuf(format string, args ...interface{}) []byte {
	w.kbuf.Reset()
	fmt.Fprintf(&w.kbuf, format, args...)
	return w.kbuf.Bytes()
}

func (w *writer) writeFields(db *DB, t *tree.Tree, fields map[string]interface{}) error {
	for k, v := range fields {
		f := &tree.Field{Touch: w.Now}
		switch v := v.(type) {
		case []byte:
			f.Value = v
			w.Put(w.keyBuf(t.ID, k), w.binaryBuf(v))
			w.addMemory(cache.SizeofBytes(v))
		case map[string]interface{}:
			subTree, err := w.createTree(db, v)
			if err != nil {
				return err
			}
			f.Value = subTree
			w.Put(w.keyBuf(t.ID, k), w.treeBuf(subTree.ID))
		default:
			return ErrInvalidValue
		}
		t.Fields[k] = f
	}
	w.addMemory(len(fields) * tree.FieldSize)
	return nil
}

func (w *writer) createTree(db *DB, fields map[string]interface{}) (*tree.Tree, error) {
	t := tree.NewTree(db.newTreeId())
	err := w.writeFields(db, t, fields)
	if err != nil {
		return nil, err
	}
	t.Full = true
	w.addMemory(tree.TreeSize)
	return t, nil
}

func (w *writer) WriteValue(db *DB, id tree.ID, key string, children []string, value interface{}) (interface{}, error) {
	var disk []byte
	switch v := value.(type) {
	case []byte:
		disk = w.binaryBuf(v)
		w.addMemory(cache.SizeofBytes(v))
	case map[string]interface{}:
		fieldTree, err := w.createTree(db, v)
		if err != nil {
			return nil, err
		}
		value = fieldTree
		disk = w.treeBuf(fieldTree.ID)
	default:
		return nil, ErrInvalidValue
	}
	for i := len(children) - 1; i >= 0; i-- {
		k := children[i]
		id := db.newTreeId()
		t := tree.NewTree(id)
		t.Full = true
		t.Fields[k] = &tree.Field{Touch: w.Now, Value: value}
		w.Put(w.keyBuf(id, k), disk)
		disk = w.treeBuf(id)
		value = t
		w.addMemory(tree.TreeSize + tree.FieldSize)
	}
	w.Put(w.keyBuf(id, key), disk)
	return value, nil
}
