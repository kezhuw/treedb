package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/kezhuw/leveldb"
)

type cloneBatch struct {
	batch  leveldb.Batch
	treeId uint64
	caches map[string]int64
}

func (b *cloneBatch) WriteDone() {
	b.batch.Put(doneKey, []byte{})
}

func (b *cloneBatch) CloneCaches(ss *leveldb.Snapshot) error {
	prefix := []byte("$cache.paths.")
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	caches := make(map[string]int64)
	for it.Next() {
		timeout, err := readCachedTimeout(it.Value())
		if err != nil {
			return err
		}
		path := string(it.Key()[len(prefix):])
		caches[path] = timeout
		b.batch.Put(it.Key(), it.Value())
	}
	b.caches = caches
	return it.Err()
}

func (b *cloneBatch) CloneData(ss *leveldb.Snapshot) error {
	var buf bytes.Buffer
	var pendings []uint64
	var id, max uint64
	for {
		fmt.Fprintf(&buf, "/tree/%d/", id)
		it := ss.Prefix(buf.Bytes(), nil)
		for it.Next() {
			value := it.Value()
			if value[0] == FieldTree {
				d, n := binary.Uvarint(value[1:])
				if n <= 0 || n+1 != len(value) {
					it.Release()
					return fmt.Errorf("treedb: invalid subtree blob: 0x%X", value)
				}
				pendings = append(pendings, d)
				if d > max {
					max = d
				}
			}
			b.batch.Put(it.Key(), value)
		}
		if err := it.Err(); err != nil {
			it.Release()
			return err
		}
		it.Release()
		last := len(pendings) - 1
		if last < 0 {
			break
		}
		id, pendings = pendings[last], pendings[:last]
		buf.Reset()
	}
	b.treeId = max
	return nil
}
