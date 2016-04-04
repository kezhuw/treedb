package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
)

type collector struct {
	buf     bytes.Buffer
	batch   leveldb.Batch
	Remains bool
}

func (c *collector) Collect(db *DB) {
	_, ss := db.latestVersion()
	defer ss.Close()
	prefix := []byte("$gc.tree.")
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		id, err := strconv.ParseUint(string(key[len(prefix):]), 10, 64)
		if err != nil {
			log.Printf("database[%s] fail to parse tree id from gc key[%s]: %s", db.Name, key, err)
			continue
		}
		ok := c.collectTree(db, ss.Dup(), id)
		if ok {
			c.batch.Delete(key)
		}
	}
	if err := it.Error(); err != nil {
		log.Printf("database[%s] fail to iterate gc keys: %s", db.Name, err)
		return
	}
	if err := db.storage.Write(&c.batch, nil); err != nil {
		log.Printf("database[%s] fail to write gc batch: %s", db.Name, err)
		return
	}
	db.commitVersion()
}

func (c *collector) collectTree(db *DB, ss leveldb.Snapshot, id uint64) bool {
	c.buf.Reset()
	fmt.Fprintf(&c.buf, "/tree/%d/", id)
	prefix := c.buf.Bytes()
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	all := true
	for it.Next() {
		key := it.Key()
		c.batch.Delete(key)
		value := it.Value()
		if value[0] != FieldTree {
			continue
		}
		subTreeId, n := binary.Uvarint(value[1:])
		if n <= 0 || n+1 != len(value) {
			log.Printf("database[%s] fail to read sub tree id for key[%s]: 0x%X", db.Name, key, value)
			all = false
			continue
		}
		c.buf.Reset()
		fmt.Fprintf(&c.buf, "$gc.tree.%d", subTreeId)
		c.batch.Put(c.buf.Bytes(), zeroBytes)
		c.Remains = true
	}
	return all
}
