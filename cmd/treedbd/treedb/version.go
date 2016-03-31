package treedb

import (
	"sync"

	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
)

type Version struct {
	sync.Mutex
	number   uint64
	snapshot leveldb.Snapshot
}

func (v *Version) Init(snapshot leveldb.Snapshot) uint64 {
	v.number = 1
	v.snapshot = snapshot
	return v.number
}

func (v *Version) Commit(snapshot leveldb.Snapshot) uint64 {
	v.Lock()
	defer v.Unlock()
	v.snapshot.Close()
	v.number++
	v.snapshot = snapshot
	return v.number
}

func (v *Version) Latest() (uint64, leveldb.Snapshot) {
	v.Lock()
	defer v.Unlock()
	return v.number, v.snapshot.Dup()
}
