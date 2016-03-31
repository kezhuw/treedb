package leveldb

import (
	"runtime"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	leveliter "github.com/syndtr/goleveldb/leveldb/iterator"
	levelopt "github.com/syndtr/goleveldb/leveldb/opt"
	levelutil "github.com/syndtr/goleveldb/leveldb/util"
)

type Iterator leveliter.Iterator
type Options levelopt.Options
type ReadOptions levelopt.ReadOptions
type WriteOptions levelopt.WriteOptions

var (
	ErrNotFound         = leveldb.ErrNotFound
	ErrSnapshotClosed   = leveldb.ErrSnapshotReleased
	ErrIteratorReleased = leveldb.ErrIterReleased
	ErrDBClosed         = leveldb.ErrClosed
)

type Batch struct {
	leveldb.Batch
}

type DB struct {
	db *leveldb.DB
}

func Open(path string, options *Options) (*DB, error) {
	db, err := leveldb.OpenFile(path, (*levelopt.Options)(options))
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Get(key []byte, options *ReadOptions) ([]byte, error) {
	return db.db.Get(key, (*levelopt.ReadOptions)(options))
}

func (db *DB) Has(key []byte, options *ReadOptions) (bool, error) {
	return db.db.Has(key, (*levelopt.ReadOptions)(options))
}

func (db *DB) Put(key, value []byte, options *WriteOptions) error {
	return db.db.Put(key, value, (*levelopt.WriteOptions)(options))
}

func (db *DB) Write(batch *Batch, options *WriteOptions) error {
	return db.db.Write(&batch.Batch, (*levelopt.WriteOptions)(options))
}

func (db *DB) Find(start []byte, options *ReadOptions) Iterator {
	return db.db.NewIterator(&levelutil.Range{Start: start}, (*levelopt.ReadOptions)(options))
}

func (db *DB) Range(start []byte, limit []byte, options *ReadOptions) Iterator {
	return db.db.NewIterator(&levelutil.Range{Start: start, Limit: limit}, (*levelopt.ReadOptions)(options))
}

func (db *DB) Prefix(prefix []byte, options *ReadOptions) Iterator {
	return db.db.NewIterator(levelutil.BytesPrefix(prefix), (*levelopt.ReadOptions)(options))
}

func (db *DB) GetSnapshot() Snapshot {
	snapshot, err := db.db.GetSnapshot()
	if err != nil {
		return nil
	}
	shared := &sharedSnapshot{Snapshot: snapshot}
	return shared.Retain()
}

type Snapshot interface {
	Dup() Snapshot
	Close() error
	Get(key []byte, options *ReadOptions) ([]byte, error)
	Has(key []byte, options *ReadOptions) (bool, error)
	Find(start []byte, options *ReadOptions) Iterator
	Range(start []byte, limit []byte, options *ReadOptions) Iterator
	Prefix(prefix []byte, options *ReadOptions) Iterator
}

type sharedSnapshot struct {
	Snapshot *leveldb.Snapshot
	refcnt   int64
}

type snapshot struct {
	shared *sharedSnapshot
	closed bool
}

func (ss *sharedSnapshot) Retain() Snapshot {
	atomic.AddInt64(&ss.refcnt, 1)
	return newSnapshot(ss)
}

func (ss *sharedSnapshot) Release() {
	if atomic.AddInt64(&ss.refcnt, -1) == 0 {
		ss.Snapshot.Release()
	}
}

func newSnapshot(shared *sharedSnapshot) Snapshot {
	ss := &snapshot{shared: shared}
	runtime.SetFinalizer(ss, (*snapshot).finalize)
	return ss
}

func (ss *snapshot) Dup() Snapshot {
	if ss.closed {
		panic(ErrSnapshotClosed)
	}
	return ss.shared.Retain()
}

func (ss *snapshot) finalize() error {
	if ss.closed {
		return ErrSnapshotClosed
	}
	ss.closed = true
	ss.shared.Release()
	return nil
}

func (ss *snapshot) Close() error {
	runtime.SetFinalizer(ss, nil)
	return ss.finalize()
}

func (ss *snapshot) Find(start []byte, options *ReadOptions) Iterator {
	if ss.closed {
		panic(ErrSnapshotClosed)
	}
	return ss.shared.Snapshot.NewIterator(&levelutil.Range{Start: start}, (*levelopt.ReadOptions)(options))
}

func (ss *snapshot) Get(key []byte, options *ReadOptions) ([]byte, error) {
	if ss.closed {
		panic(ErrSnapshotClosed)
	}
	return ss.shared.Snapshot.Get(key, (*levelopt.ReadOptions)(options))
}

func (ss *snapshot) Has(key []byte, options *ReadOptions) (bool, error) {
	if ss.closed {
		panic(ErrSnapshotClosed)
	}
	return ss.shared.Snapshot.Has(key, (*levelopt.ReadOptions)(options))
}

func (ss *snapshot) Range(start []byte, limit []byte, options *ReadOptions) Iterator {
	if ss.closed {
		panic(ErrSnapshotClosed)
	}
	return ss.shared.Snapshot.NewIterator(&levelutil.Range{Start: start, Limit: limit}, (*levelopt.ReadOptions)(options))
}

func (ss *snapshot) Prefix(prefix []byte, options *ReadOptions) Iterator {
	if ss.closed {
		panic(ErrSnapshotClosed)
	}
	return ss.shared.Snapshot.NewIterator(levelutil.BytesPrefix(prefix), (*levelopt.ReadOptions)(options))
}
