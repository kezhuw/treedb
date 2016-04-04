package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/kezhuw/guard"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/cache"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/leveldb"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/stat"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb/internal/tree"
)

var (
	ErrDBClosed       = errors.New("treedb: db closed")
	ErrDBReadonly     = errors.New("treedb: db is readonly")
	ErrMismatchedType = errors.New("treedb: mismatched type")
	ErrUnknownCommand = errors.New("treedb: unknown command")
	ErrInvalidValue   = errors.New("treedb: invalid value")
)

type request struct {
	Cmd   Command
	Reply chan interface{}
}

type DB struct {
	Name     string
	Version  uint64
	readonly bool

	closed int32

	reads    sync.WaitGroup
	writes   sync.WaitGroup
	requests chan request

	root   *tree.Tree
	treeId uint64
	caches struct {
		root  *cache.Tree
		paths map[string]int64
		guard guard.Guard
	}
	memStat *stat.Memory

	gcing  chan struct{}
	gcdone chan struct{}

	version Version
	storage *leveldb.DB
}

var doneKey = []byte("$internal.status.done")

func readStatusDone(storage *leveldb.DB) (bool, error) {
	_, err := storage.Get(doneKey, nil)
	switch err {
	case nil:
		return true, nil
	case leveldb.ErrNotFound:
		return false, nil
	}
	return false, err
}

func readMaxTreeId(storage *leveldb.DB) (uint64, error) {
	prefix := []byte("/tree/")
	it := storage.Prefix(prefix, nil)
	defer it.Release()
	i := len(prefix)
	if it.Last() {
		key := it.Key()
		j := bytes.IndexByte(key[i:], '/')
		if j == -1 {
			return 0, fmt.Errorf("treedb: invalid tree key: %s", key)
		}
		id, err := strconv.ParseUint(string(key[i:i+j]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("treedb: invalid tree key: %s", key)
		}
		return id, nil
	}
	return 0, it.Error()
}

func readCachedTimeout(value []byte) (int64, error) {
	if len(value) == 0 || value[0] != FieldBinary {
		return -1, ErrCorruptedData
	}
	timeout, n := binary.Varint(value[1:])
	if n <= 0 {
		return -1, ErrCorruptedData
	}
	return timeout, nil
}

func readCachedPaths(ss leveldb.Snapshot) (map[string]int64, error) {
	defer ss.Close()
	prefix := []byte("$cache.paths.")
	it := ss.Prefix(prefix, nil)
	defer it.Release()
	paths := make(map[string]int64)
	for it.Next() {
		timeout, err := readCachedTimeout(it.Value())
		if err != nil {
			return nil, err
		}
		path := string(it.Key()[len(prefix):])
		paths[path] = timeout
	}
	return paths, it.Error()
}

func openDB(name string, opts *OpenOptions) (db *DB, err error) {
	var options leveldb.Options
	options.ReadOnly = opts.readonly
	options.ErrorIfMissing = !opts.CreateIfMissing
	storage, err := leveldb.Open(name, &options)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			storage.Close()
		}
	}()

	done, err := readStatusDone(storage)
	if err != nil {
		return nil, err
	}

	switch {
	case done == true:
		treeId, err := readMaxTreeId(storage)
		if err != nil {
			return nil, err
		}

		paths, err := readCachedPaths(storage.GetSnapshot())
		if err != nil {
			storage.Close()
			return nil, err
		}

		caches, err := cache.Build(paths)
		if err != nil {
			return nil, err
		}
		if opts.ErrorIfExists {
			opts.err = ErrDBExist
		}
		return newDB(name, treeId, caches, paths, opts.readonly, storage), nil
	case opts.CreateIfMissing == false:
		return nil, ErrDBMissing
	case opts.snapshot == nil:
		err := storage.Put(doneKey, []byte{}, nil)
		if err != nil {
			return nil, err
		}
		caches, _ := cache.Build(nil)
		return newDB(name, 0, caches, nil, opts.readonly, storage), nil
	default:
		var batch cloneBatch
		if err := batch.CloneData(opts.snapshot); err != nil {
			return nil, err
		}
		if err := batch.CloneCaches(opts.snapshot); err != nil {
			return nil, err
		}
		batch.WriteDone()
		caches, err := cache.Build(batch.caches)
		if err != nil {
			return nil, err
		}
		if err := storage.Write(&batch.batch, nil); err != nil {
			return nil, err
		}
		return newDB(name, batch.treeId, caches, batch.caches, opts.readonly, storage), nil
	}
}

func newDB(name string, treeId uint64, cacheRoot *cache.Tree, cachePaths map[string]int64, readonly bool, storage *leveldb.DB) *DB {
	db := &DB{
		Name:     name,
		root:     tree.NewTree(0),
		treeId:   treeId,
		readonly: readonly,
		storage:  storage,
		requests: make(chan request, 500),
		gcing:    make(chan struct{}, 1),
		gcdone:   make(chan struct{}),
	}
	db.caches.root = cacheRoot
	db.caches.paths = cachePaths
	version := db.version.Init(db.storage.GetSnapshot())
	db.memStat = stat.NewMemory(db.root, cacheRoot, time.Minute*5, version, 16)
	db.memStat.Start()
	runtime.SetFinalizer(db, (*DB).finalize)
	go db.serve()
	switch readonly {
	case true:
		close(db.gcdone)
	default:
		go db.gc()
	}
	return db
}

func (db *DB) getCacheRoot() *cache.Tree {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&db.caches.root))
	return (*cache.Tree)(atomic.LoadPointer(addr))
}

func (db *DB) getSnapshot() (leveldb.Snapshot, error) {
	reply := make(chan interface{}, 1)
	db.Request(reply, (*snapshotCommand)(nil))
	switch result := (<-reply).(type) {
	case error:
		return nil, result
	case leveldb.Snapshot:
		return result, nil
	default:
		panic("unexpected result for getSnapshot()")
	}
}

func (db *DB) releaseSnapshot(ss leveldb.Snapshot) {
	ss.Close()
	db.reads.Done()
}

func (db *DB) Close() error {
	if !atomic.CompareAndSwapInt32(&db.closed, 0, 1) {
		return nil
	}
	done := make(chan interface{})
	db.Request(done, (*closeCommand)(nil))
	<-done
	close(db.gcing)
	db.memStat.Stop()
	<-db.gcdone
	db.root = nil
	db.caches.root = nil
	db.memStat = nil
	return db.storage.Close()
}

func (db *DB) newTreeId() tree.ID {
	return tree.ID(atomic.AddUint64(&db.treeId, 1))
}

func (db *DB) addMemory(n int) {
	db.memStat.Add(n)
}

func (db *DB) commitVersion() (uint64, leveldb.Snapshot) {
	snapshot := db.storage.GetSnapshot()
	version := db.version.Commit(snapshot)
	db.memStat.Update()
	return version, snapshot.Dup()
}

func (db *DB) latestVersion() (uint64, leveldb.Snapshot) {
	return db.version.Latest()
}

func (db *DB) commit(w *writer) error {
	if err := db.storage.Write(&w.Batch, nil); err != nil {
		return err
	}
	db.addMemory(w.Memory)
	for _, ss := range w.Snapshots {
		ss.Close()
	}
	if w.TreeGC {
		select {
		default:
		case db.gcing <- struct{}{}:
		}
	}
	return nil
}

func drainRequests(c <-chan request, result interface{}) {
	for req := range c {
		req.Reply <- result
	}
}

func (db *DB) finalize() {
	close(db.requests)
}

func (db *DB) Request(reply chan interface{}, cmd Command) {
	db.requests <- request{Reply: reply, Cmd: cmd}
}

func (db *DB) handleClose(done chan interface{}) {
	go drainRequests(db.requests, ErrDBClosed)
	db.reads.Wait()
	db.writes.Wait()
	close(done)
}

type handlerInfo struct {
	Reading  bool
	Callback reflect.Value
}

var requestHandlers = make(map[reflect.Type]handlerInfo)

func registerHandler(cmd Command, reading bool, callback interface{}) {
	requestHandlers[reflect.TypeOf(cmd)] = handlerInfo{reading, reflect.ValueOf(callback)}
}

func init() {
	registerHandler((*GetCommand)(nil), true, (*walker).handleGetCommand)
	registerHandler((*SetCommand)(nil), false, (*walker).handleSetCommand)
	registerHandler((*DeleteCommand)(nil), false, (*walker).handleDeleteCommand)
	registerHandler((*TouchCommand)(nil), false, (*walker).handleTouchCommand)
}

const (
	cachePathNever   = -2
	cachePathDelete  = -1
	cachePathDefault = 0
)

func (db *DB) handleCacheCommand(l guard.Locker, reply chan interface{}, cmd *CacheCommand) {
	l.Lock()
	defer l.Unlock()
	defer db.writes.Done()
	paths := copyCachePaths(db.caches.paths)
	var batch leveldb.Batch
	switch {
	case cmd.Timeout == cachePathDelete:
		if _, ok := paths[cmd.Path.Full]; !ok {
			reply <- nil
			return
		}
		delete(paths, cmd.Path.Full)
		batch.Delete(bprintf("$cache.paths.%s", cmd.Path.Full))
	case cmd.Timeout == cachePathNever || cmd.Timeout >= 0:
		var buf [binary.MaxVarintLen64 + 1]byte
		paths[cmd.Path.Full] = cmd.Timeout
		buf[0] = FieldBinary
		n := binary.PutVarint(buf[1:], cmd.Timeout)
		batch.Put(bprintf("$cache.paths.%s", cmd.Path.Full), buf[:n+1])
	default:
		reply <- ErrInvalidParam
		return
	}
	root, err := cache.Build(paths)
	if err != nil {
		reply <- nil
		return
	}
	if err := db.storage.Write(&batch, nil); err != nil {
		reply <- err
		return
	}
	reply <- nil
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.caches.root)), unsafe.Pointer(root))
	db.caches.paths = paths
	db.commitVersion()
}

func (db *DB) serve() {
	for req := range db.requests {
		handler, ok := requestHandlers[reflect.TypeOf(req.Cmd)]
		if ok {
			w := &walker{DB: db, Now: time.Now(), Reply: req.Reply}
			w.Start(handler.Callback, handler.Reading, req.Cmd)
			continue
		}
		switch cmd := req.Cmd.(type) {
		case *CacheCommand:
			l := db.caches.guard.NewLocker()
			db.writes.Add(1)
			go db.handleCacheCommand(l, req.Reply, cmd)
		case *closeCommand:
			db.handleClose(req.Reply)
			return
		case *snapshotCommand:
			db.reads.Add(1)
			db.writes.Wait()
			_, ss := db.latestVersion()
			req.Reply <- ss
		default:
			req.Reply <- ErrUnknownCommand
		}
	}
}

func (db *DB) gc() {
	defer close(db.gcdone)
	timeout := 5 * time.Minute
	gcing := db.gcing
	for {
		select {
		case _, ok := <-gcing:
			if !ok {
				return
			}
		case <-time.After(timeout):
		}
		var c collector
		c.Collect(db)
		switch c.Remains {
		case true:
			timeout = 1 * time.Minute
		default:
			timeout = 5 * time.Minute
		}
	}
}
