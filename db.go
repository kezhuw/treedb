package treedb

import (
	"errors"
	"runtime"
	"sync/atomic"
	"unsafe"
)

var (
	ErrDBClosed = errors.New("treedb: db closed")
)

type DB struct {
	id     uint32
	client *Client
	closed chan struct{}
}

type Options struct {
	ChannelCap int

	TemplateDB      string
	CreateIfMissing bool
	ErrorIfExists   bool
}

func splitOptions(opts *Options) (*DialOptions, *OpenOptions) {
	if opts == nil {
		return defaultDialOptions, defaultOpenOption
	}
	dopts := &DialOptions{ChannelCap: opts.ChannelCap}
	oopts := &OpenOptions{TemplateDB: opts.TemplateDB, CreateIfMissing: opts.CreateIfMissing, ErrorIfExists: opts.ErrorIfExists}
	return dopts, oopts
}

func Open(addr, name string, opts *Options) (*DB, error) {
	dopts, oopts := splitOptions(opts)
	client, err := Dial(addr, dopts)
	if err != nil {
		return nil, err
	}
	defer client.Release()
	db, err := client.OpenDB(name, oopts)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newDB(id uint32, client *Client) *DB {
	db := &DB{
		id:     id,
		client: client,
		closed: make(chan struct{}),
	}
	runtime.SetFinalizer(db, (*DB).finalize)
	return db
}

func (db *DB) getClient() *Client {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&db.client))
	return (*Client)(atomic.LoadPointer(addr))
}

func (db *DB) resetClient() *Client {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&db.client))
	c := atomic.LoadPointer(addr)
	switch {
	case c == nil:
		return nil
	case !atomic.CompareAndSwapPointer(addr, c, nil):
		return nil
	}
	return (*Client)(c)
}

func (db *DB) finalize() {
	go db.Close()
}

func (db *DB) Close() error {
	c := db.resetClient()
	if c == nil {
		<-db.closed
		return nil
	}
	defer close(db.closed)
	return c.closeDB(db.id)
}

func (db *DB) get(root, path string, kind FieldType) (interface{}, error) {
	c := db.getClient()
	if c == nil {
		return nil, ErrDBClosed
	}
	return c.get(db.id, root, path, kind)
}

func (db *DB) set(root, path string, value interface{}) error {
	c := db.getClient()
	if c == nil {
		return ErrDBClosed
	}
	return c.set(db.id, root, path, value)
}

func (db *DB) delete(root, path string) error {
	c := db.getClient()
	if c == nil {
		return ErrDBClosed
	}
	return c.delete(db.id, root, path)
}

func (db *DB) cache(root, path string, timeout int64) error {
	c := db.getClient()
	if c == nil {
		return ErrDBClosed
	}
	return c.cache(db.id, root, path, timeout)
}

func (db *DB) touch(root, path string) error {
	c := db.getClient()
	if c == nil {
		return ErrDBClosed
	}
	return c.touch(db.id, root, path)
}

func (db *DB) Get(path string, kind FieldType) (interface{}, error) {
	return db.get("/", path, kind)
}

func (db *DB) Set(path string, value interface{}) error {
	return db.set("/", path, value)
}

func (db *DB) Delete(path string) error {
	return db.delete("/", path)
}

func (db *DB) Cache(path string, timeout int64) error {
	return db.cache("/", path, timeout)
}

func (db *DB) Touch(path string) error {
	return db.touch("/", path)
}

func (db *DB) Tree(path string) *Tree {
	if last := len(path) - 1; last >= 0 && path[last] != '/' {
		path = path + "/"
	}
	return &Tree{db: db, root: path}
}
