package treedb

import (
	"errors"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/kezhuw/treedb/protocol"
	"github.com/kezhuw/treedb/v0"
)

var (
	ErrClientClosed        = errors.New("treedb: client closed")
	ErrServerClosed        = errors.New("treedb: server closed")
	ErrIncompatibleVersion = errors.New("treedb: incompatible version")
)

type ResponseTypeError struct {
	Type reflect.Type
}

func (e *ResponseTypeError) Error() string {
	return "treedb: unexpected response type: " + e.Type.String()
}

const (
	DefaultChannelCap = 128
)

type OpenOptions struct {
	TemplateDB      string
	CreateIfMissing bool
	ErrorIfExists   bool
}

type DialOptions struct {
	ChannelCap int
}

var defaultOpenOption = &OpenOptions{}
var defaultDialOptions = &DialOptions{ChannelCap: DefaultChannelCap}

type dbVersion struct {
	Name    string
	Version uint64
}

type Client struct {
	broker     *broker
	closed     chan struct{}
	serverAddr string
	serverConn net.Conn

	dbMutex    sync.Mutex
	dbReleased bool
	dbVersions map[uint32]dbVersion
}

func Dial(addr string, opts *DialOptions) (*Client, error) {
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = defaultDialOptions
	}
	c := &Client{
		broker:     newBroker(opts.ChannelCap, conn),
		closed:     make(chan struct{}),
		serverAddr: addr,
		serverConn: conn,
		dbVersions: make(map[uint32]dbVersion),
	}
	reply := make(chan interface{}, 1)
	c.broker.Request(reply, protocol.HANDSHAKE, &protocol.HandshakeRequest{Versions: []uint32{v0.Version}})
	switch result := (<-reply).(type) {
	case *protocol.HandshakeResponse:
		if result.Version == v0.Version {
			runtime.SetFinalizer(c, (*Client).finalize)
			return c, nil
		}
		c.Close()
		return nil, ErrIncompatibleVersion
	default:
		c.Close()
		return nil, &ResponseTypeError{reflect.TypeOf(result)}
	}
}

func (c *Client) OpenDB(name string, options *OpenOptions) (*DB, error) {
	b := c.getBroker()
	if b == nil {
		return nil, ErrClientClosed
	}
	if options == nil {
		options = defaultOpenOption
	}
	msg := &protocol.OpenDBRequest{
		Name:            name,
		Template:        options.TemplateDB,
		CreateIfMissing: options.CreateIfMissing,
		ErrorIfExists:   options.ErrorIfExists,
	}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.OPEN, msg)
	switch result := (<-reply).(type) {
	case error:
		return nil, result
	case *protocol.OpenDBResponse:
		c.refDB(result.Db, name, result.Version)
		return newDB(result.Db, c), nil
	default:
		return nil, &ResponseTypeError{reflect.TypeOf(result)}
	}
}

func (c *Client) refDB(id uint32, name string, version uint64) {
	c.dbMutex.Lock()
	c.dbVersions[id] = dbVersion{Name: name, Version: version}
	c.dbMutex.Unlock()
}

func (c *Client) unrefDB(id uint32) {
	c.dbMutex.Lock()
	delete(c.dbVersions, id)
	if c.dbReleased && len(c.dbVersions) == 0 {
		go c.Close()
	}
	c.dbMutex.Unlock()
}

func (c *Client) closeDB(db uint32) error {
	c.unrefDB(db)
	b := c.getBroker()
	if b == nil {
		return ErrClientClosed
	}
	msg := &protocol.CloseDBRequest{Db: db}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.CLOSE, msg)
	return okOrError(reply)
}

func (c *Client) getBroker() *broker {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&c.broker))
	return (*broker)(atomic.LoadPointer(addr))
}

func (c *Client) resetBroker() *broker {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&c.broker))
	b := atomic.LoadPointer(addr)
	switch {
	case b == nil:
		return nil
	case !atomic.CompareAndSwapPointer(addr, b, nil):
		return nil
	}
	return (*broker)(b)
}

func (c *Client) finalize() {
	go c.Close()
}

func (c *Client) Release() {
	c.dbMutex.Lock()
	c.dbReleased = true
	if len(c.dbVersions) == 0 {
		go c.Close()
	}
	c.dbMutex.Unlock()
}

func (c *Client) Close() error {
	b := c.resetBroker()
	if b == nil {
		<-c.closed
		return nil
	}
	defer close(c.closed)
	return b.Close()
}

func combinePath(root, path string) string {
	switch {
	case path == "" || path == "/":
		return root
	case path[0] == '/':
		return root + path[1:]
	default:
		return root + path
	}
}

func (c *Client) get(db uint32, root, path string, kind FieldType) (interface{}, error) {
	b := c.getBroker()
	if b == nil {
		return nil, ErrClientClosed
	}
	msg := &protocol.GetRequest{
		Db:   db,
		Path: combinePath(root, path),
		Type: uint32(kind),
	}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.GET, msg)
	switch result := (<-reply).(type) {
	case error:
		return nil, result
	case *protocol.GetResponse:
		return protocol.Unmarshal(result.Value)
	default:
		return nil, &ResponseTypeError{reflect.TypeOf(result)}
	}
}

func (c *Client) set(db uint32, root, path string, value interface{}) error {
	b := c.getBroker()
	if b == nil {
		return ErrClientClosed
	}
	buf, err := protocol.Marshal(value)
	if err != nil {
		return err
	}
	msg := &protocol.SetRequest{
		Db:    db,
		Path:  combinePath(root, path),
		Value: buf,
	}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.SET, msg)
	return okOrError(reply)
}

func (c *Client) delete(db uint32, root, path string) error {
	b := c.getBroker()
	if b == nil {
		return ErrClientClosed
	}
	msg := &protocol.DeleteRequest{Db: db, Path: combinePath(root, path)}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.DELETE, msg)
	return okOrError(reply)
}

func (c *Client) cache(db uint32, root, path string, timeout int64) error {
	b := c.getBroker()
	if b == nil {
		return ErrClientClosed
	}
	msg := &protocol.CacheRequest{Db: db, Path: combinePath(root, path), Timeout: timeout}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.CACHE, msg)
	return okOrError(reply)
}

func (c *Client) touch(db uint32, root, path string) error {
	b := c.getBroker()
	if b == nil {
		return ErrClientClosed
	}
	msg := &protocol.TouchRequest{Db: db, Path: combinePath(root, path)}
	reply := make(chan interface{}, 1)
	b.Request(reply, protocol.TOUCH, msg)
	return okOrError(reply)
}

func okOrError(reply chan interface{}) error {
	switch result := (<-reply).(type) {
	case nil:
		return nil
	case error:
		return result
	default:
		return &ResponseTypeError{reflect.TypeOf(result)}
	}
}
