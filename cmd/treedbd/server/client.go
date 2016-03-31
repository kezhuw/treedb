package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/kezhuw/neterrs"
	"github.com/kezhuw/treedb/cmd/treedbd/server/tip"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb"
	"github.com/kezhuw/treedb/protocol"
)

var ErrIncompatibleVersion = errors.New("incompatible protocol version")

type dbDict struct {
	mu   sync.RWMutex
	seq  uint32
	dict map[uint32]*treedb.DB
}

func newDBDict() *dbDict {
	d := new(dbDict)
	d.dict = make(map[uint32]*treedb.DB)
	return d
}

func (d *dbDict) Get(id uint32) *treedb.DB {
	d.mu.RLock()
	db := d.dict[id]
	d.mu.RUnlock()
	return db
}

func (d *dbDict) Put(db *treedb.DB) uint32 {
	d.mu.Lock()
	d.seq++
	id := d.seq
	d.dict[id] = db
	d.mu.Unlock()
	return id
}

func (d *dbDict) Delete(id uint32) bool {
	d.mu.Lock()
	db, ok := d.dict[id]
	if ok {
		delete(d.dict, id)
	}
	d.mu.Unlock()
	return db != nil
}

type Client struct {
	id     uint64
	dir    *treedb.Directory
	logger *logrus.Logger

	conn connection
	addr net.Addr

	closed       chan struct{}
	decoder      *protocol.Decoder
	disconnected chan uint64

	chans     sync.Pool
	writes    chan protocol.Packet
	asyncs    sync.WaitGroup
	databases *dbDict
}

func (c *Client) handleRead(conn connection, reads chan protocol.Packet) {
	defer close(reads)
	r := bufio.NewReader(conn)
	var err error
	var pkt protocol.Packet
	for {
		err = protocol.ReadPacket(r, &pkt)
		if err != nil {
			break
		}
		reads <- pkt
		pkt.Payload = nil
	}
	switch {
	case err == io.EOF || neterrs.IsClosed(err):
		c.logger.Infof("client[%d, %s], peer closed.", c.id, c.addr)
	default:
		c.logger.Warnf("client[%d, %s], peer aborted: %s.", c.id, c.addr, err)
	}
}

func (c *Client) handleWrite(conn connection, writes chan protocol.Packet) {
	defer conn.CloseWrite()
	w := bufio.NewWriter(conn)
	for pkt := range writes {
		protocol.WritePacket(w, &pkt)
		err := w.Flush()
		if err != nil {
			c.logger.Warnf("client[%d, %s] write error: %s", c.id, c.addr, err)
			break
		}
	}
	for range writes {
	}
	close(c.closed)
	c.disconnected <- c.id
}

func (c *Client) Start(cap int) {
	c.writes = make(chan protocol.Packet, cap)
	c.closed = make(chan struct{})
	go c.serve(cap)
}

func (c *Client) serve(cap int) {
	c.logger.Infof("client[%d, %s] serving", c.id, c.addr)
	reads := make(chan protocol.Packet, cap)
	go c.handleRead(c.conn, reads)
	go c.handleWrite(c.conn, c.writes)
	defer close(c.writes)
	for pkt := range reads {
		err := c.handlePacket(pkt.Seq, pkt.Cmd, pkt.Payload)
		if err != nil {
			c.conn.CloseRead()
			go func() {
				for range reads {
				}
			}()
			c.logger.Errorf("client[%d, %s] fail to handle command %d, aborting: %s", c.id, c.addr, pkt.Cmd, err)
			break
		}
	}
	c.logger.Infof("client[%d, %s] exiting, wait for asynchrous request done", c.id, c.addr)
	c.asyncs.Wait()
	c.logger.Infof("client[%d, %s] exited.", c.id, c.addr)
}

func (c *Client) replyOk(seq uint32) {
	c.writes <- protocol.Packet{Seq: seq, Cmd: protocol.OK}
}

func (c *Client) replyEcode(seq uint32, code int) {
	var msg protocol.ErrorResponse
	msg.Code = uint32(code)
	payload, _ := proto.Marshal(&msg)
	c.replyPacket(seq, protocol.ERROR, payload)
}

func (c *Client) replyError(seq uint32, err error) {
	var msg protocol.ErrorResponse
	switch err {
	case treedb.ErrDBClosed:
		msg.Code = protocol.EcodeDBShutdown
	case treedb.ErrDBReadonly:
		msg.Code = protocol.EcodeDBReadonly
	case treedb.ErrMismatchedType:
		msg.Code = protocol.EcodeMismatchedType
	case treedb.ErrUnknownCommand:
		msg.Code = protocol.EcodeUnknownRequest
	case treedb.ErrInvalidValue:
		msg.Code = protocol.EcodeInternalError
	default:
		switch e := err.(type) {
		case *treedb.NotFoundError:
			msg.Code = protocol.EcodePathNotFound
			msg.Info = e.Path
		case *treedb.NotTreeError:
			msg.Code = protocol.EcodePathNotTree
			msg.Info = e.Path
		default:
			msg.Code = protocol.EcodeInternalError
		}
	}
	payload, _ := proto.Marshal(&msg)
	c.replyPacket(seq, protocol.ERROR, payload)
}

func (c *Client) replyPacket(seq, cmd uint32, payload []byte) {
	c.writes <- protocol.Packet{Seq: seq, Cmd: cmd, Payload: payload}
}

func (c *Client) replyMessage(seq, cmd uint32, msg proto.Message) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		c.logger.Errorf("client[%d, %s] fail to marshal message %s: %s", c.id, c.addr, proto.MessageName(msg), msg)
		return
	}
	c.writes <- protocol.Packet{Seq: seq, Cmd: cmd, Payload: payload}
}

func (c *Client) waitOkResponse(seq uint32, info fmt.Stringer, reply chan interface{}) {
	switch result := c.waitResult(reply, info, time.Second).(type) {
	case error:
		c.replyError(seq, result)
	default:
		c.replyOk(seq)
	}
	c.asyncs.Done()
}

func (c *Client) waitGetResponse(seq uint32, msg *protocol.GetRequest, reply chan interface{}) {
	switch result := c.waitResult(reply, msg, time.Second).(type) {
	case error:
		c.replyError(seq, result)
	default:
		value, err := protocol.Marshal(result)
		if err != nil {
			c.replyError(seq, err)
			break
		}
		c.replyMessage(seq, protocol.GET, &protocol.GetResponse{value})
	}
	c.asyncs.Done()
}

var handlers = make(map[uint32]reflect.Value)

func regMessageHandler(cmd uint32, callback interface{}) {
	handlers[cmd] = reflect.ValueOf(callback)
}

func init() {
	regMessageHandler(protocol.GET, (*Client).handleCommandGet)
	regMessageHandler(protocol.SET, (*Client).handleCommandSet)
	regMessageHandler(protocol.DELETE, (*Client).handleCommandDelete)
	regMessageHandler(protocol.CACHE, (*Client).handleCommandCache)
	regMessageHandler(protocol.TOUCH, (*Client).handleCommandTouch)
	regMessageHandler(protocol.OPEN, (*Client).handleCommandOpenDB)
	regMessageHandler(protocol.CLOSE, (*Client).handleCommandCloseDB)
}

func (c *Client) call(f reflect.Value, seq uint32, msg interface{}) error {
	results := f.Call([]reflect.Value{reflect.ValueOf(c), reflect.ValueOf(seq), reflect.ValueOf(msg)})
	if len(results) == 0 {
		return nil
	}
	switch result := results[0].Interface().(type) {
	case nil:
		return nil
	case error:
		return result.(error)
	default:
		return fmt.Errorf("unexpected return type from message handling: %s", reflect.TypeOf(result))
	}
}

var (
	errDecoderNone     = errors.New("no protocol decoder selected")
	errDecoderSelected = errors.New("protocol decoder selected")
)

func (c *Client) handlePacket(seq, cmd uint32, payload []byte) error {
	switch {
	case cmd == protocol.HANDSHAKE:
		return c.handleHandshake(seq, payload)
	case c.decoder == nil:
		return errDecoderNone
	}
	msg, err := c.decoder.Unmarshal(int64(cmd), payload)
	if err != nil {
		c.replyEcode(seq, protocol.EcodeInvalidMessage)
		return nil
	}
	f, ok := handlers[cmd]
	if !ok {
		c.replyEcode(seq, protocol.EcodeUnknownRequest)
		return nil
	}
	return c.call(f, seq, msg)
}

func (c *Client) handleCommandGet(seq uint32, msg *protocol.GetRequest) error {
	db := c.databases.Get(msg.Db)
	if db == nil {
		c.replyEcode(seq, protocol.EcodeDBClosed)
		return nil
	}
	var cmd treedb.GetCommand
	if !cmd.Path.Init(msg.Path) {
		c.replyEcode(seq, protocol.EcodeInvalidPath)
		return nil
	}
	switch msg.Type {
	case protocol.FieldAny, protocol.FieldTree, protocol.FieldBinary:
		cmd.Type = treedb.FieldType(msg.Type)
	default:
		c.replyEcode(seq, protocol.EcodeInvalidParam)
		return nil
	}
	reply := c.chans.Get().(chan interface{})
	db.Request(reply, &cmd)
	c.asyncs.Add(1)
	go c.waitGetResponse(seq, msg, reply)
	return nil
}

func (c *Client) handleCommandSet(seq uint32, msg *protocol.SetRequest) error {
	db := c.databases.Get(msg.Db)
	if db == nil {
		c.replyEcode(seq, protocol.EcodeDBClosed)
		return nil
	}
	var cmd treedb.SetCommand
	if !cmd.Path.Init(msg.Path) {
		c.replyEcode(seq, protocol.EcodeInvalidPath)
		return nil
	}
	value, err := protocol.Unmarshal(msg.Value)
	if err != nil {
		c.replyEcode(seq, protocol.EcodeInvalidValue)
		return nil
	}
	cmd.Value = value
	reply := c.chans.Get().(chan interface{})
	db.Request(reply, &cmd)
	c.asyncs.Add(1)
	go c.waitOkResponse(seq, msg, reply)
	return nil
}

func (c *Client) handleCommandDelete(seq uint32, msg *protocol.DeleteRequest) error {
	db := c.databases.Get(msg.Db)
	if db == nil {
		c.replyEcode(seq, protocol.EcodeDBClosed)
		return nil
	}
	var cmd treedb.DeleteCommand
	if !cmd.Path.Init(msg.Path) {
		c.replyEcode(seq, protocol.EcodeInvalidPath)
		return nil
	}
	reply := c.chans.Get().(chan interface{})
	db.Request(reply, &cmd)
	c.asyncs.Add(1)
	go c.waitOkResponse(seq, msg, reply)
	return nil
}

func (c *Client) handleCommandCache(seq uint32, msg *protocol.CacheRequest) error {
	db := c.databases.Get(msg.Db)
	if db == nil {
		c.replyEcode(seq, protocol.EcodeDBClosed)
		return nil
	}
	var cmd treedb.CacheCommand
	if !cmd.Path.InitPattern(msg.Path) {
		c.replyEcode(seq, protocol.EcodeInvalidPath)
		return nil
	}
	cmd.Timeout = msg.Timeout
	reply := c.chans.Get().(chan interface{})
	db.Request(reply, &cmd)
	c.asyncs.Add(1)
	go c.waitOkResponse(seq, msg, reply)
	return nil
}

func (c *Client) handleCommandTouch(seq uint32, msg *protocol.TouchRequest) error {
	db := c.databases.Get(msg.Db)
	if db == nil {
		c.replyEcode(seq, protocol.EcodeDBClosed)
		return nil
	}
	var cmd treedb.TouchCommand
	if !cmd.Path.Init(msg.Path) {
		c.replyEcode(seq, protocol.EcodeInvalidPath)
		return nil
	}
	reply := c.chans.Get().(chan interface{})
	db.Request(reply, &cmd)
	c.asyncs.Add(1)
	go c.waitOkResponse(seq, msg, reply)
	return nil
}

func (c *Client) waitResult(reply chan interface{}, info fmt.Stringer, tick time.Duration) interface{} {
	defer c.chans.Put(reply)
	if tick == 0 {
		return <-reply
	}

	var n int
	var elapsed time.Duration
	logging := c.logger.Warnf
	for {
		select {
		case result := <-reply:
			return result
		case <-time.After(tick):
			n++
			elapsed += tick
			if n == 5 {
				logging = c.logger.Errorf
			}
			logging("client[%d, %s]: %s wait response, elapsed %s", c.id, c.addr, info, elapsed)
			tick += tick / 2
		}
	}
}

func (c *Client) waitOpenDBResponse(seq uint32, msg *protocol.OpenDBRequest, reply chan interface{}) {
	switch result := c.waitResult(reply, msg, time.Second).(type) {
	case error:
		c.replyError(seq, result)
	case *treedb.DB:
		id := c.databases.Put(result)
		c.replyMessage(seq, protocol.OPEN, &protocol.OpenDBResponse{Db: id, Version: result.Version})
	default:
		c.replyEcode(seq, protocol.EcodeInternalError)
		c.logger.Errorf("client[%d, %s] unexpected OpenDB response of Go type: %s", c.id, c.addr, reflect.TypeOf(result))
	}
	c.asyncs.Done()
}

func (c *Client) handleCommandOpenDB(seq uint32, msg *protocol.OpenDBRequest) error {
	var options treedb.OpenOptions
	options.TemplateDB = msg.Template
	options.CreateIfMissing = msg.CreateIfMissing
	options.ErrorIfExists = msg.ErrorIfExists
	reply := c.chans.Get().(chan interface{})
	c.dir.Open(msg.Name, &options, reply)
	c.asyncs.Add(1)
	go c.waitOpenDBResponse(seq, msg, reply)
	return nil
}

func (c *Client) handleCommandCloseDB(seq uint32, msg *protocol.CloseDBRequest) error {
	switch c.databases.Delete(msg.Db) {
	case true:
		c.replyOk(seq)
	default:
		c.replyEcode(seq, protocol.EcodeDBClosed)
	}
	return nil
}

func (c *Client) handleHandshake(seq uint32, payload []byte) error {
	if c.decoder != nil {
		return errDecoderSelected
	}
	var handshake protocol.HandshakeRequest
	err := handshake.Unmarshal(payload)
	if err != nil {
		return err
	}
	c.replyPacket(seq, protocol.HANDSHAKE, tip.HandshakeReplyPayload)
	for _, v := range handshake.Versions {
		if v == tip.Version {
			c.decoder = &tip.Decoder
			return nil
		}
	}
	return ErrIncompatibleVersion
}

func NewClient(id uint64, conn connection, dir *treedb.Directory, logger *logrus.Logger, disconnected chan uint64) *Client {
	c := &Client{
		id:           id,
		dir:          dir,
		conn:         conn,
		addr:         conn.RemoteAddr(),
		logger:       logger,
		closed:       make(chan struct{}),
		databases:    newDBDict(),
		disconnected: disconnected,
	}
	c.chans.New = func() interface{} { return make(chan interface{}, 1) }
	return c
}

func (c *Client) Shutdown() {
	c.logger.Infof("client[%d, %s] shutting down", c.id, c.addr)
	c.conn.CloseRead()
	<-c.closed
	c.conn.Close()
	c.logger.Infof("client[%d, %s] shut down", c.id, c.addr)
}
