package tip

import "github.com/kezhuw/treedb/protocol"

var Decoder protocol.Decoder

func init() {
	Decoder.Register(protocol.GET, (*protocol.GetRequest)(nil))
	Decoder.Register(protocol.SET, (*protocol.SetRequest)(nil))
	Decoder.Register(protocol.DELETE, (*protocol.DeleteRequest)(nil))
	Decoder.Register(protocol.OPEN, (*protocol.OpenDBRequest)(nil))
	Decoder.Register(protocol.CLOSE, (*protocol.CloseDBRequest)(nil))
	Decoder.Register(protocol.CACHE, (*protocol.CacheRequest)(nil))
	Decoder.Register(protocol.TOUCH, (*protocol.TouchRequest)(nil))
}
