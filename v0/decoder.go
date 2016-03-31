package v0

import "github.com/kezhuw/treedb/protocol"

var Decoder protocol.Decoder

func init() {
	Decoder.Register(protocol.GET, (*protocol.GetResponse)(nil))
	Decoder.Register(protocol.OPEN, (*protocol.OpenDBResponse)(nil))

	Decoder.Register(protocol.OK, (*protocol.OkResponse)(nil))
	Decoder.Register(protocol.ERROR, (*protocol.ErrorResponse)(nil))
	Decoder.Register(protocol.HANDSHAKE, (*protocol.HandshakeResponse)(nil))
}
