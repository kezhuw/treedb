package tip

import "github.com/kezhuw/treedb/protocol"

const Version = 0

var HandshakeReplyPayload = func() []byte {
	var msg protocol.HandshakeResponse
	msg.Version = Version
	payload, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	return payload
}()
