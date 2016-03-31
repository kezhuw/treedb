package treedb

import (
	"net"
	"strings"
)

type ServerAddressError struct {
	Addr string
}

func (e *ServerAddressError) Error() string {
	return "treedb: invalid server address: " + e.Addr
}

func dial(addr string) (net.Conn, error) {
	var (
		network string
		address string
	)
	parts := strings.Split(addr, "://")
	switch len(parts) {
	case 1:
		network = "tcp"
		address = parts[0]
	case 2:
		network = parts[0]
		address = parts[1]
	default:
		return nil, &ServerAddressError{addr}
	}
	return net.Dial(network, address)
}
