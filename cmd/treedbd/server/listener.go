package server

import (
	"fmt"
	"net"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/kezhuw/neterrs"
)

type listener struct {
	addr     string
	logger   *logrus.Logger
	clients  chan<- connection
	listener net.Listener
	closed   chan struct{}
}

func listen(addr string, logger *logrus.Logger, clients chan<- connection) (*listener, error) {
	i := strings.Index(addr, "://")
	if i == -1 {
		return nil, fmt.Errorf("invalid listen address: %s", addr)
	}
	l, err := net.Listen(addr[:i], addr[i+len("://"):])
	if err != nil {
		return nil, err
	}
	return &listener{
		addr:     addr,
		logger:   logger,
		clients:  clients,
		listener: l,
		closed:   make(chan struct{}),
	}, nil
}

func (l *listener) Serve() {
	defer close(l.closed)
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if !neterrs.IsClosed(err) {
				l.logger.Errorf("listener[%s] accept error: %s", l.addr, err)
			}
			break
		}
		l.logger.Infof("listener[%s] new client come from %s", l.addr, conn.RemoteAddr())
		l.clients <- conn.(connection)
	}
}

func (l *listener) Close() {
	l.listener.Close()
	<-l.closed
}
