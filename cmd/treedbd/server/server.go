package server

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/kezhuw/treedb/cmd/treedbd/config"
	"github.com/kezhuw/treedb/cmd/treedbd/treedb"
)

type connection interface {
	net.Conn
	CloseRead() error
	CloseWrite() error
}

type Server struct {
	dir       *treedb.Directory
	netLogger *logrus.Logger

	clientId     uint64
	clients      map[uint64]*Client
	concurrents  int
	disconnected chan uint64

	incomings chan connection
	listeners []*listener

	signalc      chan os.Signal
	shuttingDown bool
}

func Listen(cfg *config.Config) (s *Server, err error) {
	dir, err := treedb.Listen(cfg.DataDir, cfg.DataReadonly)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			dir.Close()
		}
	}()

	netLogger := logrus.New()
	incomings := make(chan connection, 128)
	listeners := make([]*listener, 0, len(cfg.Listens))
	for _, addr := range cfg.Listens {
		l, err := listen(addr, netLogger, incomings)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, l)
	}

	sigc := make(chan os.Signal, 8)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)

	return &Server{
		dir:          dir,
		disconnected: make(chan uint64, 128),
		incomings:    incomings,
		clients:      make(map[uint64]*Client),
		listeners:    listeners,
		netLogger:    netLogger,
		signalc:      sigc,
	}, nil
}

func (s *Server) Serve() {
	defer s.dir.Close()

	for _, l := range s.listeners {
		go l.Serve()
	}

	incomings := s.incomings
	for {
		select {
		case conn, ok := <-incomings:
			switch ok {
			case true:
				s.clientId++
				c := NewClient(s.clientId, conn, s.dir, s.netLogger, s.disconnected)
				c.Start(s.concurrents)
				s.clients[s.clientId] = c
			case false:
				incomings = nil
				go s.closeClients(s.clients)
				s.clients = nil
			}
		case id, ok := <-s.disconnected:
			switch ok {
			case true:
				delete(s.clients, id)
			case false:
				return
			}
		case sig := <-s.signalc:
			s.handleSignal(sig)
		}
	}
}

func (s *Server) handleSignal(sig os.Signal) {
	switch sig {
	case os.Interrupt, os.Kill, syscall.SIGTERM:
		if s.shuttingDown {
			return
		}
		s.shuttingDown = true
		go s.closeListeners(s.listeners)
	}
}

func (s *Server) closeClients(clients map[uint64]*Client) {
	for _, c := range clients {
		c.Shutdown()
	}
	close(s.disconnected)
}

func (s *Server) closeListeners(listeners []*listener) {
	for _, l := range listeners {
		l.Close()
	}
	close(s.incomings)
}
