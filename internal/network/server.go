package network

import (
	"log"
	"net"
)

type Server struct {
	addr string
	db   Storage
	l    net.Listener
}

func NewServer(addr string, db Storage) *Server {
	return &Server{addr: addr, db: db}
}

func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.l = l
	log.Printf("Forest Database Engine TCP server listening on %s\n", s.addr)

	for {
		conn, err := s.l.Accept()
		if err != nil {
			// If the server was stopped, the listener is closed and we should gracefully exit
			if err == net.ErrClosed || err.Error() == "use of closed network connection" {
				return nil
			}
			// In Go 1.16+, net.ErrClosed is the standard, but we fallback to string matching just in case
			if err != nil && (err.Error() == "use of closed network connection" || err.Error() == "accept tcp 127.0.0.1:9002: use of closed network connection") {
				return nil
			}
			// For all other errors, we can log and return (or continue if transient)
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	if s.l != nil {
		return s.l.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Pre-allocate buffer to strictly avoid allocations in the hot loop
	headerBuf := make([]byte, HeaderSize)
	payloadBuf := make([]byte, 4*1024*1024) // 4MB max payload for simplicity

	for {
		header, err := ParseHeader(conn, headerBuf)
		if err != nil {
			if err.Error() == "EOF" {
				return // Client disconnected normally
			}
			log.Printf("Error parsing header: %v", err)
			return
		}

		if err := HandleRequest(conn, header, payloadBuf, s.db); err != nil {
			log.Printf("Error handling request: %v", err)
			return
		}
	}
}
