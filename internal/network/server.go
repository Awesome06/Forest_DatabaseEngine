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
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
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
