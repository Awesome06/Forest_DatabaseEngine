package network

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

// Server represents the TCP server harness.
type Server struct {
	addr     string
	listener net.Listener
}

// NewServer creates a new TCP Server instance.
func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

// Start binds to the socket and accepts incoming connections.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.addr, err)
	}
	s.listener = l
	log.Printf("Forest Database Engine TCP server listening on %s", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("failed to accept connection: %v", err)
			continue
		}

		// Spawn a goroutine per connection.
		// Go's internal netpoller makes this highly efficient.
		go s.handleConnection(conn)
	}
}

// Stop closes the listener and shuts down the server.
func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// handleConnection reads requests in a loop using connection-scoped buffers.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Pre-allocate buffers for the lifetime of this connection to ensure
	// zero heap allocations on the hot path.
	var headerBuf [HeaderSize]byte
	// 4MB max payload buffer. In a real system, we might use a sync.Pool
	// or dynamically size this if payloads exceed a certain threshold,
	// but 4MB is a good conservative max for KV values.
	payloadBuf := make([]byte, 4*1024*1024) 

	for {
		// Read and parse header
		header, err := ParseHeader(conn, headerBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Client closed connection cleanly
				return
			}
			log.Printf("error parsing header: %v", err)
			return
		}

		// Dispatch based on OpCode
		switch header.Op {
		case OpEcho:
			if err := HandleEcho(conn, header, payloadBuf); err != nil {
				log.Printf("error handling echo: %v", err)
				return
			}
		default:
			log.Printf("unknown opcode: %d", header.Op)
			return
		}
	}
}
