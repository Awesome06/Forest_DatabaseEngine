// Package network implements the zero-copy, binary TCP protocol for the Forest engine.
// It is designed to read packets directly from socket buffers to eliminate heap allocations
// and Garbage Collection (GC) pauses during high-throughput workloads.
package network

import (
	"errors"
	"fmt"
	"log"
	"net"
)

// Server handles incoming TCP connections and routes them to the storage engine.
type Server struct {
	addr     string
	db       Storage
	listener net.Listener
}

// NewServer initializes a new TCP server bound to the given address.
func NewServer(addr string, db Storage) *Server {
	return &Server{
		addr: addr,
		db:   db,
	}
}

// Start opens the TCP listener and begins accepting client connections.
// This method blocks until the server is stopped or encounters a fatal error.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind TCP listener on %s: %w", s.addr, err)
	}
	
	s.listener = listener
	log.Printf("Forest Database Engine TCP server listening on %s\n", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// If the server was stopped, the listener is closed and we should gracefully exit.
			if errors.Is(err, net.ErrClosed) || isClosedNetworkError(err) {
				return nil
			}
			// For all other errors, we return which acts as a fatal error for the server loop.
			return fmt.Errorf("failed to accept TCP connection: %w", err)
		}
		
		// Handle each client concurrently.
		go s.handleConnection(conn)
	}
}

// isClosedNetworkError is a fallback helper to detect closed listeners.
func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "use of closed network connection" || err.Error() == "accept tcp 127.0.0.1:9002: use of closed network connection"
}

// Stop closes the active TCP listener, signaling the Start loop to exit gracefully.
func (s *Server) Stop() error {
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close TCP listener: %w", err)
		}
	}
	return nil
}

// handleConnection manages the lifecycle of a single client TCP socket.
// It utilizes pre-allocated buffers to maintain a zero-allocation hot path.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Pre-allocate buffers for the connection lifecycle to strictly avoid
	// heap allocations (mallocgc) in the hot request/response loop.
	headerBuf := make([]byte, HeaderSize)
	// 4MB max payload for simplicity, matching the MemTable flush threshold.
	payloadBuf := make([]byte, 4*1024*1024) 

	for {
		header, err := ParseHeader(conn, headerBuf)
		if err != nil {
			// Expected client disconnects (EOF) or short reads terminate the session cleanly.
			// Malformed packets also drop the connection. We avoid logging in the hot path
			// to prevent IO bottlenecks during heavy loads or chaos testing.
			return 
		}

		if err := HandleRequest(conn, header, payloadBuf, s.db); err != nil {
			// Log critical storage/engine errors, but avoid logging routine connection drops.
			log.Printf("critical: failed to handle request for operation %v: %v", header.Op, err)
			return
		}
	}
}
