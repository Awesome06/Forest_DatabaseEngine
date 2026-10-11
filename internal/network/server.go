// Package network implements the zero-copy, binary TCP protocol for the Forest engine.
// It is designed to read packets directly from socket buffers to eliminate heap allocations
// and Garbage Collection (GC) pauses during high-throughput workloads.
// It supports mutual TLS (mTLS) encrypted node communication and concurrent UDP telemetry streams.
package network

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Forest_DatabaseEngine/internal/metrics"
)

// ServerOption configures properties of the Network Server.
type ServerOption func(*Server)

// WithTLS configures mutual TLS or server TLS for encrypted node communication.
func WithTLS(cfg *tls.Config) ServerOption {
	return func(s *Server) {
		s.tlsConfig = cfg
	}
}

// WithUDP configures concurrent UDP socket listener address for loss-tolerant telemetry.
func WithUDP(udpAddr string) ServerOption {
	return func(s *Server) {
		s.udpAddr = udpAddr
	}
}

// WithMetrics overrides the default engine metrics bundle.
func WithMetrics(m *metrics.EngineMetrics) ServerOption {
	return func(s *Server) {
		if m != nil {
			s.metrics = m
		}
	}
}

// TelemetryHandler defines the callback for received UDP datagrams.
type TelemetryHandler func(op OpCode, payload []byte, from net.Addr)

// WithTelemetryHandler registers a custom handler for incoming UDP telemetry packets.
func WithTelemetryHandler(fn TelemetryHandler) ServerOption {
	return func(s *Server) {
		s.telemetryFn = fn
	}
}

// Server handles incoming TCP connections (plaintext or mTLS) and concurrent UDP telemetry datagrams.
type Server struct {
	addr        string
	udpAddr     string
	db          Storage
	listener    net.Listener
	udpConn     *net.UDPConn
	tlsConfig   *tls.Config
	metrics     *metrics.EngineMetrics
	mu          sync.Mutex
	stopCh      chan struct{}
	wg          sync.WaitGroup
	telemetryFn TelemetryHandler
}

// NewServer initializes a new Server bound to the given address.
func NewServer(addr string, db Storage, opts ...ServerOption) *Server {
	s := &Server{
		addr:    addr,
		db:      db,
		metrics: metrics.DefaultEngineMetrics,
		stopCh:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start opens network listeners and begins processing client requests and telemetry streams.
// This method blocks until the server is stopped or encounters a fatal error on the primary TCP listener.
func (s *Server) Start() error {
	var listener net.Listener
	var err error

	if s.tlsConfig != nil {
		listener, err = tls.Listen("tcp", s.addr, s.tlsConfig)
	} else {
		listener, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		return fmt.Errorf("failed to bind TCP listener on %s: %w", s.addr, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	// Launch UDP listener if configured
	if s.udpAddr != "" {
		uAddr, err := net.ResolveUDPAddr("udp", s.udpAddr)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("failed to resolve UDP address %s: %w", s.udpAddr, err)
		}
		udpConn, err := net.ListenUDP("udp", uAddr)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("failed to bind UDP listener on %s: %w", s.udpAddr, err)
		}

		s.mu.Lock()
		s.udpConn = udpConn
		s.mu.Unlock()

		s.wg.Add(1)
		go s.listenUDP()
		log.Printf("Forest Database Engine UDP telemetry listening on %s\n", s.udpAddr)
	}

	protoName := "TCP"
	if s.tlsConfig != nil {
		protoName = "mTLS TCP"
	}
	log.Printf("Forest Database Engine %s server listening on %s\n", protoName, s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || isClosedNetworkError(err) {
				return nil
			}
			return fmt.Errorf("failed to accept TCP connection: %w", err)
		}

		go s.handleConnection(conn)
	}
}

var udpBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 65535)
		return &b
	},
}

// listenUDP handles concurrent UDP telemetry streams using pooled buffers and non-blocking drops.
func (s *Server) listenUDP() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		bufPtr := udpBufPool.Get().(*[]byte)
		n, from, err := s.udpConn.ReadFromUDP(*bufPtr)
		if err != nil {
			udpBufPool.Put(bufPtr)
			if errors.Is(err, net.ErrClosed) || isClosedNetworkError(err) {
				return
			}
			continue
		}

		s.metrics.UDPPacketsTotal.Inc()

		op, payload, err := ParseTelemetryPacket((*bufPtr)[:n])
		if err != nil {
			s.metrics.UDPPacketsDropped.Inc()
			udpBufPool.Put(bufPtr)
			continue
		}

		if s.telemetryFn != nil {
			t0 := time.Now()
			s.telemetryFn(op, payload, from)
			s.metrics.UDPTelemetryDuration.Observe(time.Since(t0).Seconds())
		}

		udpBufPool.Put(bufPtr)
	}
}

// isClosedNetworkError is a fallback helper to detect closed listeners.
func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "use of closed network connection" ||
		errors.Is(err, net.ErrClosed)
}

// Stop closes active listeners and signals background goroutines to drain.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopCh:
		return nil
	default:
		close(s.stopCh)
	}

	var firstErr error

	if s.listener != nil {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !isClosedNetworkError(err) {
			firstErr = err
		}
		s.listener = nil
	}

	if s.udpConn != nil {
		if err := s.udpConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !isClosedNetworkError(err) {
			if firstErr == nil {
				firstErr = err
			}
		}
		s.udpConn = nil
	}

	s.wg.Wait()
	return firstErr
}

// handleConnection manages the lifecycle of a single client TCP socket.
// It utilizes pre-allocated buffers to maintain a zero-allocation hot path.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	headerBuf := make([]byte, HeaderSize)
	payloadBuf := make([]byte, 4*1024*1024)

	for {
		t0 := time.Now()
		header, err := ParseHeader(conn, headerBuf)
		if err != nil {
			return
		}

		if err := HandleRequest(conn, header, payloadBuf, s.db, s.metrics); err != nil {
			log.Printf("critical: failed to handle request for operation %v: %v", header.Op, err)
			return
		}

		s.metrics.TCPParseDuration.Observe(time.Since(t0).Seconds())
	}
}
