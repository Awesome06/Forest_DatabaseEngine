// Package client provides an idiomatic, high-performance Go SDK for interacting with
// the Forest Database Engine over both guaranteed TCP (plaintext and mTLS) and loss-tolerant UDP streams.
package client

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

var (
	// ErrConnectionClosed is returned when an operation is attempted on a closed client.
	ErrConnectionClosed = errors.New("client connection is closed")
	// ErrKeyNotFound indicates that the requested key does not exist or was deleted.
	ErrKeyNotFound = errors.New("key not found in storage engine")
	// ErrUnexpectedResponse is returned when the server returns a malformed response header.
	ErrUnexpectedResponse = errors.New("received unexpected response from server")
	// ErrInvalidServerMagic is returned when a packet lacks the expected protocol magic byte.
	ErrInvalidServerMagic = errors.New("invalid protocol magic byte received from server")
)

// Option configures optional parameters for the Forest client.
type Option func(*Client)

// WithTLS configures mutual TLS (mTLS) or TLS encryption for TCP cluster connections.
func WithTLS(tlsConfig *tls.Config) Option {
	return func(c *Client) {
		c.tlsConfig = tlsConfig
	}
}

// WithUDP sets the remote UDP address for loss-tolerant telemetry and heartbeat streaming.
func WithUDP(udpAddr string) Option {
	return func(c *Client) {
		c.udpAddr = udpAddr
	}
}

// WithTimeout configures network dial and I/O deadlines for requests.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// Client manages persistent TCP connections and optional UDP telemetry channels to a Forest cluster node.
// All TCP operations are thread-safe and serialized through an internal mutex.
type Client struct {
	tcpAddr   string
	udpAddr   string
	tlsConfig *tls.Config
	timeout   time.Duration

	mu      sync.Mutex
	tcpConn net.Conn
	udpConn net.Conn
	closed  bool
}

// New creates a new Forest client and establishes the primary TCP connection.
// If WithTLS is provided, a secure TLS 1.3 handshake is performed.
// If WithUDP is provided, a UDP client socket is initialized for background telemetry.
func New(tcpAddr string, opts ...Option) (*Client, error) {
	c := &Client{
		tcpAddr: tcpAddr,
		timeout: 5 * time.Second,
	}

	for _, opt := range opts {
		opt(c)
	}

	if err := c.connectTCP(); err != nil {
		return nil, fmt.Errorf("failed to establish TCP connection to %s: %w", tcpAddr, err)
	}

	if c.udpAddr != "" {
		uConn, err := net.Dial("udp", c.udpAddr)
		if err != nil {
			_ = c.tcpConn.Close()
			return nil, fmt.Errorf("failed to establish UDP connection to %s: %w", c.udpAddr, err)
		}
		c.udpConn = uConn
	}

	return c, nil
}

// connectTCP dials the server with optional TLS encryption and deadlines.
func (c *Client) connectTCP() error {
	var conn net.Conn
	var err error

	if c.tlsConfig != nil {
		dialer := &net.Dialer{Timeout: c.timeout}
		conn, err = tls.DialWithDialer(dialer, "tcp", c.tcpAddr, c.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", c.tcpAddr, c.timeout)
	}

	if err != nil {
		return err
	}

	c.tcpConn = conn
	return nil
}

// Put writes or updates a key-value record in the engine.
// The mutation is written to the Write-Ahead Log (WAL) and active MemTable before receiving an ACK.
func (c *Client) Put(key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrConnectionClosed
	}

	if c.timeout > 0 {
		_ = c.tcpConn.SetDeadline(time.Now().Add(c.timeout))
	}

	var header [network.HeaderSize]byte
	header[0] = network.MagicByte
	header[1] = byte(network.OpPut)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(value)))

	if _, err := c.tcpConn.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write put header: %w", err)
	}
	if len(key) > 0 {
		if _, err := c.tcpConn.Write(key); err != nil {
			return fmt.Errorf("failed to write put key: %w", err)
		}
	}
	if len(value) > 0 {
		if _, err := c.tcpConn.Write(value); err != nil {
			return fmt.Errorf("failed to write put value: %w", err)
		}
	}

	var ack [network.HeaderSize]byte
	if _, err := io.ReadFull(c.tcpConn, ack[:]); err != nil {
		return fmt.Errorf("failed to read put ack: %w", err)
	}

	if ack[0] != network.MagicByte {
		return ErrInvalidServerMagic
	}

	return nil
}

// Get retrieves the value associated with the specified key.
// It returns the byte slice, a boolean indicating if found, and any network error.
func (c *Client) Get(key []byte) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, false, ErrConnectionClosed
	}

	if c.timeout > 0 {
		_ = c.tcpConn.SetDeadline(time.Now().Add(c.timeout))
	}

	var header [network.HeaderSize]byte
	header[0] = network.MagicByte
	header[1] = byte(network.OpGet)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], 0)

	if _, err := c.tcpConn.Write(header[:]); err != nil {
		return nil, false, fmt.Errorf("failed to write get header: %w", err)
	}
	if len(key) > 0 {
		if _, err := c.tcpConn.Write(key); err != nil {
			return nil, false, fmt.Errorf("failed to write get key: %w", err)
		}
	}

	var resp [network.HeaderSize]byte
	if _, err := io.ReadFull(c.tcpConn, resp[:]); err != nil {
		return nil, false, fmt.Errorf("failed to read get response header: %w", err)
	}

	if resp[0] != network.MagicByte {
		return nil, false, ErrInvalidServerMagic
	}

	retKeyLen := binary.BigEndian.Uint16(resp[2:4])
	if retKeyLen == 0xFFFF {
		// Key not found sentinel
		return nil, false, nil
	}

	retValLen := binary.BigEndian.Uint32(resp[4:8])

	// Read echoing key
	retKey := make([]byte, retKeyLen)
	if _, err := io.ReadFull(c.tcpConn, retKey); err != nil {
		return nil, false, fmt.Errorf("failed to read get response key: %w", err)
	}

	// Read value payload
	retVal := make([]byte, retValLen)
	if _, err := io.ReadFull(c.tcpConn, retVal); err != nil {
		return nil, false, fmt.Errorf("failed to read get response value: %w", err)
	}

	return retVal, true, nil
}

// Delete marks a key for deletion by writing a tombstone record to the engine.
func (c *Client) Delete(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrConnectionClosed
	}

	if c.timeout > 0 {
		_ = c.tcpConn.SetDeadline(time.Now().Add(c.timeout))
	}

	var header [network.HeaderSize]byte
	header[0] = network.MagicByte
	header[1] = byte(network.OpDelete)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], 0)

	if _, err := c.tcpConn.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write delete header: %w", err)
	}
	if len(key) > 0 {
		if _, err := c.tcpConn.Write(key); err != nil {
			return fmt.Errorf("failed to write delete key: %w", err)
		}
	}

	var ack [network.HeaderSize]byte
	if _, err := io.ReadFull(c.tcpConn, ack[:]); err != nil {
		return fmt.Errorf("failed to read delete ack: %w", err)
	}

	if ack[0] != network.MagicByte {
		return ErrInvalidServerMagic
	}

	return nil
}

// Scan performs an ordered range query across the key space [startKey, endKey] in ascending sorted order.
// If startKey is empty, iteration starts from the lowest key.
// If endKey is empty, iteration continues through the highest key.
func (c *Client) Scan(startKey, endKey []byte) ([]network.KeyValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, ErrConnectionClosed
	}

	if c.timeout > 0 {
		_ = c.tcpConn.SetDeadline(time.Now().Add(c.timeout))
	}

	var header [network.HeaderSize]byte
	header[0] = network.MagicByte
	header[1] = byte(network.OpScan)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(startKey)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(endKey)))

	if _, err := c.tcpConn.Write(header[:]); err != nil {
		return nil, fmt.Errorf("failed to write scan header: %w", err)
	}
	if len(startKey) > 0 {
		if _, err := c.tcpConn.Write(startKey); err != nil {
			return nil, fmt.Errorf("failed to write scan startKey: %w", err)
		}
	}
	if len(endKey) > 0 {
		if _, err := c.tcpConn.Write(endKey); err != nil {
			return nil, fmt.Errorf("failed to write scan endKey: %w", err)
		}
	}

	var resp [network.HeaderSize]byte
	if _, err := io.ReadFull(c.tcpConn, resp[:]); err != nil {
		return nil, fmt.Errorf("failed to read scan response header: %w", err)
	}

	if resp[0] != network.MagicByte || resp[1] != byte(network.OpScan) {
		return nil, ErrUnexpectedResponse
	}

	itemCount := int(binary.BigEndian.Uint16(resp[2:4]))
	results := make([]network.KeyValue, 0, itemCount)

	var itemHdr [6]byte
	for i := 0; i < itemCount; i++ {
		if _, err := io.ReadFull(c.tcpConn, itemHdr[:]); err != nil {
			return nil, fmt.Errorf("failed to read scan item %d header: %w", i, err)
		}

		klen := int(binary.BigEndian.Uint16(itemHdr[0:2]))
		vlen := int(binary.BigEndian.Uint32(itemHdr[2:6]))

		itemKey := make([]byte, klen)
		if _, err := io.ReadFull(c.tcpConn, itemKey); err != nil {
			return nil, fmt.Errorf("failed to read scan item %d key: %w", i, err)
		}

		itemVal := make([]byte, vlen)
		if vlen > 0 {
			if _, err := io.ReadFull(c.tcpConn, itemVal); err != nil {
				return nil, fmt.Errorf("failed to read scan item %d value: %w", i, err)
			}
		}

		results = append(results, network.KeyValue{
			Key:   itemKey,
			Value: itemVal,
		})
	}

	return results, nil
}

// Echo tests round-trip network latency with the server, mirroring the payload bytes.
func (c *Client) Echo(payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, ErrConnectionClosed
	}

	if c.timeout > 0 {
		_ = c.tcpConn.SetDeadline(time.Now().Add(c.timeout))
	}

	var header [network.HeaderSize]byte
	header[0] = network.MagicByte
	header[1] = byte(network.OpEcho)
	binary.BigEndian.PutUint16(header[2:4], 0)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))

	if _, err := c.tcpConn.Write(header[:]); err != nil {
		return nil, fmt.Errorf("failed to write echo header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := c.tcpConn.Write(payload); err != nil {
			return nil, fmt.Errorf("failed to write echo payload: %w", err)
		}
	}

	var resp [network.HeaderSize]byte
	if _, err := io.ReadFull(c.tcpConn, resp[:]); err != nil {
		return nil, fmt.Errorf("failed to read echo response header: %w", err)
	}

	respLen := binary.BigEndian.Uint32(resp[4:8])
	res := make([]byte, respLen)
	if _, err := io.ReadFull(c.tcpConn, res); err != nil {
		return nil, fmt.Errorf("failed to read echo payload: %w", err)
	}

	return res, nil
}

// SendHeartbeat sends an ephemeral health heartbeat over the loss-tolerant UDP channel.
func (c *Client) SendHeartbeat() error {
	if c.udpConn == nil {
		return errors.New("UDP channel not configured on this client")
	}

	var packet [network.TelemetryHeaderSize]byte
	packet[0] = network.MagicByte
	packet[1] = byte(network.OpHeartbeat)
	binary.BigEndian.PutUint16(packet[2:4], 0)

	_, err := c.udpConn.Write(packet[:])
	return err
}

// SendTelemetry emits an arbitrary metric or diagnostic payload over the loss-tolerant UDP stream.
func (c *Client) SendTelemetry(payload []byte) error {
	if c.udpConn == nil {
		return errors.New("UDP channel not configured on this client")
	}

	packet := make([]byte, network.TelemetryHeaderSize+len(payload))
	packet[0] = network.MagicByte
	packet[1] = byte(network.OpTelemetry)
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(payload)))
	copy(packet[network.TelemetryHeaderSize:], payload)

	_, err := c.udpConn.Write(packet)
	return err
}

// Close gracefully closes the active TCP connection and UDP socket.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var firstErr error
	if c.tcpConn != nil {
		if err := c.tcpConn.Close(); err != nil {
			firstErr = err
		}
	}
	if c.udpConn != nil {
		if err := c.udpConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
