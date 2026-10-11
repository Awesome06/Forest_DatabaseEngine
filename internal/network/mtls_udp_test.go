package network_test

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Forest_DatabaseEngine/internal/metrics"
	"github.com/Forest_DatabaseEngine/internal/network"
)

type inMemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newInMemoryStorage() *inMemoryStorage {
	return &inMemoryStorage{data: make(map[string][]byte)}
}

func (s *inMemoryStorage) Put(k, v []byte) error {
	s.mu.Lock()
	s.data[string(k)] = append([]byte(nil), v...)
	s.mu.Unlock()
	return nil
}

func (s *inMemoryStorage) Get(k []byte) ([]byte, bool, error) {
	s.mu.RLock()
	val, ok := s.data[string(k)]
	s.mu.RUnlock()
	return val, ok, nil
}

func (s *inMemoryStorage) Delete(k []byte) error {
	s.mu.Lock()
	delete(s.data, string(k))
	s.mu.Unlock()
	return nil
}

func TestMTLSSecureNodeCommunication(t *testing.T) {
	serverCfg, clientCfg, err := network.GenerateTestMTLSCredentials()
	if err != nil {
		t.Fatalf("failed to generate test credentials: %v", err)
	}

	storage := newInMemoryStorage()
	addr := "127.0.0.1:9100"
	server := network.NewServer(addr, storage, network.WithTLS(serverCfg))

	go func() {
		_ = server.Start()
	}()
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// 1. Authorized mTLS client should succeed
	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("authorized mTLS client failed to connect: %v", err)
	}
	defer conn.Close()

	key := []byte("secret-key")
	val := []byte("encrypted-payload")

	// Execute Put
	var putHdr [network.HeaderSize]byte
	putHdr[0] = network.MagicByte
	putHdr[1] = byte(network.OpPut)
	binary.BigEndian.PutUint16(putHdr[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(putHdr[4:8], uint32(len(val)))

	if _, err := conn.Write(putHdr[:]); err != nil {
		t.Fatalf("failed to write put header: %v", err)
	}
	if _, err := conn.Write(append(key, val...)); err != nil {
		t.Fatalf("failed to write put payload: %v", err)
	}

	var ack [network.HeaderSize]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil {
		t.Fatalf("failed to read ACK: %v", err)
	}
	if ack[0] != network.MagicByte {
		t.Fatalf("invalid ACK magic byte")
	}

	// Execute Get
	var getHdr [network.HeaderSize]byte
	getHdr[0] = network.MagicByte
	getHdr[1] = byte(network.OpGet)
	binary.BigEndian.PutUint16(getHdr[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(getHdr[4:8], 0)

	if _, err := conn.Write(getHdr[:]); err != nil {
		t.Fatalf("failed to write get header: %v", err)
	}
	if _, err := conn.Write(key); err != nil {
		t.Fatalf("failed to write get key: %v", err)
	}

	var getResp [network.HeaderSize]byte
	if _, err := io.ReadFull(conn, getResp[:]); err != nil {
		t.Fatalf("failed to read get resp: %v", err)
	}
	retrievedKeyLen := binary.BigEndian.Uint16(getResp[2:4])
	retrievedValLen := binary.BigEndian.Uint32(getResp[4:8])

	retrievedPayload := make([]byte, int(retrievedKeyLen)+int(retrievedValLen))
	if _, err := io.ReadFull(conn, retrievedPayload); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}
	retrievedVal := retrievedPayload[retrievedKeyLen:]
	if !bytes.Equal(retrievedVal, val) {
		t.Fatalf("expected value %s, got %s", val, retrievedVal)
	}

	// 2. Plaintext client without TLS must be rejected
	plainConn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		_ = plainConn.SetDeadline(time.Now().Add(300 * time.Millisecond))
		_, writeErr := plainConn.Write([]byte{network.MagicByte, 0x01, 0, 0, 0, 0, 0, 0})
		var buf [8]byte
		_, readErr := plainConn.Read(buf[:])
		plainConn.Close()
		if writeErr == nil && readErr == nil {
			t.Fatalf("expected plaintext connection to fail over mTLS listener")
		}
	}

	// 3. Client without client certificate must be rejected
	unauthCfg := &tls.Config{
		InsecureSkipVerify: true,
	}
	badConn, err := tls.Dial("tcp", addr, unauthCfg)
	if err == nil {
		var buf [8]byte
		_ = badConn.SetDeadline(time.Now().Add(200 * time.Millisecond))
		_, rErr := badConn.Read(buf[:])
		badConn.Close()
		if rErr == nil {
			t.Fatalf("expected connection without certificate to be rejected by mTLS")
		}
	}
}

func TestDualProtocolTCPAndUDP(t *testing.T) {
	storage := newInMemoryStorage()
	tcpAddr := "127.0.0.1:9101"
	udpAddr := "127.0.0.1:9102"

	receivedUDPTelemetry := make(chan string, 10)

	telemetryHandler := func(op network.OpCode, payload []byte, from net.Addr) {
		if op == network.OpTelemetry {
			receivedUDPTelemetry <- string(payload)
		}
	}

	customMetrics := metrics.NewEngineMetrics()
	server := network.NewServer(
		tcpAddr,
		storage,
		network.WithUDP(udpAddr),
		network.WithTelemetryHandler(telemetryHandler),
		network.WithMetrics(customMetrics),
	)

	go func() {
		_ = server.Start()
	}()
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Send UDP Telemetry packet
	udpConn, err := net.Dial("udp", udpAddr)
	if err != nil {
		t.Fatalf("failed to dial UDP: %v", err)
	}
	defer udpConn.Close()

	telemetryData := []byte("node-health:ok;cpu:12.5;mem:420MB")
	var udpPacket []byte
	udpPacket = append(udpPacket, network.MagicByte)
	udpPacket = append(udpPacket, byte(network.OpTelemetry))
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(telemetryData)))
	udpPacket = append(udpPacket, lenBuf[:]...)
	udpPacket = append(udpPacket, telemetryData...)

	if _, err := udpConn.Write(udpPacket); err != nil {
		t.Fatalf("failed to write UDP telemetry packet: %v", err)
	}

	select {
	case received := <-receivedUDPTelemetry:
		if received != string(telemetryData) {
			t.Fatalf("expected UDP telemetry %s, got %s", telemetryData, received)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for UDP telemetry packet")
	}

	// Send corrupt UDP packet (invalid magic byte) to verify loss-tolerance and drop tracking
	corruptPacket := []byte{0x00, byte(network.OpTelemetry), 0x00, 0x04, 't', 'e', 's', 't'}
	_, _ = udpConn.Write(corruptPacket)

	time.Sleep(50 * time.Millisecond)
	if customMetrics.UDPPacketsDropped.Value() == 0 {
		t.Fatalf("expected UDPPacketsDropped counter to increment on invalid packet")
	}

	// Simultaneously verify TCP ingestion continues unhindered
	tcpConn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatalf("failed to dial TCP during UDP ingestion: %v", err)
	}
	defer tcpConn.Close()

	key := []byte("mission-critical-key")
	val := []byte("guaranteed-data")

	var putHdr [network.HeaderSize]byte
	putHdr[0] = network.MagicByte
	putHdr[1] = byte(network.OpPut)
	binary.BigEndian.PutUint16(putHdr[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(putHdr[4:8], uint32(len(val)))

	if _, err := tcpConn.Write(putHdr[:]); err != nil {
		t.Fatalf("failed to write TCP put header: %v", err)
	}
	if _, err := tcpConn.Write(append(key, val...)); err != nil {
		t.Fatalf("failed to write TCP put payload: %v", err)
	}

	var ack [network.HeaderSize]byte
	if _, err := io.ReadFull(tcpConn, ack[:]); err != nil {
		t.Fatalf("failed to read TCP ACK: %v", err)
	}
	if ack[0] != network.MagicByte {
		t.Fatalf("invalid TCP ACK")
	}
}
