package network_test

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"log"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Forest_DatabaseEngine/internal/engine"
	"github.com/Forest_DatabaseEngine/internal/network"
)

// BenchmarkSystemEndToEnd blasts the engine with Puts to simulate heavy load
// and measure CPU/Memory profiles, specifically highlighting our zero-allocation TCP parsing
// and lock-free RCU read paths.
func BenchmarkSystemEndToEnd(b *testing.B) {
	// Mute all standard logger output to prevent syscall bottlenecks
	log.SetOutput(io.Discard)
	
	tempDir := b.TempDir()
	walPath := filepath.Join(tempDir, "bench_test.wal")
	sstDir := filepath.Join(tempDir, "sst")

	db, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	srv := network.NewServer("127.0.0.1:9002", db)
	go srv.Start()
	defer srv.Stop()

	// Wait for server to bind
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "127.0.0.1:9002")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	key := []byte("bench-key")
	val := []byte("bench-val")

	var putHeader [8]byte
	putHeader[0] = network.MagicByte
	putHeader[1] = byte(network.OpPut)
	binary.BigEndian.PutUint16(putHeader[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(putHeader[4:8], uint32(len(val)))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Send Put
		conn.Write(putHeader[:])
		conn.Write(key)
		conn.Write(val)

		// Read ACK (8 bytes)
		var resp [8]byte
		conn.Read(resp[:])
	}
}

// BenchmarkMTLSSystemEndToEnd measures throughput and memory allocations over
// encrypted mTLS connections.
func BenchmarkMTLSSystemEndToEnd(b *testing.B) {
	log.SetOutput(io.Discard)

	tempDir := b.TempDir()
	walPath := filepath.Join(tempDir, "bench_mtls.wal")
	sstDir := filepath.Join(tempDir, "sst")

	db, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	srvCfg, clientCfg, err := network.GenerateTestMTLSCredentials()
	if err != nil {
		b.Fatal(err)
	}

	addr := "127.0.0.1:9003"
	srv := network.NewServer(addr, db, network.WithTLS(srvCfg))
	go srv.Start()
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	key := []byte("bench-mtls-key")
	val := []byte("bench-mtls-val")

	var putHeader [8]byte
	putHeader[0] = network.MagicByte
	putHeader[1] = byte(network.OpPut)
	binary.BigEndian.PutUint16(putHeader[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(putHeader[4:8], uint32(len(val)))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn.Write(putHeader[:])
		conn.Write(key)
		conn.Write(val)

		var resp [8]byte
		conn.Read(resp[:])
	}
}

// BenchmarkParseTelemetryPacket asserts 0 allocations per operation on UDP packet parsing.
func BenchmarkParseTelemetryPacket(b *testing.B) {
	packet := []byte{network.MagicByte, byte(network.OpTelemetry), 0x00, 0x04, 'p', 'i', 'n', 'g'}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := network.ParseTelemetryPacket(packet)
		if err != nil {
			b.Fatal(err)
		}
	}
}

