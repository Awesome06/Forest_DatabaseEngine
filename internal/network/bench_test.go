package network_test

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
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
	
	os.RemoveAll("bench_test.wal")
	os.RemoveAll("data-L0-bench.sst")
	defer os.RemoveAll("bench_test.wal")

	db, err := engine.NewDB("bench_test.wal")
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
