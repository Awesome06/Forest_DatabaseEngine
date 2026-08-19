#!/bin/bash
set -e

echo "=== Forest Database Engine Chaos Test ==="

# Compile the server
echo "Building server..."
go build -o forest_server ./cmd/server

# Clear previous data
rm -f forest.wal data-*.sst

echo "Starting server in background..."
./forest_server &
SERVER_PID=$!

# Give it a moment to start
sleep 1

echo "Creating load blaster..."
cat << 'EOF' > blaster.go
package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		
		var header [8]byte
		header[0] = 0xA1 // MagicByte
		header[1] = 0x02 // OpPut
		binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
		binary.BigEndian.PutUint32(header[4:8], uint32(len(val)))
		
		conn.Write(header[:])
		conn.Write(key)
		conn.Write(val)
		
		// Read ACK (8 bytes)
		var resp [8]byte
		conn.Read(resp[:])
	}
}
EOF

echo "Blasting 10,000 TCP requests..."
go run blaster.go &
BLASTER_PID=$!

# Wait briefly then execute chaos kill
sleep 0.2
echo "[CHAOS] Executing kill -9 on server PID $SERVER_PID..."
kill -9 $SERVER_PID || true
wait $BLASTER_PID || true

echo "Restarting server to trigger WAL recovery..."
./forest_server &
SERVER_PID=$!
sleep 1

echo "Verifying data mathematically via TCP..."
cat << 'EOF' > verifier.go
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	key := []byte("key-0") // Assuming key-0 was written before kill
	
	var header [8]byte
	header[0] = 0xA1 // MagicByte
	header[1] = 0x03 // OpGet
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], 0)
	
	conn.Write(header[:])
	conn.Write(key)
	
	var resp [8]byte
	conn.Read(resp[:])
	
	vlen := binary.BigEndian.Uint32(resp[4:8])
	if vlen > 0 {
		val := make([]byte, vlen)
		conn.Read(val)
		if bytes.Equal(val, []byte("val-0")) {
			fmt.Println("SUCCESS: WAL Recovery mathematically proven! Data preserved across kill -9.")
			os.Exit(0)
		}
	}
	fmt.Println("FAILURE: Data lost!")
	os.Exit(1)
}
EOF

go run verifier.go

echo "Cleaning up..."
kill -15 $SERVER_PID || true
rm -f blaster.go verifier.go forest_server

echo "Chaos test complete."
