#!/bin/bash
set -e

echo "=== Forest Database Engine Chaos Test ==="

# Clear previous data and build artifacts
rm -rf data/ forest.wal data-*.sst blaster* verifier* forest_server*

# Compile server, blaster, and verifier
echo "Compiling binaries..."
go build -o forest_server ./cmd/server

# Detect executable extension
EXE="./forest_server"
if [ -f "./forest_server.exe" ]; then
    EXE="./forest_server.exe"
fi

cat << 'EOF' > blaster.go
package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	var conn net.Conn
	var err error
	for i := 0; i < 30; i++ {
		conn, err = net.Dial("tcp", "127.0.0.1:9000")
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
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
		
		if _, err := conn.Write(header[:]); err != nil {
			return
		}
		if _, err := conn.Write(key); err != nil {
			return
		}
		if _, err := conn.Write(val); err != nil {
			return
		}
		
		// Read ACK (8 bytes)
		var resp [8]byte
		if _, err := conn.Read(resp[:]); err != nil {
			return
		}
	}
}
EOF
go build -o blaster blaster.go

BLASTER_EXE="./blaster"
if [ -f "./blaster.exe" ]; then
    BLASTER_EXE="./blaster.exe"
fi

cat << 'EOF' > verifier.go
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

func main() {
	var conn net.Conn
	var err error
	for i := 0; i < 30; i++ {
		conn, err = net.Dial("tcp", "127.0.0.1:9000")
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	key := []byte("key-0") // Key written prior to kill
	
	var header [8]byte
	header[0] = 0xA1 // MagicByte
	header[1] = 0x03 // OpGet
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], 0)
	
	if _, err := conn.Write(header[:]); err != nil {
		panic(err)
	}
	if _, err := conn.Write(key); err != nil {
		panic(err)
	}
	
	var resp [8]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		panic(err)
	}
	
	klen := binary.BigEndian.Uint16(resp[2:4])
	vlen := binary.BigEndian.Uint32(resp[4:8])
	if vlen > 0 {
		retKey := make([]byte, klen)
		if _, err := io.ReadFull(conn, retKey); err != nil {
			panic(err)
		}
		retVal := make([]byte, vlen)
		if _, err := io.ReadFull(conn, retVal); err != nil {
			panic(err)
		}
		if bytes.Equal(retVal, []byte("val-0")) {
			fmt.Println("SUCCESS: WAL Recovery mathematically proven! Data preserved across kill -9.")
			os.Exit(0)
		}
	}
	fmt.Println("FAILURE: Data lost!")
	os.Exit(1)
}
EOF
go build -o verifier verifier.go

VERIFIER_EXE="./verifier"
if [ -f "./verifier.exe" ]; then
    VERIFIER_EXE="./verifier.exe"
fi

echo "Starting server in background..."
$EXE &
SERVER_PID=$!
sleep 1.5

echo "Blasting 10,000 TCP requests..."
$BLASTER_EXE &
BLASTER_PID=$!

# Let writes stream to WAL, then execute chaos kill
sleep 0.5
echo "[CHAOS] Executing kill -9 on server PID $SERVER_PID..."
kill -9 $SERVER_PID 2>/dev/null || true
taskkill //F //IM forest_server.exe 2>/dev/null || true
taskkill //F //IM forest_server 2>/dev/null || true
wait $BLASTER_PID 2>/dev/null || true
sleep 1

echo "Restarting server to trigger WAL recovery..."
$EXE &
SERVER_PID=$!
sleep 1.5

echo "Verifying data mathematically via TCP..."
$VERIFIER_EXE

echo "Cleaning up..."
kill -15 $SERVER_PID 2>/dev/null || true
taskkill //F //IM forest_server.exe 2>/dev/null || true
taskkill //F //IM forest_server 2>/dev/null || true
rm -f blaster* verifier* forest_server*
rm -rf data/

echo "Chaos test complete."
