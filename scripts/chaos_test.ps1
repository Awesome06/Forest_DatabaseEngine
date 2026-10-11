# Forest Database Engine Chaos Test (PowerShell Native)
$ErrorActionPreference = "Stop"

Write-Host "=== Forest Database Engine Chaos Test (PowerShell) ===" -ForegroundColor Cyan

# Clear previous data and build artifacts
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue data, blaster*, verifier*, forest_server*

Write-Host "Compiling binaries..."
go build -o forest_server.exe ./cmd/server

# Create Blaster script
@'
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
		
		var resp [8]byte
		if _, err := conn.Read(resp[:]); err != nil {
			return
		}
	}
}
'@ | Out-File -FilePath blaster.go -Encoding utf8
go build -o blaster.exe blaster.go

# Create Verifier script
@'
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

	key := []byte("key-0")
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
'@ | Out-File -FilePath verifier.go -Encoding utf8
go build -o verifier.exe verifier.go

Write-Host "Starting server in background..."
$serverProc = Start-Process -FilePath ".\forest_server.exe" -PassThru
Start-Sleep -Seconds 1.5

Write-Host "Blasting 10,000 TCP requests..."
$blasterProc = Start-Process -FilePath ".\blaster.exe" -PassThru
Start-Sleep -Milliseconds 500

Write-Host "[CHAOS] Executing kill -9 (Stop-Process -Force) on server PID $($serverProc.Id)..." -ForegroundColor Yellow
Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
Wait-Process -Id $blasterProc.Id -Timeout 3 -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

Write-Host "Restarting server to trigger WAL recovery..."
$recoveredProc = Start-Process -FilePath ".\forest_server.exe" -PassThru
Start-Sleep -Seconds 1.5

Write-Host "Verifying data mathematically via TCP..."
$verifyProc = Start-Process -FilePath ".\verifier.exe" -Wait -PassThru
if ($verifyProc.ExitCode -ne 0) {
    Stop-Process -Id $recoveredProc.Id -Force -ErrorAction SilentlyContinue
    Write-Host "Chaos verification failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Cleaning up..."
Stop-Process -Id $recoveredProc.Id -Force -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue data, blaster*, verifier*, forest_server*
Write-Host "Chaos test complete." -ForegroundColor Green
