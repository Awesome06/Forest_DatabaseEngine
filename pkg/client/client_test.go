package client_test

import (
	"bytes"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Forest_DatabaseEngine/internal/engine"
	"github.com/Forest_DatabaseEngine/internal/network"
	"github.com/Forest_DatabaseEngine/pkg/client"
)

func TestClientEndToEnd(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "forest.wal")
	sstDir := filepath.Join(tempDir, "sst")

	db, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		t.Fatalf("failed to initialize db: %v", err)
	}
	defer db.Close()

	tcpAddr := "127.0.0.1:9200"
	udpAddr := "127.0.0.1:9201"

	receivedUDP := make(chan string, 5)
	handler := func(op network.OpCode, payload []byte, from net.Addr) {
		if op == network.OpTelemetry {
			receivedUDP <- string(payload)
		} else if op == network.OpHeartbeat {
			receivedUDP <- "heartbeat"
		}
	}

	server := network.NewServer(
		tcpAddr,
		db,
		network.WithUDP(udpAddr),
		network.WithTelemetryHandler(handler),
	)

	go func() {
		_ = server.Start()
	}()
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	// Initialize SDK Client
	cli, err := client.New(
		tcpAddr,
		client.WithUDP(udpAddr),
		client.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	// 1. Echo Test
	echoResp, err := cli.Echo([]byte("ping-test"))
	if err != nil {
		t.Fatalf("Echo failed: %v", err)
	}
	if string(echoResp) != "ping-test" {
		t.Fatalf("expected ping-test, got %s", echoResp)
	}

	// 2. Put & Get
	key := []byte("customer:1001")
	val := []byte("Alice Wonderland")
	if err := cli.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retVal, found, err := cli.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || !bytes.Equal(retVal, val) {
		t.Fatalf("expected found=true and value %s, got %s", val, retVal)
	}

	// 3. Range Scan Test
	keys := []string{"sensor:01", "sensor:02", "sensor:03", "sensor:04", "sensor:05"}
	for _, k := range keys {
		if err := cli.Put([]byte(k), []byte("reading-"+k)); err != nil {
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	scanResults, err := cli.Scan([]byte("sensor:02"), []byte("sensor:04"))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(scanResults) != 3 {
		t.Fatalf("expected 3 scan results, got %d", len(scanResults))
	}
	if string(scanResults[0].Key) != "sensor:02" || string(scanResults[2].Key) != "sensor:04" {
		t.Fatalf("unexpected scan results: %v", scanResults)
	}

	// 4. Delete & Verify Key Not Found
	if err := cli.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, found, err = cli.Get(key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if found {
		t.Fatalf("expected key to be deleted, but was found")
	}

	// 5. UDP Telemetry & Heartbeat
	if err := cli.SendHeartbeat(); err != nil {
		t.Fatalf("SendHeartbeat failed: %v", err)
	}

	select {
	case msg := <-receivedUDP:
		if msg != "heartbeat" {
			t.Fatalf("expected heartbeat, got %s", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for heartbeat")
	}

	if err := cli.SendTelemetry([]byte("cpu:99%;load:1.2")); err != nil {
		t.Fatalf("SendTelemetry failed: %v", err)
	}

	select {
	case msg := <-receivedUDP:
		if msg != "cpu:99%;load:1.2" {
			t.Fatalf("expected telemetry payload, got %s", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for telemetry")
	}
}

func TestClientMTLS(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "forest.wal")
	sstDir := filepath.Join(tempDir, "sst")

	db, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer db.Close()

	srvTLS, cliTLS, err := network.GenerateTestMTLSCredentials()
	if err != nil {
		t.Fatalf("failed to generate TLS creds: %v", err)
	}

	tcpAddr := "127.0.0.1:9202"
	server := network.NewServer(
		tcpAddr,
		db,
		network.WithTLS(srvTLS),
	)

	go func() {
		_ = server.Start()
	}()
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	cli, err := client.New(
		tcpAddr,
		client.WithTLS(cliTLS),
		client.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to connect via mTLS: %v", err)
	}
	defer cli.Close()

	// Verify operation over mTLS
	if err := cli.Put([]byte("secure-key"), []byte("secure-value")); err != nil {
		t.Fatalf("secure Put failed: %v", err)
	}

	val, found, err := cli.Get([]byte("secure-key"))
	if err != nil || !found || string(val) != "secure-value" {
		t.Fatalf("secure Get failed: found=%v, val=%s, err=%v", found, val, err)
	}
}
