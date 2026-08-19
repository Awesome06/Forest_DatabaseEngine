package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// TestWAL_AppendAndRecover verifies that standard Put operations survive a crash
// by persisting to the WAL and correctly reconstructing the MemTable.
func TestWAL_AppendAndRecover(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	key1, val1 := []byte("key1"), []byte("val1")
	key2, val2 := []byte("key2"), []byte("val2")

	_ = wal.Append(network.OpPut, key1, val1)
	_ = wal.Append(network.OpPut, key2, val2)

	// Wait for background syncer to fsync (mechanical sympathy test)
	time.Sleep(50 * time.Millisecond)

	err = wal.Close()
	if err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	mt := NewMemTable()
	err = Recover(walPath, mt)
	if err != nil {
		t.Fatalf("failed to recover WAL: %v", err)
	}

	// Verify MemTable lock-free reconstruction
	gotVal1, ok := mt.Get(key1)
	if !ok || !bytes.Equal(gotVal1, val1) {
		t.Errorf("expected val1, got %s", string(gotVal1))
	}
	gotVal2, ok := mt.Get(key2)
	if !ok || !bytes.Equal(gotVal2, val2) {
		t.Errorf("expected val2, got %s", string(gotVal2))
	}
}

// TestWAL_CorruptionRecovery proves that torn writes (simulated by byte flipping)
// are safely detected via CRC checksums, halting recovery cleanly without panicking.
func TestWAL_CorruptionRecovery(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "corrupt.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	key, val := []byte("corrupt_key"), []byte("corrupt_val")
	_ = wal.Append(network.OpPut, key, val)
	
	time.Sleep(50 * time.Millisecond)
	_ = wal.Close()

	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}
	
	// Flip the last byte to invalidate the CRC checksum of the final record
	data[len(data)-1] ^= 0xFF
	
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("failed to write corrupted WAL: %v", err)
	}

	mt := NewMemTable()
	// Recovery should succeed but silently stop before the corrupted record 
	// (as it is treated as a torn write / EOF).
	if err := Recover(walPath, mt); err != nil {
		t.Fatalf("recovery returned an error instead of stopping at EOF/Torn Write: %v", err)
	}

	// The record should NOT be in the MemTable
	if _, ok := mt.Get(key); ok {
		t.Errorf("record should not have been recovered due to CRC mismatch")
	}
}
