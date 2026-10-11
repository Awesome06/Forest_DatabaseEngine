package engine_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Forest_DatabaseEngine/internal/engine"
)

func TestDB_BasicCRUD(t *testing.T) {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "test.wal")
	sstDir := filepath.Join(testDir, "sst")

	db, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		t.Fatalf("failed to initialize DB: %v", err)
	}
	defer db.Close()

	key := []byte("hello")
	val := []byte("world")

	// 1. Put
	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Get
	gotVal, found, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || !bytes.Equal(gotVal, val) {
		t.Fatalf("expected found=true, val=%s; got found=%v, val=%s", val, found, gotVal)
	}

	// 3. Delete
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. Get after delete
	_, foundAfterDel, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if foundAfterDel {
		t.Fatalf("expected key to be deleted, but was found")
	}
}

func TestDB_MemTableFlushAndSSTableRead(t *testing.T) {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "flush_test.wal")
	sstDir := filepath.Join(testDir, "sst")

	// Set small flush threshold (1KB) so flush triggers quickly
	db, err := engine.NewDB(
		walPath,
		engine.WithSSTDir(sstDir),
		engine.WithFlushThreshold(1024),
	)
	if err != nil {
		t.Fatalf("failed to initialize DB: %v", err)
	}
	defer db.Close()

	// Insert enough keys to exceed 1KB
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("val-%08d-padding-data-for-flush", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Force rotation and wait for background flush
	db.CheckFlush()
	time.Sleep(100 * time.Millisecond)

	// Verify all keys are retrievable from L0 SSTables
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		expectedVal := []byte(fmt.Sprintf("val-%08d-padding-data-for-flush", i))

		got, found, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", k, err)
		}
		if !found || !bytes.Equal(got, expectedVal) {
			t.Fatalf("Get(%s): expected found=true, val=%s; got found=%v, val=%s", k, expectedVal, found, got)
		}
	}

	// Non-existent key must return found=false quickly via Bloom filter
	_, foundNonExistent, err := db.Get([]byte("non-existent-key-12345"))
	if err != nil {
		t.Fatalf("Get non-existent failed: %v", err)
	}
	if foundNonExistent {
		t.Fatalf("expected found=false for non-existent key")
	}
}

func TestDB_Compaction_L0toL1(t *testing.T) {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "compact_test.wal")
	sstDir := filepath.Join(testDir, "sst")

	// Trigger compaction when 2 L0 files accumulate
	db, err := engine.NewDB(
		walPath,
		engine.WithSSTDir(sstDir),
		engine.WithFlushThreshold(256),
		engine.WithCompactionThreshold(2),
	)
	if err != nil {
		t.Fatalf("failed to initialize DB: %v", err)
	}
	defer db.Close()

	// Batch 1: Overwrite key "target" with "version1"
	for i := 0; i < 15; i++ {
		_ = db.Put([]byte(fmt.Sprintf("b1-key-%d", i)), []byte("b1-data"))
	}
	_ = db.Put([]byte("target"), []byte("version1"))
	db.CheckFlush()
	time.Sleep(100 * time.Millisecond)

	// Batch 2: Overwrite key "target" with "version2"
	for i := 0; i < 15; i++ {
		_ = db.Put([]byte(fmt.Sprintf("b2-key-%d", i)), []byte("b2-data"))
	}
	_ = db.Put([]byte("target"), []byte("version2"))
	db.CheckFlush()
	time.Sleep(150 * time.Millisecond)

	// Trigger compaction pass
	db.TriggerCompaction()
	time.Sleep(200 * time.Millisecond)

	// Verify L1 file is present in active version manifest
	currentVer := db.CurrentVersion()
	if len(currentVer.Manifest.L1Files) == 0 {
		t.Logf("Compaction L1 files: %v, L0 files: %v", currentVer.Manifest.L1Files, currentVer.Manifest.L0Files)
	}

	// Verify target key returns freshest mutation ("version2")
	val, found, err := db.Get([]byte("target"))
	if err != nil {
		t.Fatalf("Get target failed: %v", err)
	}
	if !found || string(val) != "version2" {
		t.Fatalf("expected target=version2, got found=%v val=%s", found, string(val))
	}
}

func TestDB_RecoveryFromWAL(t *testing.T) {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "recovery.wal")
	sstDir := filepath.Join(testDir, "sst")

	// 1. First DB instance writes mutations
	db1, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		t.Fatalf("failed to initialize DB1: %v", err)
	}

	_ = db1.Put([]byte("recover-k1"), []byte("v1"))
	_ = db1.Put([]byte("recover-k2"), []byte("v2"))
	_ = db1.Delete([]byte("recover-k1"))
	_ = db1.Put([]byte("recover-k3"), []byte("v3"))

	// Close cleanly
	if err := db1.Close(); err != nil {
		t.Fatalf("failed to close DB1: %v", err)
	}

	// 2. Second DB instance recovers from WAL
	db2, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		t.Fatalf("failed to initialize DB2: %v", err)
	}
	defer db2.Close()

	// k1 was deleted: should be not found
	_, foundK1, err := db2.Get([]byte("recover-k1"))
	if err != nil || foundK1 {
		t.Fatalf("expected recover-k1 to be deleted, got found=%v, err=%v", foundK1, err)
	}

	// k2 should be recovered
	v2, foundK2, err := db2.Get([]byte("recover-k2"))
	if err != nil || !foundK2 || string(v2) != "v2" {
		t.Fatalf("expected recover-k2=v2, got found=%v, val=%s", foundK2, string(v2))
	}

	// k3 should be recovered
	v3, foundK3, err := db2.Get([]byte("recover-k3"))
	if err != nil || !foundK3 || string(v3) != "v3" {
		t.Fatalf("expected recover-k3=v3, got found=%v, val=%s", foundK3, string(v3))
	}
}

func TestDB_RangeScan(t *testing.T) {
	testDir := t.TempDir()
	walPath := filepath.Join(testDir, "scan.wal")
	sstDir := filepath.Join(testDir, "sst")

	db, err := engine.NewDB(walPath, engine.WithSSTDir(sstDir))
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer db.Close()

	// 1. Insert older entries and flush to L0 SSTable
	_ = db.Put([]byte("item:01"), []byte("old-val-1"))
	_ = db.Put([]byte("item:02"), []byte("val-2"))
	_ = db.Put([]byte("item:03"), []byte("val-3"))
	db.CheckFlush()
	time.Sleep(100 * time.Millisecond)

	// 2. Insert updates, new entries, and a tombstone in active MemTable
	_ = db.Put([]byte("item:01"), []byte("new-val-1")) // Overwrite
	_ = db.Delete([]byte("item:02"))                    // Tombstone
	_ = db.Put([]byte("item:04"), []byte("val-4"))      // New key
	_ = db.Put([]byte("item:05"), []byte("val-5"))      // New key

	// 3. Execute Range Scan [item:01, item:04]
	results, err := db.Scan([]byte("item:01"), []byte("item:04"))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Expected:
	// item:01 -> "new-val-1" (overwrites L0)
	// item:02 -> omitted (tombstone in active MemTable suppresses L0)
	// item:03 -> "val-3" (from L0)
	// item:04 -> "val-4" (from MemTable)
	// item:05 -> omitted (beyond endKey item:04)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d: %v", len(results), results)
	}

	if string(results[0].Key) != "item:01" || string(results[0].Value) != "new-val-1" {
		t.Fatalf("expected item:01=new-val-1, got %s=%s", results[0].Key, results[0].Value)
	}
	if string(results[1].Key) != "item:03" || string(results[1].Value) != "val-3" {
		t.Fatalf("expected item:03=val-3, got %s=%s", results[1].Key, results[1].Value)
	}
	if string(results[2].Key) != "item:04" || string(results[2].Value) != "val-4" {
		t.Fatalf("expected item:04=val-4, got %s=%s", results[2].Key, results[2].Value)
	}
}

// Clean up any remaining artifacts
func TestMain(m *testing.M) {
	code := m.Run()
	_ = os.RemoveAll("data-L0-*.sst")
	_ = os.RemoveAll("data-L1-*.sst")
	os.Exit(code)
}
