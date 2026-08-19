// Package engine contains the core data structures and logic for the LSM-Tree database.
package engine

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// DB coordinates the active MemTable, WAL, and background SSTable flushes.
// It uses atomic pointers and RCU (Read-Copy-Update) semantics to ensure
// that network readers are never blocked by background compactions or flushes.
type DB struct {
	// Active and immutable MemTables are stored as atomic pointers.
	// This eliminates the need for reader mutexes during the hot Get() path.
	activeMT    atomic.Pointer[MemTable]
	immutableMT atomic.Pointer[MemTable] 
	
	wal *WAL
	
	// mu is strictly used to serialize flush requests and background coordination.
	// It is purposefully bypassed by the hot read/write paths.
	mu sync.Mutex 
	
	flushQ         chan *MemTable
	wg             sync.WaitGroup
	stopCh         chan struct{}
	flushThreshold int64
	
	// currentVersion uses RCU to swap the active SSTable manifest atomically.
	currentVersion atomic.Pointer[Version]
}

// NewDB initializes the engine state, recovers from the WAL, and starts background workers.
func NewDB(walPath string) (*DB, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("engine failed to initialize WAL: %w", err)
	}

	mt := NewMemTable()
	if err := Recover(walPath, mt); err != nil {
		return nil, fmt.Errorf("engine failed to recover from WAL: %w", err)
	}

	db := &DB{
		wal:            wal,
		flushQ:         make(chan *MemTable, 1),
		stopCh:         make(chan struct{}),
		flushThreshold: 4 * 1024 * 1024, // 4MB
	}
	db.activeMT.Store(mt)

	initialVersion := &Version{
		Manifest: Manifest{
			L0Files: []string{},
			L1Files: []string{},
		},
	}
	db.currentVersion.Store(initialVersion)

	db.wg.Add(1)
	go db.flushWorker()

	return db, nil
}

// Get executes the complete LSM read path (MemTable -> Immutable -> SSTables).
// It is strictly lock-free and utilizes atomic pointers and RCU to prevent read stalls.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	// 1. Search Active MemTable (Lock-Free)
	if active := db.activeMT.Load(); active != nil {
		if val, ok := active.Get(key); ok {
			if len(val) == 0 { // Tombstone check
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// 2. Search Immutable MemTable (Lock-Free)
	if immutable := db.immutableMT.Load(); immutable != nil {
		if val, ok := immutable.Get(key); ok {
			if len(val) == 0 {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// 3. Search RCU Version SSTables
	// Acquire a read reference to prevent files from being deleted during read.
	version := db.currentVersion.Load()
	version.Acquire()
	defer version.Release()

	// Search L0 files (newer files are appended to the end, so we search backwards)
	for i := len(version.Manifest.L0Files) - 1; i >= 0; i-- {
		val, found, err := ReadSSTable(version.Manifest.L0Files[i], key)
		if err != nil {
			return nil, false, fmt.Errorf("failed to read L0 SSTable: %w", err)
		}
		if found {
			if len(val) == 0 {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// Search L1 files
	for i := len(version.Manifest.L1Files) - 1; i >= 0; i-- {
		val, found, err := ReadSSTable(version.Manifest.L1Files[i], key)
		if err != nil {
			return nil, false, fmt.Errorf("failed to read L1 SSTable: %w", err)
		}
		if found {
			if len(val) == 0 {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	return nil, false, nil
}

// Put appends the mutation to the WAL and inserts it into the active MemTable lock-free.
func (db *DB) Put(key, val []byte) error {
	if err := db.wal.Append(network.OpPut, key, val); err != nil {
		return fmt.Errorf("engine failed to append Put to WAL: %w", err)
	}

	// Lock-free retry loop
	for {
		active := db.activeMT.Load()
		err := active.Put(key, val)
		if err != nil {
			// Table was frozen by a concurrent CheckFlush, retry to get the new table
			continue
		}

		if active.Size() >= db.flushThreshold {
			db.CheckFlush()
		}
		return nil
	}
}

// Delete appends a tombstone to the WAL and inserts it into the active MemTable lock-free.
func (db *DB) Delete(key []byte) error {
	if err := db.wal.Append(network.OpDelete, key, nil); err != nil {
		return fmt.Errorf("engine failed to append Delete to WAL: %w", err)
	}

	// Lock-free retry loop
	for {
		active := db.activeMT.Load()
		err := active.Put(key, nil)
		if err != nil {
			// Table was frozen by a concurrent CheckFlush, retry
			continue
		}

		if active.Size() >= db.flushThreshold {
			db.CheckFlush()
		}
		return nil
	}
}

// CheckFlush safely rotates the active MemTable into the flush queue if it exceeds the capacity threshold.
func (db *DB) CheckFlush() {
	db.mu.Lock()
	defer db.mu.Unlock()

	active := db.activeMT.Load()
	if active.Size() < db.flushThreshold {
		return
	}

	if db.immutableMT.Load() != nil {
		// This acts as a backpressure signal. If disk I/O cannot keep up with network inserts,
		// the active table will temporarily exceed 4MB until the current flush completes.
		log.Println("warn: MemTable flush triggered while background flush is still active. Write delayed.")
		return
	}

	// Drain in-flight writers and mark the active table as frozen.
	active.Freeze()

	// Atomically rotate the MemTables.
	db.immutableMT.Store(active)
	db.activeMT.Store(NewMemTable())
	
	// Queue the frozen table for disk persistence.
	db.flushQ <- active
}

// flushWorker acts as a background sink, persisting frozen MemTables to L0 SSTables.
func (db *DB) flushWorker() {
	defer db.wg.Done()
	flushCount := time.Now().UnixNano()

	for {
		select {
		case mt := <-db.flushQ:
			filepath := fmt.Sprintf("data-L0-%d.sst", flushCount)
			if err := FlushMemTable(mt, filepath); err != nil {
				log.Printf("critical: failed to flush MemTable to %s: %v", filepath, err)
			}
			flushCount++

			// Perform an RCU (Read-Copy-Update) replacement of the manifest.
			// This allows lock-free readers to safely continue using the old manifest
			// while we introduce the newly created SSTable.
			oldVersion := db.currentVersion.Load()
			newManifest := Manifest{
				L0Files: append([]string(nil), oldVersion.Manifest.L0Files...),
				L1Files: append([]string(nil), oldVersion.Manifest.L1Files...),
			}
			newManifest.L0Files = append(newManifest.L0Files, filepath)

			newVersion := &Version{
				Manifest: newManifest,
			}
			db.currentVersion.Store(newVersion)

			// Cleanly remove the immutable table so the next flush can trigger.
			db.mu.Lock()
			db.immutableMT.Store(nil)
			db.mu.Unlock()

		case <-db.stopCh:
			// Drain remaining flush queue on shutdown to guarantee crash durability.
			close(db.flushQ)
			for mt := range db.flushQ {
				filepath := fmt.Sprintf("data-L0-%d.sst", flushCount)
				FlushMemTable(mt, filepath)
				flushCount++
			}
			return
		}
	}
}

// Close gracefully stops the database. It halts the WAL and forces remaining MemTables to disk.
func (db *DB) Close() error {
	// First close WAL so no more mutations can be received.
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to cleanly close WAL: %w", err)
	}
	
	// Force a final flush of whatever remains in the active MemTable.
	db.mu.Lock()
	active := db.activeMT.Load()
	if active.Size() > 0 {
		// Stably wait if the background worker is currently busy flushing.
		for db.immutableMT.Load() != nil {
			db.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			db.mu.Lock()
		}
		db.immutableMT.Store(active)
		db.flushQ <- active
	}
	db.mu.Unlock()

	// Signal the worker to stop and wait for it to drain the queue.
	close(db.stopCh)
	db.wg.Wait()
	return nil
}
