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
type DB struct {
	activeMT       *MemTable
	immutableMT    *MemTable // Holds the frozen MemTable during background flush
	wal            *WAL
	mu             sync.RWMutex
	flushQ         chan *MemTable
	wg             sync.WaitGroup
	stopCh         chan struct{}
	flushThreshold int64
	currentVersion atomic.Pointer[Version]
}

// NewDB initializes the engine state and starts background workers.
func NewDB(walPath string) (*DB, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	mt := NewMemTable()
	if err := Recover(walPath, mt); err != nil {
		return nil, err
	}

	db := &DB{
		activeMT:       mt,
		wal:            wal,
		flushQ:         make(chan *MemTable, 1),
		stopCh:         make(chan struct{}),
		flushThreshold: 4 * 1024 * 1024, // 4MB
	}

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

// Get executes the complete LSM read path.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	// 1. Active MemTable (and Immutable MemTable)
	db.mu.RLock()
	active := db.activeMT
	immutable := db.immutableMT
	db.mu.RUnlock()

	if val, ok := active.Get(key); ok {
		if len(val) == 0 { // Tombstone check
			return nil, false, nil
		}
		return val, true, nil
	}

	if immutable != nil {
		if val, ok := immutable.Get(key); ok {
			if len(val) == 0 {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// 2. RCU Version SSTables
	version := db.currentVersion.Load()
	version.Acquire()
	defer version.Release()

	// Search L0 files (newer files are appended to the end, so we search backwards)
	for i := len(version.Manifest.L0Files) - 1; i >= 0; i-- {
		val, found, err := ReadSSTable(version.Manifest.L0Files[i], key)
		if err != nil {
			return nil, false, err
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
			return nil, false, err
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

// Put writes the key-value pair to the WAL and active MemTable.
func (db *DB) Put(key, val []byte) error {
	if err := db.wal.Append(network.OpPut, key, val); err != nil {
		return err
	}

	db.mu.RLock()
	mt := db.activeMT
	db.mu.RUnlock()

	mt.Put(key, val)

	if mt.Size() >= db.flushThreshold {
		db.CheckFlush()
	}
	return nil
}

// Delete appends a tombstone for the key.
func (db *DB) Delete(key []byte) error {
	if err := db.wal.Append(network.OpDelete, key, nil); err != nil {
		return err
	}

	db.mu.RLock()
	mt := db.activeMT
	db.mu.RUnlock()

	mt.Put(key, nil)

	if mt.Size() >= db.flushThreshold {
		db.CheckFlush()
	}
	return nil
}

// CheckFlush safely swaps the active MemTable if it exceeds the threshold.
func (db *DB) CheckFlush() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeMT.Size() < db.flushThreshold {
		return
	}

	if db.immutableMT != nil {
		log.Println("WARNING: Flush triggered while immutableMT is still flushing. Stalling.")
		return
	}

	db.immutableMT = db.activeMT
	db.activeMT = NewMemTable()
	db.flushQ <- db.immutableMT
}

// flushWorker runs in the background and writes frozen MemTables to SSTable files.
func (db *DB) flushWorker() {
	defer db.wg.Done()
	flushCount := time.Now().UnixNano()

	for {
		select {
		case mt := <-db.flushQ:
			filepath := fmt.Sprintf("data-L0-%d.sst", flushCount)
			log.Printf("Flushing MemTable to %s...", filepath)
			if err := FlushMemTable(mt, filepath); err != nil {
				log.Printf("FATAL: failed to flush SSTable: %v", err)
			}
			flushCount++

			// RCU Manifest Update
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

			// Cleanly remove immutableMT so next flush can happen
			db.mu.Lock()
			db.immutableMT = nil
			db.mu.Unlock()

		case <-db.stopCh:
			// Drain remaining flush queue on shutdown
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

// Close cleanly shuts down the WAL and flushes remaining MemTables.
func (db *DB) Close() error {
	// First close WAL so no more Puts can happen
	if err := db.wal.Close(); err != nil {
		return err
	}
	
	// Force flush whatever is in activeMT if it's not empty
	db.mu.Lock()
	if db.activeMT.Size() > 0 {
		// Wait if immutable is currently flushing
		for db.immutableMT != nil {
			db.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			db.mu.Lock()
		}
		db.immutableMT = db.activeMT
		db.flushQ <- db.immutableMT
	}
	db.mu.Unlock()

	close(db.stopCh)
	db.wg.Wait()
	return nil
}
