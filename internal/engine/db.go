package engine

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// DB coordinates the active MemTable, WAL, and background SSTable flushes.
type DB struct {
	activeMT *MemTable
	wal      *WAL
	mu       sync.RWMutex
	flushQ   chan *MemTable
	wg       sync.WaitGroup
	stopCh   chan struct{}
	
	flushThreshold int64
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
		flushQ:         make(chan *MemTable, 10),
		stopCh:         make(chan struct{}),
		flushThreshold: 4 * 1024 * 1024, // 4MB
	}

	db.wg.Add(1)
	go db.flushWorker()

	return db, nil
}

// Put writes the key-value pair to the WAL and active MemTable.
func (db *DB) Put(key, val []byte) error {
	// Write to WAL first
	if err := db.wal.Append(network.OpEcho, key, val); err != nil {
		return err
	}

	db.mu.RLock()
	mt := db.activeMT
	db.mu.RUnlock()

	// Insert into active MemTable lock-free relative to readers
	mt.Put(key, val)

	// Check if we need to flush
	if mt.Size() >= db.flushThreshold {
		db.CheckFlush()
	}
	return nil
}

// CheckFlush safely swaps the active MemTable if it exceeds the threshold.
func (db *DB) CheckFlush() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Double check inside the lock
	if db.activeMT.Size() < db.flushThreshold {
		return
	}

	// Freeze current MemTable
	frozen := db.activeMT
	
	// Atomically swap in a new MemTable
	db.activeMT = NewMemTable()
	
	// Send frozen MemTable to background flusher
	db.flushQ <- frozen
}

// flushWorker runs in the background and writes frozen MemTables to SSTable files.
func (db *DB) flushWorker() {
	defer db.wg.Done()
	flushCount := time.Now().UnixNano() // Unique file identifier

	for {
		select {
		case mt := <-db.flushQ:
			filepath := fmt.Sprintf("data-%d.sst", flushCount)
			log.Printf("Flushing MemTable to %s...", filepath)
			if err := FlushMemTable(mt, filepath); err != nil {
				log.Printf("FATAL: failed to flush SSTable: %v", err)
			}
			flushCount++
		case <-db.stopCh:
			// Drain remaining flush queue on shutdown
			close(db.flushQ)
			for mt := range db.flushQ {
				filepath := fmt.Sprintf("data-%d.sst", flushCount)
				FlushMemTable(mt, filepath)
				flushCount++
			}
			return
		}
	}
}

// Close cleanly shuts down the WAL and flushes remaining MemTables.
func (db *DB) Close() error {
	close(db.stopCh)
	db.wg.Wait()
	return db.wal.Close()
}
