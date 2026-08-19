// Package engine contains the core data structures and logic for the LSM-Tree database.
// It includes the MemTable, Write-Ahead Log (WAL), SSTables, and background compaction.
package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// WALHeaderSize defines the fixed byte length of a WAL entry header.
// Layout: Magic(1) + CRC32(4) + OpCode(1) + KeyLen(2) + ValLen(4) = 12 bytes.
const WALHeaderSize = 12

// WAL represents a crash-safe, append-only Write-Ahead Log.
// It utilizes double-buffering (via bufio) and asynchronous fsyncs
// to prevent disk I/O from blocking the fast network parser.
type WAL struct {
	file      *os.File
	bufWriter *bufio.Writer
	mu        sync.Mutex
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewWAL creates or opens a WAL file and starts the background fsync loop.
func NewWAL(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %s: %w", filepath, err)
	}

	wal := &WAL{
		file:      file,
		// 4MB buffer prevents frequent small disk writes, relying on the background 
		// syncer to persist chunks asynchronously.
		bufWriter: bufio.NewWriterSize(file, 4*1024*1024), 
		stopCh:    make(chan struct{}),
	}

	wal.wg.Add(1)
	go wal.syncLoop()

	return wal, nil
}

// Append serializes a new operation into the WAL buffer.
// The lock ensures thread-safe sequential appending across concurrent client requests.
func (w *WAL) Append(op network.OpCode, key, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var header [WALHeaderSize]byte
	header[0] = network.MagicByte
	header[5] = byte(op)
	binary.BigEndian.PutUint16(header[6:8], uint16(len(key)))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(val)))

	// Compute CRC32 over the mutable fields and payload to detect disk corruption or torn writes.
	h := crc32.NewIEEE()
	h.Write(header[5:12])
	h.Write(key)
	h.Write(val)
	binary.BigEndian.PutUint32(header[1:5], h.Sum32())

	// Write header and payload sequentially to the memory buffer.
	if _, err := w.bufWriter.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}
	if _, err := w.bufWriter.Write(key); err != nil {
		return fmt.Errorf("failed to write WAL key: %w", err)
	}
	if _, err := w.bufWriter.Write(val); err != nil {
		return fmt.Errorf("failed to write WAL value: %w", err)
	}

	return nil
}

// syncLoop is a dedicated goroutine that flushes the memory buffer to the OS,
// and forces an fsync to persistent storage every 10ms.
func (w *WAL) syncLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flush()
		case <-w.stopCh:
			// Ensure final writes are persisted during graceful shutdown.
			w.flush()
			return
		}
	}
}

// flush safely pushes buffered data to the underlying file and triggers an fsync.
func (w *WAL) flush() {
	w.mu.Lock()
	err := w.bufWriter.Flush()
	w.mu.Unlock()

	if err == nil {
		// Sync is called outside the mutex so disk I/O latency does not block active Appends.
		_ = w.file.Sync()
	}
}

// Close gracefully stops the background syncer, forcing a final flush, and closes the file descriptor.
func (w *WAL) Close() error {
	close(w.stopCh)
	w.wg.Wait()
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}
	return nil
}

// Recover replays a WAL file sequentially to repopulate the MemTable upon startup.
// It gracefully halts on torn writes (EOF or CRC mismatch) indicating the end of safe data.
func Recover(filepath string, mt *MemTable) error {
	file, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // A new database instance, nothing to recover.
		}
		return fmt.Errorf("failed to open WAL for recovery: %w", err)
	}
	defer file.Close()

	var header [WALHeaderSize]byte
	for {
		if _, err := io.ReadFull(file, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // Reached the end of the fully synced data.
			}
			return fmt.Errorf("failed reading WAL header: %w", err)
		}

		if header[0] != network.MagicByte {
			return fmt.Errorf("corrupted WAL: expected magic byte 0x%X, got 0x%X", network.MagicByte, header[0])
		}

		expectedCRC := binary.BigEndian.Uint32(header[1:5])
		op := network.OpCode(header[5])
		keyLen := binary.BigEndian.Uint16(header[6:8])
		valLen := binary.BigEndian.Uint32(header[8:12])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(file, key); err != nil {
			return nil // Torn write at key payload, stop recovery.
		}

		val := make([]byte, valLen)
		if _, err := io.ReadFull(file, val); err != nil {
			return nil // Torn write at value payload, stop recovery.
		}

		// Verify data integrity before applying state.
		h := crc32.NewIEEE()
		h.Write(header[5:12])
		h.Write(key)
		h.Write(val)
		if h.Sum32() != expectedCRC {
			return nil // CRC mismatch implies corrupted tail data, stop recovery cleanly.
		}

		// Apply the recovered state to the MemTable.
		if op == network.OpDelete {
			mt.Put(key, nil) // Tombstone for delete
		} else {
			mt.Put(key, val) // Echo or Put
		}
	}
}
