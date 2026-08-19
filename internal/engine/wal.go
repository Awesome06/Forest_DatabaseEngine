package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

const WALHeaderSize = 12 // Magic(1) + CRC32(4) + OpCode(1) + KeyLen(2) + ValLen(4)

// WAL represents an append-only Write-Ahead Log.
type WAL struct {
	file   *os.File
	bw     *bufio.Writer
	mu     sync.Mutex
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewWAL creates or opens a WAL file and starts the background syncer.
func NewWAL(filepath string) (*WAL, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:   f,
		bw:     bufio.NewWriterSize(f, 4*1024*1024), // 4MB buffer
		stopCh: make(chan struct{}),
	}

	wal.wg.Add(1)
	go wal.syncLoop()

	return wal, nil
}

// Append writes a new entry to the WAL buffer.
func (w *WAL) Append(op network.OpCode, key, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var header [WALHeaderSize]byte
	header[0] = network.MagicByte

	// OpCode, KeyLen, ValLen
	header[5] = byte(op)
	binary.BigEndian.PutUint16(header[6:8], uint16(len(key)))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(val)))

	// Compute CRC32 of OpCode + KeyLen + ValLen + Key + Val
	h := crc32.NewIEEE()
	h.Write(header[5:12])
	h.Write(key)
	h.Write(val)
	crc := h.Sum32()
	binary.BigEndian.PutUint32(header[1:5], crc)

	// Write header and payload
	if _, err := w.bw.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(key); err != nil {
		return err
	}
	if _, err := w.bw.Write(val); err != nil {
		return err
	}

	return nil
}

// syncLoop flushes the buffer to disk every 10ms.
func (w *WAL) syncLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flush()
		case <-w.stopCh:
			// Final flush on shutdown
			w.flush()
			return
		}
	}
}

func (w *WAL) flush() {
	w.mu.Lock()
	err := w.bw.Flush()
	w.mu.Unlock()

	if err == nil {
		// Sync to disk outside the lock to avoid blocking incoming appends.
		_ = w.file.Sync()
	}
}

// Close gracefully stops the background syncer and closes the file.
func (w *WAL) Close() error {
	close(w.stopCh)
	w.wg.Wait()
	return w.file.Close()
}

// Recover reads a WAL file and repopulates a MemTable.
func Recover(filepath string, mt *MemTable) error {
	f, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Nothing to recover
		}
		return err
	}
	defer f.Close()

	var header [WALHeaderSize]byte
	for {
		_, err := io.ReadFull(f, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // End of file or torn write at header
			}
			return err
		}

		if header[0] != network.MagicByte {
			return errors.New("corrupted WAL: invalid magic byte")
		}

		expectedCRC := binary.BigEndian.Uint32(header[1:5])
		_ = network.OpCode(header[5])
		keyLen := binary.BigEndian.Uint16(header[6:8])
		valLen := binary.BigEndian.Uint32(header[8:12])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil // Torn write at key
		}

		val := make([]byte, valLen)
		if _, err := io.ReadFull(f, val); err != nil {
			return nil // Torn write at val
		}

		h := crc32.NewIEEE()
		h.Write(header[5:12])
		h.Write(key)
		h.Write(val)
		if h.Sum32() != expectedCRC {
			return nil // Torn write / corruption detected, stop reading
		}

		// Apply to MemTable
		// For a real DB, we'd handle deletes as well. 
		// Assuming OpEcho or OpPut acts as a Put for this phase.
		mt.Put(key, val)
	}
}
