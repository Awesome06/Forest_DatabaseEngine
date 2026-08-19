package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// SSTIterator streams key-value pairs sequentially from the data block phase of a single SSTable.
// It is heavily used during compaction (K-Way merge) to read large contiguous chunks of data without loading the entire file into memory.
type SSTIterator struct {
	file    *os.File
	dataEnd int64
	current int64
	key     []byte
	val     []byte
	op      network.OpCode
	fileSeq uint64
	err     error
}

// NewSSTIterator opens an SSTable and parses the footer to determine the exact byte boundary 
// where the Data Blocks end and the Sparse Index begins.
func NewSSTIterator(filepath string, fileSeq uint64) (*SSTIterator, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable iterator for %s: %w", filepath, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat SSTable for iterator: %w", err)
	}

	if stat.Size() < SSTableFooterSize {
		file.Close()
		return nil, fmt.Errorf("SSTable %s is too small (corrupt)", filepath)
	}

	// 1. Read Footer to find IndexBlockOffset (which marks the exact end of Data Blocks)
	var footer [SSTableFooterSize]byte
	if _, err := file.ReadAt(footer[:], stat.Size()-SSTableFooterSize); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read SSTable footer in iterator: %w", err)
	}

	magic := binary.BigEndian.Uint32(footer[16:20])
	if magic != SSTableMagic {
		file.Close()
		return nil, fmt.Errorf("invalid SSTable magic number in iterator: expected %X, got %X", SSTableMagic, magic)
	}

	dataEnd := int64(binary.BigEndian.Uint64(footer[0:8]))

	return &SSTIterator{
		file:    file,
		dataEnd: dataEnd,
		current: 0,
		fileSeq: fileSeq,
	}, nil
}

// Next reads the next key-value pair from the data blocks. Returns true if successful.
// It strictly halts when the read head reaches the Sparse Index phase (it.dataEnd).
func (it *SSTIterator) Next() bool {
	if it.current >= it.dataEnd {
		return false
	}

	var header [7]byte
	n, err := it.file.ReadAt(header[:], it.current)
	if err != nil {
		it.err = fmt.Errorf("iterator failed to read block header at offset %d: %w", it.current, err)
		return false
	}
	if n < 7 {
		it.err = io.ErrUnexpectedEOF
		return false
	}

	it.op = network.OpCode(header[0])
	keyLen := int(binary.BigEndian.Uint16(header[1:3]))
	valLen := int(binary.BigEndian.Uint32(header[3:7]))

	payload := make([]byte, keyLen+valLen)
	n, err = it.file.ReadAt(payload, it.current+7)
	if err != nil {
		it.err = fmt.Errorf("iterator failed to read block payload at offset %d: %w", it.current+7, err)
		return false
	}
	if n < keyLen+valLen {
		it.err = io.ErrUnexpectedEOF
		return false
	}

	it.key = payload[:keyLen]
	it.val = payload[keyLen:]
	it.current += int64(7 + keyLen + valLen)
	return true
}

// Key returns the current key.
func (it *SSTIterator) Key() []byte { return it.key }

// Value returns the current value.
func (it *SSTIterator) Value() []byte { return it.val }

// OpCode returns the current operation code (e.g., OpPut or OpDelete).
func (it *SSTIterator) OpCode() network.OpCode { return it.op }

// FileSeq returns the sequence identifier for tie-breaking during compaction.
func (it *SSTIterator) FileSeq() uint64 { return it.fileSeq }

// Error returns the last encountered read error, if any.
func (it *SSTIterator) Error() error { return it.err }

// Close cleanly releases the underlying file handle.
func (it *SSTIterator) Close() error { return it.file.Close() }
