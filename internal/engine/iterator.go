package engine

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// SSTIterator streams key-value pairs from a single SSTable sequentially.
type SSTIterator struct {
	f       *os.File
	dataEnd int64
	current int64
	key     []byte
	val     []byte
	op      network.OpCode
	fileSeq uint64
	err     error
}

// NewSSTIterator opens an SSTable and parses the footer to find the end of the Data Blocks.
func NewSSTIterator(filepath string, fileSeq uint64) (*SSTIterator, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if stat.Size() < 20 {
		f.Close()
		return nil, io.ErrUnexpectedEOF
	}

	// Read Footer to find IndexBlockOffset (which marks the end of Data Blocks)
	var footer [20]byte
	if _, err := f.ReadAt(footer[:], stat.Size()-20); err != nil {
		f.Close()
		return nil, err
	}
	dataEnd := int64(binary.BigEndian.Uint64(footer[0:8]))

	return &SSTIterator{
		f:       f,
		dataEnd: dataEnd,
		current: 0,
		fileSeq: fileSeq,
	}, nil
}

// Next reads the next key-value pair from the data blocks. Returns true if successful.
func (it *SSTIterator) Next() bool {
	if it.current >= it.dataEnd {
		return false
	}

	var header [7]byte
	n, err := it.f.ReadAt(header[:], it.current)
	if err != nil {
		it.err = err
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
	n, err = it.f.ReadAt(payload, it.current+7)
	if err != nil {
		it.err = err
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

func (it *SSTIterator) Key() []byte { return it.key }
func (it *SSTIterator) Value() []byte { return it.val }
func (it *SSTIterator) OpCode() network.OpCode { return it.op }
func (it *SSTIterator) FileSeq() uint64 { return it.fileSeq }
func (it *SSTIterator) Error() error { return it.err }
func (it *SSTIterator) Close() error { return it.f.Close() }
