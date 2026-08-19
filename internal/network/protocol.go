package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MagicByte is used to validate our protocol packets, immediately rejecting malformed data.
// It is set to 0xA1 as per the binary protocol specification.
const MagicByte byte = 0xA1

// HeaderSize defines the fixed size of the protocol header in bytes.
const HeaderSize = 8

// OpCode represents the type of operation in the binary protocol.
type OpCode byte

const (
	OpEcho   OpCode = 0x01
	OpPut    OpCode = 0x02
	OpGet    OpCode = 0x03
	OpDelete OpCode = 0x04
)

// RequestHeader represents the parsed 8-byte TCP packet header.
type RequestHeader struct {
	Op       OpCode
	KeyLen   uint16
	ValueLen uint32
}

var (
	// ErrInvalidMagic is returned when a packet does not start with the expected MagicByte.
	ErrInvalidMagic = errors.New("invalid magic byte")
	// ErrShortBuffer is returned if the provided parsing buffer is smaller than HeaderSize.
	ErrShortBuffer  = errors.New("provided buffer is too short for header")
)

// Storage defines the interface for the database engine to prevent cyclic dependencies.
// It mandates the core LSM-Tree operations required by the network handlers.
type Storage interface {
	Put(key, val []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
}

// ParseHeader reads exactly HeaderSize bytes from the io.Reader into the provided buffer
// and decodes them into a RequestHeader. The buffer must be pre-allocated to avoid heap allocations.
func ParseHeader(r io.Reader, buf []byte) (RequestHeader, error) {
	if len(buf) < HeaderSize {
		return RequestHeader{}, ErrShortBuffer
	}

	headerBuf := buf[:HeaderSize]
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		if errors.Is(err, io.EOF) {
			return RequestHeader{}, err // Do not wrap EOF so the caller can handle client disconnects
		}
		return RequestHeader{}, fmt.Errorf("failed to read protocol header: %w", err)
	}

	if headerBuf[0] != MagicByte {
		return RequestHeader{}, fmt.Errorf("%w: expected 0x%X, got 0x%X", ErrInvalidMagic, MagicByte, headerBuf[0])
	}

	return RequestHeader{
		Op:       OpCode(headerBuf[1]),
		KeyLen:   binary.BigEndian.Uint16(headerBuf[2:4]),
		ValueLen: binary.BigEndian.Uint32(headerBuf[4:8]),
	}, nil
}
