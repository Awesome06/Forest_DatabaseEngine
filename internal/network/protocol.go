package network

import (
	"encoding/binary"
	"errors"
	"io"
)

// MagicByte is used to identify our protocol packets.
const MagicByte byte = 0xFE

// HeaderSize is the fixed size of our protocol header:
// Magic (1) + OpCode (1) + KeyLen (2) + ValueLen (4) = 8 bytes.
const HeaderSize = 8

// OpCode defines the operation type.
type OpCode byte

const (
	OpEcho OpCode = 0x01
	// OpPut    OpCode = 0x02
	// OpGet    OpCode = 0x03
	// OpDelete OpCode = 0x04
)

// RequestHeader is the strictly parsed 8-byte header.
type RequestHeader struct {
	Op       OpCode
	KeyLen   uint16
	ValueLen uint32
}

var (
	ErrInvalidMagic = errors.New("invalid magic byte")
	ErrShortBuffer  = errors.New("provided buffer is too short for header")
)

// ParseHeader reads directly from an io.Reader into a pre-allocated buffer
// to avoid heap allocations on the hot path. The provided buf must be at least
// HeaderSize (8 bytes) long.
func ParseHeader(r io.Reader, buf []byte) (RequestHeader, error) {
	if len(buf) < HeaderSize {
		return RequestHeader{}, ErrShortBuffer
	}

	// We only read the exact HeaderSize
	headerBuf := buf[:HeaderSize]
	_, err := io.ReadFull(r, headerBuf)
	if err != nil {
		return RequestHeader{}, err
	}

	if headerBuf[0] != MagicByte {
		return RequestHeader{}, ErrInvalidMagic
	}

	return RequestHeader{
		Op:       OpCode(headerBuf[1]),
		KeyLen:   binary.BigEndian.Uint16(headerBuf[2:4]),
		ValueLen: binary.BigEndian.Uint32(headerBuf[4:8]),
	}, nil
}
