package network

import (
	"encoding/binary"
	"errors"
	"io"
)

// MagicByte is used to identify our protocol packets.
const MagicByte byte = 0xFE
const HeaderSize = 8

type OpCode byte

const (
	OpEcho   OpCode = 0x01
	OpPut    OpCode = 0x02
	OpGet    OpCode = 0x03
	OpDelete OpCode = 0x04
)

type RequestHeader struct {
	Op       OpCode
	KeyLen   uint16
	ValueLen uint32
}

var (
	ErrInvalidMagic = errors.New("invalid magic byte")
	ErrShortBuffer  = errors.New("provided buffer is too short for header")
)

// Storage defines the interface for the database engine to prevent cyclic dependencies.
type Storage interface {
	Put(key, val []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
}

func ParseHeader(r io.Reader, buf []byte) (RequestHeader, error) {
	if len(buf) < HeaderSize {
		return RequestHeader{}, ErrShortBuffer
	}

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
