package network

import (
	"encoding/binary"
	"io"
	"net"
)

// HandleEcho reads the payload specified by the header from the connection,
// and immediately writes the exact same packet (header + payload) back.
// To enforce zero-allocation on the hot path, it expects a pre-allocated payloadBuf
// that is large enough to hold the payload.
func HandleEcho(conn net.Conn, header RequestHeader, payloadBuf []byte) error {
	totalLen := uint32(header.KeyLen) + header.ValueLen

	if uint32(len(payloadBuf)) < totalLen {
		return ErrShortBuffer
	}

	// Read payload directly into the provided buffer
	payload := payloadBuf[:totalLen]
	if totalLen > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}
	}

	// Prepare response header
	var respHeader [HeaderSize]byte
	respHeader[0] = MagicByte
	respHeader[1] = byte(OpEcho)
	binary.BigEndian.PutUint16(respHeader[2:4], header.KeyLen)
	binary.BigEndian.PutUint32(respHeader[4:8], header.ValueLen)

	// Write header back
	if _, err := conn.Write(respHeader[:]); err != nil {
		return err
	}

	// Write payload back
	if totalLen > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}

	return nil
}
