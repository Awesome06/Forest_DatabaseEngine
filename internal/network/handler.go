package network

import (
	"encoding/binary"
	"io"
	"net"
)

// HandleRequest processes the TCP packet using zero-allocation strategies where possible.
// payloadBuf is pre-allocated by the caller to avoid heap allocations on the hot path.
func HandleRequest(conn net.Conn, header RequestHeader, payloadBuf []byte, db Storage) error {
	totalLen := int(header.KeyLen) + int(header.ValueLen)
	if totalLen > 0 {
		_, err := io.ReadFull(conn, payloadBuf[:totalLen])
		if err != nil {
			return err
		}
	}

	key := payloadBuf[:header.KeyLen]
	val := payloadBuf[header.KeyLen:totalLen]

	switch header.Op {
	case OpEcho:
		// Echo response: [KeyLen(2B)] [ValLen(4B)] [Key] [Val]
		// For Echo, we write directly from the payload buffer
		var headerBuf [8]byte
		headerBuf[0] = MagicByte
		headerBuf[1] = byte(OpEcho)
		binary.BigEndian.PutUint16(headerBuf[2:4], header.KeyLen)
		binary.BigEndian.PutUint32(headerBuf[4:8], header.ValueLen)
		
		conn.Write(headerBuf[:])
		if totalLen > 0 {
			conn.Write(payloadBuf[:totalLen])
		}
		return nil

	case OpPut:
		if err := db.Put(key, val); err != nil {
			return err
		}
		return sendAck(conn)

	case OpGet:
		foundVal, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			return sendNotFound(conn)
		}
		
		// Send found value
		var headerBuf [8]byte
		headerBuf[0] = MagicByte
		headerBuf[1] = byte(OpGet)
		binary.BigEndian.PutUint16(headerBuf[2:4], header.KeyLen)
		binary.BigEndian.PutUint32(headerBuf[4:8], uint32(len(foundVal)))
		conn.Write(headerBuf[:])
		conn.Write(key)
		conn.Write(foundVal)
		return nil

	case OpDelete:
		if err := db.Delete(key); err != nil {
			return err
		}
		return sendAck(conn)

	default:
		// Unknown op code
		return sendNotFound(conn)
	}
}

func sendAck(conn net.Conn) error {
	var resp [8]byte
	resp[0] = MagicByte
	_, err := conn.Write(resp[:])
	return err
}

func sendNotFound(conn net.Conn) error {
	var resp [8]byte
	resp[0] = MagicByte
	binary.BigEndian.PutUint16(resp[2:4], 0xFFFF)
	_, err := conn.Write(resp[:])
	return err
}
