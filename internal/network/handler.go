package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/Forest_DatabaseEngine/internal/metrics"
)

// HandleRequest processes the parsed TCP packet header and payload using zero-allocation strategies.
// payloadBuf is pre-allocated by the caller to avoid heap allocations on the hot path.
// The optional m parameter allows injecting a custom EngineMetrics instance, defaulting to DefaultEngineMetrics.
func HandleRequest(conn net.Conn, header RequestHeader, payloadBuf []byte, db Storage, m ...*metrics.EngineMetrics) error {
	engMetrics := metrics.DefaultEngineMetrics
	if len(m) > 0 && m[0] != nil {
		engMetrics = m[0]
	}

	totalLen := int(header.KeyLen) + int(header.ValueLen)
	
	// Explicit bounds check to prevent buffer overflow attacks
	if totalLen > len(payloadBuf) {
		engMetrics.TCPRequestsErrors.Inc()
		return fmt.Errorf("protocol error: payload length %d exceeds buffer capacity %d", totalLen, len(payloadBuf))
	}
	
	// Read the variable length payload directly into the pre-allocated buffer
	if totalLen > 0 {
		if _, err := io.ReadFull(conn, payloadBuf[:totalLen]); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("failed to read payload from connection: %w", err)
		}
	}

	key := payloadBuf[:header.KeyLen]
	val := payloadBuf[header.KeyLen:totalLen]

	switch header.Op {
	case OpEcho:
		// Echo response: [MagicByte(1B)] [Op(1B)] [KeyLen(2B)] [ValLen(4B)] [Key] [Val]
		// We construct the response header directly on the stack to prevent allocations.
		var headerBuf [8]byte
		headerBuf[0] = MagicByte
		headerBuf[1] = byte(OpEcho)
		binary.BigEndian.PutUint16(headerBuf[2:4], header.KeyLen)
		binary.BigEndian.PutUint32(headerBuf[4:8], header.ValueLen)
		
		if _, err := conn.Write(headerBuf[:]); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("failed to write echo header: %w", err)
		}
		if totalLen > 0 {
			if _, err := conn.Write(payloadBuf[:totalLen]); err != nil {
				engMetrics.TCPRequestsErrors.Inc()
				return fmt.Errorf("failed to write echo payload: %w", err)
			}
		}
		engMetrics.TCPRequestsEchoOk.Inc()
		return nil

	case OpPut:
		if err := db.Put(key, val); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("storage engine failed to process Put: %w", err)
		}
		engMetrics.TCPRequestsPutOk.Inc()
		return sendAck(conn)

	case OpGet:
		foundVal, found, err := db.Get(key)
		if err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("storage engine failed to process Get: %w", err)
		}
		if !found {
			engMetrics.TCPRequestsGetOk.Inc()
			return sendNotFound(conn)
		}
		
		// Construct the response header with the actual value length retrieved from storage
		var headerBuf [8]byte
		headerBuf[0] = MagicByte
		headerBuf[1] = byte(OpGet)
		binary.BigEndian.PutUint16(headerBuf[2:4], header.KeyLen)
		binary.BigEndian.PutUint32(headerBuf[4:8], uint32(len(foundVal)))
		
		if _, err := conn.Write(headerBuf[:]); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("failed to write get header: %w", err)
		}
		if _, err := conn.Write(key); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("failed to write get key: %w", err)
		}
		if _, err := conn.Write(foundVal); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("failed to write get value: %w", err)
		}
		engMetrics.TCPRequestsGetOk.Inc()
		return nil

	case OpDelete:
		if err := db.Delete(key); err != nil {
			engMetrics.TCPRequestsErrors.Inc()
			return fmt.Errorf("storage engine failed to process Delete: %w", err)
		}
		engMetrics.TCPRequestsDelOk.Inc()
		return sendAck(conn)

	default:
		engMetrics.TCPRequestsErrors.Inc()
		return sendNotFound(conn)
	}
}

// sendAck sends a lightweight acknowledgment packet back to the client.
// It is used for successful Put and Delete operations.
func sendAck(conn net.Conn) error {
	var resp [8]byte
	resp[0] = MagicByte
	// The rest of the header is 0x00, indicating an empty payload response
	if _, err := conn.Write(resp[:]); err != nil {
		return fmt.Errorf("failed to send ACK: %w", err)
	}
	return nil
}

// sendNotFound sends a standard "key not found" response.
// The KeyLen field is set to 0xFFFF as an indicator to the client.
func sendNotFound(conn net.Conn) error {
	var resp [8]byte
	resp[0] = MagicByte
	binary.BigEndian.PutUint16(resp[2:4], 0xFFFF)
	
	if _, err := conn.Write(resp[:]); err != nil {
		return fmt.Errorf("failed to send NotFound response: %w", err)
	}
	return nil
}
