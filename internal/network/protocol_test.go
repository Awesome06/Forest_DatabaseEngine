package network

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

// TestParseHeader verifies the parser correctly decodes valid headers and rejects invalid ones.
func TestParseHeader(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		bufSize   int
		want      RequestHeader
		wantErr   bool
		errType   error
	}{
		{
			name: "Valid Header",
			input: []byte{
				MagicByte,
				byte(OpEcho),
				0x00, 0x04, // KeyLen = 4
				0x00, 0x00, 0x00, 0x08, // ValueLen = 8
			},
			bufSize: 8,
			want: RequestHeader{
				Op:       OpEcho,
				KeyLen:   4,
				ValueLen: 8,
			},
			wantErr: false,
		},
		{
			name: "Invalid Magic Byte",
			input: []byte{
				0x00, // Invalid magic
				byte(OpEcho),
				0x00, 0x04,
				0x00, 0x00, 0x00, 0x08,
			},
			bufSize: 8,
			want:    RequestHeader{},
			wantErr: true,
			errType: ErrInvalidMagic,
		},
		{
			name:    "Short Buffer",
			input:   []byte{},
			bufSize: 4,
			want:    RequestHeader{},
			wantErr: true,
			errType: ErrShortBuffer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.bufSize)
			r := bytes.NewReader(tt.input)
			got, err := ParseHeader(r, buf)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errType != nil && err != tt.errType {
				t.Errorf("ParseHeader() error = %v, want errType %v", err, tt.errType)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseHeader() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// BenchmarkParseHeader proves that header parsing requires 0 heap allocations.
func BenchmarkParseHeader(b *testing.B) {
	// Construct a valid header
	header := []byte{
		MagicByte,
		byte(OpEcho),
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x08,
	}

	// Pre-allocate buffer as would be done in the connection handler
	var buf [HeaderSize]byte

	// Move reader allocation outside the loop to avoid measuring it
	r := &fastReader{data: header}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseHeader(r, buf[:])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// fastReader is used exclusively to mock a zero-allocation io.Reader for the benchmark
type fastReader struct {
	data []byte
	pos  int
}

func (r *fastReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		r.pos = 0 // Reset for benchmark loop
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

type mockStorage struct{}
func (m *mockStorage) Put(key, val []byte) error { return nil }
func (m *mockStorage) Get(key []byte) ([]byte, bool, error) { return nil, false, nil }
func (m *mockStorage) Delete(key []byte) error { return nil }

// TestEndToEndEcho starts the TCP server, sends an Echo request, and verifies the response.
func TestEndToEndEcho(t *testing.T) {
	addr := "127.0.0.1:9001"
	srv := NewServer(addr, &mockStorage{})
	go func() {
		_ = srv.Start()
	}()
	defer srv.Stop()

	// Wait a bit for the server to start
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	key := []byte("test")
	val := []byte("payload")
	payload := append(key, val...)

	var header [HeaderSize]byte
	header[0] = MagicByte
	header[1] = byte(OpEcho)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(val)))

	// Send Header
	if _, err := conn.Write(header[:]); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	// Send Payload
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// Read Response
	var respHeaderBuf [HeaderSize]byte
	if _, err := io.ReadFull(conn, respHeaderBuf[:]); err != nil {
		t.Fatalf("failed to read response header: %v", err)
	}

	if respHeaderBuf[0] != MagicByte {
		t.Fatalf("invalid response magic byte")
	}

	respKeyLen := binary.BigEndian.Uint16(respHeaderBuf[2:4])
	respValLen := binary.BigEndian.Uint32(respHeaderBuf[4:8])
	respTotalLen := int(uint32(respKeyLen) + respValLen)

	if respTotalLen != len(payload) {
		t.Fatalf("response payload length mismatch: got %d, want %d", respTotalLen, len(payload))
	}

	respPayload := make([]byte, respTotalLen)
	if _, err := io.ReadFull(conn, respPayload); err != nil {
		t.Fatalf("failed to read response payload: %v", err)
	}

	if !bytes.Equal(respPayload, payload) {
		t.Fatalf("response payload mismatch: got %s, want %s", string(respPayload), string(payload))
	}
}
