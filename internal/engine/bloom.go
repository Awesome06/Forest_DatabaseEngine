package engine

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a highly optimized bit-array used to probabilistically test whether a key 
// is present in an SSTable. It mathematically guarantees no false negatives, allowing us to 
// bypass expensive disk seeks for keys that definitively do not exist.
type BloomFilter struct {
	bits []uint64 // The underlying bit array packed into 64-bit words for fast bitwise math.
	k    uint8    // The optimal number of hash functions applied per key.
	m    uint64   // The total number of bits in the filter.
}

// NewBloomFilter calculates the optimal bit array size (m) and hash functions (k) 
// for an expected number of elements (n) and a target false positive probability (p).
func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0.0 || p >= 1.0 {
		p = 0.01 // default to 1% false positive rate
	}

	// m = - (n * ln(p)) / (ln(2)^2)
	// We allocate the exact mathematical minimum number of bits required.
	mFloat := math.Ceil((float64(n) * math.Log(p)) / math.Log(1.0/math.Pow(2.0, math.Log(2.0))))
	m := uint64(mFloat)
	if m == 0 {
		m = 64
	}

	// k = (m / n) * ln(2)
	// Balances the bit array saturation so we don't overwrite too many bits per insert.
	kFloat := math.Round((float64(m) / float64(n)) * math.Log(2.0))
	k := uint8(kFloat)
	if k == 0 {
		k = 1
	}

	// Pack bits into 64-bit words. The +63 ensures we round up to the nearest word boundary.
	numUint64s := (m + 63) / 64
	return &BloomFilter{
		bits: make([]uint64, numUint64s),
		k:    k,
		m:    m,
	}
}

// hash generates two distinct 32-bit hashes from a single 64-bit FNV hash.
// This implements the Kirsch-Mitzenmacher optimization, which proves mathematically 
// that we can simulate 'k' independent hash functions using just two base hashes: 
// h_i = h1 + i * h2. This drastically cuts down CPU hashing overhead.
func hash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	// Split the 64-bit hash into two 32-bit halves.
	return uint32(sum), uint32(sum >> 32)
}

// Add sets 'k' bits in the array for the given key.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hash(key)
	for i := uint8(0); i < bf.k; i++ {
		// Calculate the compound hash for this iteration.
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.m
		
		// Map the bit index to a specific uint64 word and flip the specific bit.
		wordIdx := idx / 64
		bitIdx := idx % 64
		bf.bits[wordIdx] |= (1 << bitIdx)
	}
}

// Contains checks if all 'k' bits for the key are set. 
// If any bit is 0, the key absolutely does NOT exist (saving a disk read).
// If all bits are 1, the key MIGHT exist (subject to false positive rate 'p').
func (bf *BloomFilter) Contains(key []byte) bool {
	h1, h2 := hash(key)
	for i := uint8(0); i < bf.k; i++ {
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.m
		wordIdx := idx / 64
		bitIdx := idx % 64
		
		// If the specific bit is zero via bitwise AND, this key was never added.
		if (bf.bits[wordIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom Filter state to a raw byte slice for SSTable persistence.
// Layout: k (1B) | m (8B) | bit array (m/8 B)
func (bf *BloomFilter) Encode() []byte {
	buf := make([]byte, 1+8+(len(bf.bits)*8))
	buf[0] = bf.k
	binary.BigEndian.PutUint64(buf[1:9], bf.m)
	
	offset := 9
	for _, word := range bf.bits {
		binary.BigEndian.PutUint64(buf[offset:offset+8], word)
		offset += 8
	}
	return buf
}

// DecodeBloomFilter reconstructs a BloomFilter from a byte slice loaded from disk.
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("bloom filter data too short: expected at least 9 bytes, got %d", len(data))
	}
	
	k := data[0]
	m := binary.BigEndian.Uint64(data[1:9])
	
	numUint64s := (m + 63) / 64
	bits := make([]uint64, numUint64s)
	
	offset := 9
	for i := 0; i < int(numUint64s); i++ {
		if offset+8 > len(data) {
			return nil, fmt.Errorf("bloom filter data truncated: missing bytes")
		}
		bits[i] = binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
	}
	
	return &BloomFilter{
		bits: bits,
		k:    k,
		m:    m,
	}, nil
}
