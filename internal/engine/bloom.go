package engine

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter is a custom bit-array bloom filter.
type BloomFilter struct {
	bits []uint64 // The underlying bit array
	k    uint8    // Number of hash functions
	m    uint64   // Total number of bits
}

// NewBloomFilter calculates the optimal number of bits and hash functions.
func NewBloomFilter(n int, p float64) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if p <= 0.0 || p >= 1.0 {
		p = 0.01 // default to 1% false positive rate
	}

	// m = ceil((n * log(p)) / log(1 / pow(2, log(2))))
	mFloat := math.Ceil((float64(n) * math.Log(p)) / math.Log(1.0/math.Pow(2.0, math.Log(2.0))))
	m := uint64(mFloat)
	if m == 0 {
		m = 64
	}

	// k = round((m / n) * log(2))
	kFloat := math.Round((float64(m) / float64(n)) * math.Log(2.0))
	k := uint8(kFloat)
	if k == 0 {
		k = 1
	}

	numUint64s := (m + 63) / 64
	return &BloomFilter{
		bits: make([]uint64, numUint64s),
		k:    k,
		m:    m,
	}
}

// hash generates two 32-bit hashes from a single 64-bit FNV hash.
func hash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	return uint32(sum), uint32(sum >> 32)
}

// Add inserts a key into the Bloom Filter using Kirsch-Mitzenmacher double-hashing.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hash(key)
	for i := uint8(0); i < bf.k; i++ {
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.m
		wordIdx := idx / 64
		bitIdx := idx % 64
		bf.bits[wordIdx] |= (1 << bitIdx)
	}
}

// Contains checks if a key might be in the set.
func (bf *BloomFilter) Contains(key []byte) bool {
	h1, h2 := hash(key)
	for i := uint8(0); i < bf.k; i++ {
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.m
		wordIdx := idx / 64
		bitIdx := idx % 64
		if (bf.bits[wordIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom Filter to a byte slice for SSTable storage.
// Format: [k (1B)] | [m (8B)] | [bit array...]
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

// DecodeBloomFilter reconstructs a BloomFilter from a byte slice.
func DecodeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 9 {
		return nil
	}
	
	k := data[0]
	m := binary.BigEndian.Uint64(data[1:9])
	
	numUint64s := (m + 63) / 64
	bits := make([]uint64, numUint64s)
	
	offset := 9
	for i := 0; i < int(numUint64s); i++ {
		if offset+8 > len(data) {
			break
		}
		bits[i] = binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
	}
	
	return &BloomFilter{
		bits: bits,
		k:    k,
		m:    m,
	}
}
