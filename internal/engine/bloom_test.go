package engine

import (
	"fmt"
	"testing"
)

func TestBloomFilter_Basic(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	key1 := []byte("hello")
	key2 := []byte("world")
	key3 := []byte("missing")

	bf.Add(key1)
	bf.Add(key2)

	if !bf.Contains(key1) {
		t.Errorf("expected to contain key1")
	}
	if !bf.Contains(key2) {
		t.Errorf("expected to contain key2")
	}
	// Note: It's technically possible for Contains to return true for key3 (false positive),
	// but with 2 items and a capacity of 1000, it's extremely unlikely.
	if bf.Contains(key3) {
		t.Errorf("did not expect to contain key3")
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := 10000
	p := 0.01
	bf := NewBloomFilter(n, p)

	// Add n items
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		bf.Add(key)
	}

	// Test n items that were NOT added
	falsePositives := 0
	for i := n; i < 2*n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if bf.Contains(key) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(n)
	t.Logf("Expected False Positive Rate: %.4f", p)
	t.Logf("Actual False Positive Rate:   %.4f (False Positives: %d)", actualRate, falsePositives)

	// We allow a small tolerance above the target rate due to hash collisions
	if actualRate > p+0.01 {
		t.Errorf("false positive rate too high: got %.4f, want ~%.4f", actualRate, p)
	}
}

func TestBloomFilter_EncodeDecode(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add([]byte("test_key"))

	encoded := bf.Encode()
	decoded := DecodeBloomFilter(encoded)

	if decoded == nil {
		t.Fatalf("failed to decode bloom filter")
	}
	if decoded.k != bf.k {
		t.Errorf("k mismatch: got %d, want %d", decoded.k, bf.k)
	}
	if decoded.m != bf.m {
		t.Errorf("m mismatch: got %d, want %d", decoded.m, bf.m)
	}
	if !decoded.Contains([]byte("test_key")) {
		t.Errorf("decoded bloom filter lost the key")
	}
	if decoded.Contains([]byte("missing_key")) {
		t.Errorf("decoded bloom filter returned false positive on missing key")
	}
}
