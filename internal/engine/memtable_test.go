package engine

import (

	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestMemTable_BasicOperations(t *testing.T) {
	mt := NewMemTable()

	// Put some keys
	_ = mt.Put([]byte("apple"), []byte("red"))
	_ = mt.Put([]byte("banana"), []byte("yellow"))
	_ = mt.Put([]byte("cherry"), []byte("red"))

	// Get existing keys
	val, ok := mt.Get([]byte("apple"))
	if !ok || string(val) != "red" {
		t.Errorf("expected apple=red, got %s (ok=%v)", string(val), ok)
	}

	val, ok = mt.Get([]byte("banana"))
	if !ok || string(val) != "yellow" {
		t.Errorf("expected banana=yellow, got %s", string(val))
	}

	// Get non-existing key
	_, ok = mt.Get([]byte("durian"))
	if ok {
		t.Errorf("durian should not exist")
	}

	// Update existing key
	_ = mt.Put([]byte("apple"), []byte("green"))
	val, ok = mt.Get([]byte("apple"))
	if !ok || string(val) != "green" {
		t.Errorf("expected apple=green, got %s", string(val))
	}
}

func TestMemTable_SortedOrder(t *testing.T) {
	mt := NewMemTable()
	
	// Insert in reverse order
	_ = mt.Put([]byte("e"), []byte("5"))
	_ = mt.Put([]byte("d"), []byte("4"))
	_ = mt.Put([]byte("c"), []byte("3"))
	_ = mt.Put([]byte("b"), []byte("2"))
	_ = mt.Put([]byte("a"), []byte("1"))

	// Traverse level 0 and ensure sorted order
	curr := mt.head.nexts[0].Load()
	expectedKeys := []string{"a", "b", "c", "d", "e"}
	
	count := 0
	for curr != nil {
		if count >= len(expectedKeys) {
			t.Fatalf("too many items in skiplist")
		}
		if string(curr.key) != expectedKeys[count] {
			t.Errorf("expected key %s, got %s", expectedKeys[count], string(curr.key))
		}
		curr = curr.nexts[0].Load()
		count++
	}

	if count != len(expectedKeys) {
		t.Errorf("expected %d items, got %d", len(expectedKeys), count)
	}
}

func TestMemTable_Concurrency(t *testing.T) {
	t.Run("Concurrency", func(t *testing.T) {
		mt := NewMemTable()
		var wg sync.WaitGroup

		numGoroutines := 100
		numOperations := 1000

		// Readers and Writers running concurrently to trigger race detector
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := []byte(fmt.Sprintf("key-%d-%d", id, rand.Intn(100)))
					val := []byte(fmt.Sprintf("val-%d", j))

					// Randomly Put or Get
					if rand.Float32() < 0.5 {
						_ = mt.Put(key, val)
					} else {
						mt.Get(key)
					}
				}
			}(i)
		}

		wg.Wait()
	})
}
