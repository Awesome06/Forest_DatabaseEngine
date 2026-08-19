package engine

import (
	"bytes"
	"container/heap"
	"testing"
	"github.com/Forest_DatabaseEngine/internal/network"
)

func TestCompaction_MinHeap_Purge(t *testing.T) {
	h := &MinHeap{}
	heap.Init(h)

	// Simulate Item from old file (FileSeq 0)
	heap.Push(h, HeapItem{
		Key:      []byte("apple"),
		Value:    []byte("red"),
		OpCode:   network.OpEcho,
		FileSeq:  0,
	})

	// Simulate Item from new file (FileSeq 1) overwriting apple
	heap.Push(h, HeapItem{
		Key:      []byte("apple"),
		Value:    []byte("green"),
		OpCode:   network.OpEcho,
		FileSeq:  1,
	})

	// Simulate another key
	heap.Push(h, HeapItem{
		Key:      []byte("banana"),
		Value:    []byte("yellow"),
		OpCode:   network.OpEcho,
		FileSeq:  0,
	})

	if h.Len() != 3 {
		t.Fatalf("heap should have 3 items")
	}

	// 1. Pop should yield "apple" from FileSeq 1 (green)
	first := heap.Pop(h).(HeapItem)
	if string(first.Key) != "apple" || string(first.Value) != "green" {
		t.Fatalf("expected apple=green, got %s=%s", string(first.Key), string(first.Value))
	}

	// Purge logic simulation (exactly like in Compactor.go loop)
	for h.Len() > 0 {
		nextItem := (*h)[0]
		if bytes.Equal(nextItem.Key, first.Key) {
			// Discard the older one
			heap.Pop(h)
		} else {
			break
		}
	}

	// 2. Next pop should yield "banana", because the older apple (red) was purged
	second := heap.Pop(h).(HeapItem)
	if string(second.Key) != "banana" {
		t.Fatalf("expected banana, got %s", string(second.Key))
	}

	// 3. Heap should now be empty
	if h.Len() != 0 {
		t.Fatalf("heap should be empty, but has %d items", h.Len())
	}
}
