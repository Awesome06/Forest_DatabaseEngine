package engine

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
)

const maxLevel = 16

// Node represents a node in the lock-free SkipList MemTable.
type Node struct {
	key   []byte
	// val is an atomic pointer to a byte slice to allow lock-free, race-free value updates.
	val   atomic.Pointer[[]byte]
	nexts [maxLevel]atomic.Pointer[Node]
}

// MemTable is a concurrent, lock-free SkipList (readers are lock-free).
type MemTable struct {
	head *Node
	mu   sync.RWMutex
	size atomic.Int64
}

// NewMemTable initializes an empty MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		head: &Node{},
	}
}

// randomLevel generates a random height for a new node.
func randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float32() < 0.25 {
		level++
	}
	return level
}

// Put inserts or updates a key-value pair in the MemTable.
// It uses a mutex to serialize writers but does not block readers.
func (m *MemTable) Put(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var preds [maxLevel]*Node
	curr := m.head

	// Traverse to find the predecessors at each level
	for i := maxLevel - 1; i >= 0; i-- {
		for {
			next := curr.nexts[i].Load()
			if next != nil && bytes.Compare(next.key, key) < 0 {
				curr = next
			} else {
				break
			}
		}
		preds[i] = curr
	}

	// Check if the key already exists
	next := preds[0].nexts[0].Load()
	if next != nil && bytes.Equal(next.key, key) {
		// Key exists: atomically update the value to prevent races with lock-free readers.
		// Note: This does not update the approximate size for the new/old value difference
		// for simplicity in this phase.
		newVal := make([]byte, len(val))
		copy(newVal, val)
		next.val.Store(&newVal)
		return
	}

	// Create new node
	level := randomLevel()
	newNode := &Node{
		key: key,
	}
	newVal := make([]byte, len(val))
	copy(newVal, val)
	newNode.val.Store(&newVal)

	// Increment size (key + val)
	m.size.Add(int64(len(key) + len(val)))

	// Wire the new node to the next nodes
	for i := 0; i < level; i++ {
		newNode.nexts[i].Store(preds[i].nexts[i].Load())
	}

	// Atomically swap the predecessor's next pointers from bottom to top.
	// This ensures lock-free readers traversing from top-to-bottom or left-to-right
	// will see a fully linked node if they encounter it.
	for i := 0; i < level; i++ {
		preds[i].nexts[i].Store(newNode)
	}
}

// Get performs a lock-free search for a key in the MemTable.
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	curr := m.head

	// Traverse lock-free from top to bottom
	for i := maxLevel - 1; i >= 0; i-- {
		for {
			next := curr.nexts[i].Load()
			if next == nil {
				break
			}
			cmp := bytes.Compare(next.key, key)
			if cmp < 0 {
				curr = next
			} else if cmp == 0 {
				// Exact match found
				valPtr := next.val.Load()
				if valPtr != nil {
					return *valPtr, true
				}
				return nil, false
			} else {
				// next.key > key, drop down a level
				break
			}
		}
	}

	// Check the level 0 next node just in case
	next := curr.nexts[0].Load()
	if next != nil && bytes.Equal(next.key, key) {
		valPtr := next.val.Load()
		if valPtr != nil {
			return *valPtr, true
		}
	}

	return nil, false
}

// Size returns the approximate byte size of the MemTable.
func (m *MemTable) Size() int64 {
	return m.size.Load()
}
