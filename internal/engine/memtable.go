// Package engine implements a concurrent lock-free SkipList MemTable that sustains high write
// throughput while serving infinite concurrent readers with zero lock contention.
package engine

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// maxLevel defines the maximum height of the SkipList.
// 16 is mathematically sufficient for up to 2^16 elements with a branching factor of 0.5.
// For p=0.25, it comfortably supports standard 4MB MemTable capacities.
const maxLevel = 16

// Node represents a single key-value entry in the lock-free SkipList.
// Readers traverse this structure without holding any locks.
type Node struct {
	key []byte
	// val is stored as an atomic pointer to a byte slice.
	// This ensures that readers never observe partially updated values
	// when a key is overwritten by a concurrent writer.
	val   atomic.Pointer[[]byte]
	// nexts holds the forward pointers for each level of the SkipList.
	// They are updated atomically from bottom-to-top during insertion
	// to guarantee lock-free readers always see a structurally valid list.
	nexts [maxLevel]atomic.Pointer[Node]
}

// MemTable is a thread-safe, lock-free SkipList serving as the active write buffer.
// It allows single-writer serialization (via mutex) while sustaining infinite 
// concurrent readers (via atomic pointer chasing) with zero contention.
type MemTable struct {
	head *Node
	// mu serializes writers. Readers completely bypass this lock.
	mu   sync.RWMutex
	size atomic.Int64
	frozen atomic.Bool
}

// NewMemTable initializes and returns an empty MemTable with a dummy head node.
func NewMemTable() *MemTable {
	return &MemTable{
		head: &Node{},
	}
}

// randomLevel generates a randomized height for a newly inserted node.
// It uses a geometric distribution with p=0.25 to balance search speed and memory footprint.
func randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float32() < 0.25 {
		level++
	}
	return level
}

// Put inserts or updates a key-value pair in the MemTable.
// It locks the structure to prevent write-write conflicts but relies on atomic
// operations so read-write conflicts are handled lock-free.
func (m *MemTable) Put(key, val []byte) error {
	if m.frozen.Load() {
		return errors.New("memtable is frozen")
	}

	// Serialize writers to prevent structural corruption of the SkipList.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check frozen status under lock in case a concurrent CheckFlush froze it
	// while we were waiting for m.mu.Lock().
	if m.frozen.Load() {
		return errors.New("memtable is frozen")
	}

	var preds [maxLevel]*Node
	curr := m.head

	// Traverse from the top level down, recording the predecessor node at each level.
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

	next := preds[0].nexts[0].Load()
	if next != nil && bytes.Equal(next.key, key) {
		// Key already exists: we only need to update the value.
		// By doing an atomic pointer swap, active readers will either see the old
		// value or the new one, but never corrupted memory.
		var newVal []byte
		if val != nil {
			newVal = make([]byte, len(val))
			copy(newVal, val)
		}
		next.val.Store(&newVal)
		
		// Note: Approximate size is not adjusted on updates to avoid heavy calculation 
		// on the hot path. The 4MB threshold is a soft limit.
		return nil
	}

	// Key does not exist: create a new node and determine its height.
	level := randomLevel()
	
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	
	newNode := &Node{
		key: keyCopy,
	}

	var newVal []byte
	if val != nil {
		newVal = make([]byte, len(val))
		copy(newVal, val)
	}
	newNode.val.Store(&newVal)

	// Increment approximate memory size (useful for flush triggers).
	m.size.Add(int64(len(key) + len(val)))

	// Wire the new node's forward pointers to the existing nodes.
	// This step is completely invisible to readers since newNode is not yet linked.
	for i := 0; i < level; i++ {
		newNode.nexts[i].Store(preds[i].nexts[i].Load())
	}

	// Publish the new node to readers by atomically swapping the predecessors' pointers.
	// We MUST link from bottom (Level 0) to top. If a reader finds the node at a higher level,
	// it will inevitably drop down and expect the lower levels to be linked as well.
	for i := 0; i < level; i++ {
		preds[i].nexts[i].Store(newNode)
	}
	return nil
}

// Get performs a lock-free search across the SkipList.
// It returns the value and a boolean indicating if the key was found.
// A nil/empty value with true indicates a tombstone (deleted key).
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	curr := m.head

	// Traverse lock-free from top to bottom.
	for i := maxLevel - 1; i >= 0; i-- {
		for {
			next := curr.nexts[i].Load()
			if next == nil {
				break
			}
			
			cmp := bytes.Compare(next.key, key)
			if cmp < 0 {
				// Move right if the next key is smaller than our target.
				curr = next
			} else if cmp == 0 {
				// Exact match found. Load the atomic value pointer.
				valPtr := next.val.Load()
				if valPtr != nil {
					return *valPtr, true
				}
				// Failsafe (should never happen by design)
				return nil, false
			} else {
				// next.key > target key, drop down one level.
				break
			}
		}
	}

	// Check the base level (Level 0) next node just in case we overshot.
	next := curr.nexts[0].Load()
	if next != nil && bytes.Equal(next.key, key) {
		valPtr := next.val.Load()
		if valPtr != nil {
			return *valPtr, true
		}
	}

	return nil, false // Key truly not found
}

// Size returns the approximate memory footprint of the MemTable in bytes.
// It is used by the Engine to trigger background SSTable flushes.
func (m *MemTable) Size() int64 {
	return m.size.Load()
}

// Freeze marks the MemTable as immutable. It acquires the write lock to ensure
// all in-flight writers have completed their inserts before it returns.
func (m *MemTable) Freeze() {
	m.mu.Lock()
	m.frozen.Store(true)
	m.mu.Unlock()
}

// Scan returns all key-value entries in the MemTable within the range [startKey, endKey]
// in ascending sorted order. If startKey is nil/empty, iteration starts from the lowest key.
// If endKey is nil/empty, iteration continues through the highest key.
// The returned slice includes tombstones (where len(Value) == 0) so callers can correctly
// suppress older values during layered LSM reads.
// This operation traverses Level 0 of the SkipList lock-free with zero lock contention.
func (m *MemTable) Scan(startKey, endKey []byte) []network.KeyValue {
	var results []network.KeyValue
	curr := m.head

	// Traverse higher levels to locate the first node strictly preceding startKey.
	if len(startKey) > 0 {
		for i := maxLevel - 1; i >= 0; i-- {
			for {
				next := curr.nexts[i].Load()
				if next != nil && bytes.Compare(next.key, startKey) < 0 {
					curr = next
				} else {
					break
				}
			}
		}
	}

	// Traverse Level 0 linked list from curr forward.
	for {
		next := curr.nexts[0].Load()
		if next == nil {
			break
		}

		// If next.key exceeds endKey, terminate early (SkipList is strictly sorted).
		if len(endKey) > 0 && bytes.Compare(next.key, endKey) > 0 {
			break
		}

		if len(startKey) == 0 || bytes.Compare(next.key, startKey) >= 0 {
			valPtr := next.val.Load()
			if valPtr != nil {
				val := *valPtr
				kCopy := make([]byte, len(next.key))
				copy(kCopy, next.key)

				var vCopy []byte
				if val != nil {
					vCopy = make([]byte, len(val))
					copy(vCopy, val)
				}

				results = append(results, network.KeyValue{
					Key:   kCopy,
					Value: vCopy,
				})
			}
		}

		curr = next
	}

	return results
}

