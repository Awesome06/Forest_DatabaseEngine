package engine

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// HeapItem represents the current head of an SSTIterator inside the Min-Heap.
// We track the FileSeq to ensure that when keys collide, the mutation from the
// newer file (higher sequence) shadows the older one.
type HeapItem struct {
	Key      []byte
	Value    []byte
	OpCode   network.OpCode
	FileSeq  uint64
	Iterator *SSTIterator
}

// MinHeap implements heap.Interface for K-Way Merging of SSTables.
type MinHeap []HeapItem

func (h MinHeap) Len() int { return len(h) }
func (h MinHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].Key, h[j].Key)
	if cmp == 0 {
		// If keys are identical, prioritize the newer file (higher sequence number)
		// so it pops first and shadows the older entry.
		return h[i].FileSeq > h[j].FileSeq
	}
	return cmp < 0
}
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an item to the heap.
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(HeapItem))
}

// Pop removes and returns the smallest item from the heap.
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// CompactL0toL1 performs a K-Way Merge on multiple overlapping L0 files to produce a single sorted L1 file.
// This prevents read-amplification by ensuring that older values and duplicates are aggressively purged.
func CompactL0toL1(l0Files []string, l1Filepath string) error {
	var iterators []*SSTIterator
	h := &MinHeap{}
	heap.Init(h)

	// 1. Initialize iterators for all L0 files and push their first elements to seed the heap.
	for i, fpath := range l0Files {
		// File sequence is proxied by the slice index (ordered oldest to newest).
		it, err := NewSSTIterator(fpath, uint64(i))
		if err != nil {
			// Clean up safely if we fail mid-initialization.
			for _, prev := range iterators {
				prev.Close()
			}
			return fmt.Errorf("failed to open iterator for %s: %w", fpath, err)
		}
		iterators = append(iterators, it)

		if it.Next() {
			heap.Push(h, HeapItem{
				Key:      it.Key(),
				Value:    it.Value(),
				OpCode:   it.OpCode(),
				FileSeq:  it.FileSeq(),
				Iterator: it,
			})
		}
	}

	// Ensure all iterators are closed when compaction completes.
	defer func() {
		for _, it := range iterators {
			it.Close()
		}
	}()

	file, err := os.OpenFile(l1Filepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create L1 SSTable file %s: %w", l1Filepath, err)
	}
	defer file.Close()

	// 2. Setup the target SSTable writer.
	bufWriter, bf, index, currentOffset, lastIndexOffset := initSSTableWriter(file)
	var lastKey []byte

	// 3. K-Way Merge Loop
	for h.Len() > 0 {
		item := heap.Pop(h).(HeapItem)

		// Purge Duplicates: If the next items in the heap have the exact same key,
		// they are older (enforced by MinHeap.Less), so we aggressively advance their 
		// iterators and discard them, keeping only the freshest mutation.
		for h.Len() > 0 {
			nextItem := (*h)[0]
			if bytes.Equal(nextItem.Key, item.Key) {
				popped := heap.Pop(h).(HeapItem)
				if popped.Iterator.Next() {
					heap.Push(h, HeapItem{
						Key:      popped.Iterator.Key(),
						Value:    popped.Iterator.Value(),
						OpCode:   popped.Iterator.OpCode(),
						FileSeq:  popped.Iterator.FileSeq(),
						Iterator: popped.Iterator,
					})
				}
			} else {
				break
			}
		}

		// Write the freshest item to the L1 file.
		// Note: We intentionally write tombstones (OpDelete) into L1 so they can continue 
		// to shadow values in L2+ if multi-level compaction is implemented later.
		if !bytes.Equal(lastKey, item.Key) {
			var err error
			currentOffset, lastIndexOffset, err = writeSSTableEntry(bufWriter, bf, &index, currentOffset, lastIndexOffset, item)
			if err != nil {
				return fmt.Errorf("compaction failed to write entry: %w", err)
			}
			lastKey = append([]byte(nil), item.Key...) // copy to avoid memory aliasing
		}

		// Advance the iterator of the chosen item.
		if item.Iterator.Next() {
			heap.Push(h, HeapItem{
				Key:      item.Iterator.Key(),
				Value:    item.Iterator.Value(),
				OpCode:   item.Iterator.OpCode(),
				FileSeq:  item.Iterator.FileSeq(),
				Iterator: item.Iterator,
			})
		}
	}

	// 4. Finalize the L1 SSTable (Indexes, Bloom Filter, Footer).
	if err := finalizeSSTable(file, bufWriter, bf, index, currentOffset); err != nil {
		return fmt.Errorf("failed to finalize L1 SSTable: %w", err)
	}
	return nil
}

// PurgeObsoleteFiles executes the physical deletion of old SSTable files in the background.
// It uses an RCU (Read-Copy-Update) spinlock to guarantee that no active network readers 
// are referencing the file before the OS unlinks it.
func PurgeObsoleteFiles(v *Version, files []string) {
	// Wait until all active readers that acquired this version have released it.
	for {
		refs := v.Refs.Load()
		if refs == 0 {
			if v.Refs.CompareAndSwap(0, -1) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			log.Printf("critical: failed to purge obsolete file %s (possible disk leak): %v", f, err)
		}
	}
}

// initSSTableWriter primes the internal buffers and structures for writing an SSTable.
func initSSTableWriter(file *os.File) (*bufio.Writer, *BloomFilter, []IndexEntry, uint64, uint64) {
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)
	bf := NewBloomFilter(4000, 0.01)
	return bufWriter, bf, nil, 0, 0
}

// writeSSTableEntry serializes a single merged record into the data block phase of the SSTable.
func writeSSTableEntry(bufWriter *bufio.Writer, bf *BloomFilter, index *[]IndexEntry, currentOffset, lastIndexOffset uint64, item HeapItem) (uint64, uint64, error) {
	bf.Add(item.Key)
	
	if currentOffset-lastIndexOffset >= 4096 || currentOffset == 0 {
		*index = append(*index, IndexEntry{Key: item.Key, Offset: currentOffset})
		lastIndexOffset = currentOffset
	}

	var header [7]byte
	header[0] = byte(item.OpCode)
	binary.BigEndian.PutUint16(header[1:3], uint16(len(item.Key)))
	binary.BigEndian.PutUint32(header[3:7], uint32(len(item.Value)))

	n, err := bufWriter.Write(header[:])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write header: %w", err)
	}
	currentOffset += uint64(n)
	
	n, err = bufWriter.Write(item.Key)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write key: %w", err)
	}
	currentOffset += uint64(n)
	
	n, err = bufWriter.Write(item.Value)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write value: %w", err)
	}
	currentOffset += uint64(n)

	return currentOffset, lastIndexOffset, nil
}

// finalizeSSTable appends the trailing metadata layers to seal the SSTable.
func finalizeSSTable(file *os.File, bufWriter *bufio.Writer, bf *BloomFilter, index []IndexEntry, currentOffset uint64) error {
	indexBlockOffset := currentOffset

	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(index)))
	n, err := bufWriter.Write(countBuf[:])
	if err != nil {
		return fmt.Errorf("failed to write index count: %w", err)
	}
	currentOffset += uint64(n)
	
	for _, entry := range index {
		var idxHeader [10]byte 
		binary.BigEndian.PutUint16(idxHeader[0:2], uint16(len(entry.Key)))
		binary.BigEndian.PutUint64(idxHeader[2:10], entry.Offset)
		n1, err := bufWriter.Write(idxHeader[0:2])
		if err != nil { return fmt.Errorf("failed to write index header: %w", err) }
		n2, err := bufWriter.Write(entry.Key)
		if err != nil { return fmt.Errorf("failed to write index key: %w", err) }
		n3, err := bufWriter.Write(idxHeader[2:10])
		if err != nil { return fmt.Errorf("failed to write index offset: %w", err) }
		currentOffset += uint64(n1 + n2 + n3)
	}

	bloomFilterOffset := currentOffset
	bloomData := bf.Encode()
	if _, err := bufWriter.Write(bloomData); err != nil {
		return fmt.Errorf("failed to write bloom filter: %w", err)
	}

	var footer [SSTableFooterSize]byte
	binary.BigEndian.PutUint64(footer[0:8], indexBlockOffset)
	binary.BigEndian.PutUint64(footer[8:16], bloomFilterOffset)
	binary.BigEndian.PutUint32(footer[16:20], SSTableMagic)
	if _, err := bufWriter.Write(footer[:]); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync file: %w", err)
	}
	return nil
}
