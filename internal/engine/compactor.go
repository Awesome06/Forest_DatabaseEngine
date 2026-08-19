package engine

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"log"
	"os"
	"time"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// HeapItem represents the current head of an SSTIterator in the Min-Heap.
type HeapItem struct {
	Key      []byte
	Value    []byte
	OpCode   network.OpCode
	FileSeq  uint64
	Iterator *SSTIterator
}

// MinHeap implements heap.Interface
type MinHeap []HeapItem

func (h MinHeap) Len() int { return len(h) }
func (h MinHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].Key, h[j].Key)
	if cmp == 0 {
		// Higher sequence (newer file) comes first
		return h[i].FileSeq > h[j].FileSeq
	}
	return cmp < 0
}
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(HeapItem))
}
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// CompactL0toL1 performs a K-Way Merge on the provided L0 files and produces an L1 file.
func CompactL0toL1(l0Files []string, l1Filepath string) error {
	var iterators []*SSTIterator
	h := &MinHeap{}
	heap.Init(h)

	// Open all iterators and push their first element
	for i, fpath := range l0Files {
		// Use the index as a proxy for file sequence (assuming l0Files is ordered oldest to newest)
		// For robustness, file names could contain true timestamps, but slice order suffices for V1.
		it, err := NewSSTIterator(fpath, uint64(i))
		if err != nil {
			// Cleanup previously opened iterators
			for _, prev := range iterators {
				prev.Close()
			}
			return err
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

	// We will manually build the L1 SSTable using an SSTableWriter abstraction.
	// (For simplicity here, we create a mini inline writer mimicking FlushMemTable)
	f, err := os.OpenFile(l1Filepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// 1. Setup Writer
	bw, bf, index, currentOffset, lastIndexOffset := initSSTableWriter(f)

	var lastKey []byte

	// 2. K-Way Merge Loop
	for h.Len() > 0 {
		item := heap.Pop(h).(HeapItem)

		// Purge Duplicates: If the next item in the heap has the exact same key,
		// it is older (due to Less() logic), so we advance its iterator and discard it.
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

		// Write item to L1
		// We preserve tombstones by writing OpCode exactly as it is.
		if !bytes.Equal(lastKey, item.Key) {
			currentOffset, lastIndexOffset = writeSSTableEntry(bw, bf, &index, currentOffset, lastIndexOffset, item)
			lastKey = append([]byte(nil), item.Key...) // copy
		}

		// Advance the iterator of the chosen item
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

	// Close all iterators
	for _, it := range iterators {
		it.Close()
	}

	// 3. Finalize L1 SSTable
	return finalizeSSTable(f, bw, bf, index, currentOffset)
}

// Background deletion worker to delete files once refs hit 0
func PurgeObsoleteFiles(v *Version, files []string) {
	// Spinlock wait with sleep to prevent busy waiting.
	// In production, sync.Cond or channel-based notification is better, 
	// but atomic polling is robust for V1 file deletion.
	for v.Refs.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			log.Printf("Failed to purge obsolete file %s: %v", f, err)
		} else {
			log.Printf("Purged obsolete file %s", f)
		}
	}
}

func initSSTableWriter(f *os.File) (*bufio.Writer, *BloomFilter, []IndexEntry, uint64, uint64) {
	bw := bufio.NewWriterSize(f, 4*1024*1024)
	bf := NewBloomFilter(4000, 0.01)
	return bw, bf, nil, 0, 0
}

func writeSSTableEntry(bw *bufio.Writer, bf *BloomFilter, index *[]IndexEntry, currentOffset, lastIndexOffset uint64, item HeapItem) (uint64, uint64) {
	bf.Add(item.Key)
	if currentOffset-lastIndexOffset >= 4096 || currentOffset == 0 {
		*index = append(*index, IndexEntry{Key: item.Key, Offset: currentOffset})
		lastIndexOffset = currentOffset
	}

	var header [7]byte
	header[0] = byte(item.OpCode)
	binary.BigEndian.PutUint16(header[1:3], uint16(len(item.Key)))
	binary.BigEndian.PutUint32(header[3:7], uint32(len(item.Value)))

	n, _ := bw.Write(header[:])
	currentOffset += uint64(n)
	n, _ = bw.Write(item.Key)
	currentOffset += uint64(n)
	n, _ = bw.Write(item.Value)
	currentOffset += uint64(n)

	return currentOffset, lastIndexOffset
}

func finalizeSSTable(f *os.File, bw *bufio.Writer, bf *BloomFilter, index []IndexEntry, currentOffset uint64) error {
	indexBlockOffset := currentOffset

	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(index)))
	n, _ := bw.Write(countBuf[:])
	currentOffset += uint64(n)
	
	for _, entry := range index {
		var idxHeader [10]byte 
		binary.BigEndian.PutUint16(idxHeader[0:2], uint16(len(entry.Key)))
		binary.BigEndian.PutUint64(idxHeader[2:10], entry.Offset)
		n1, _ := bw.Write(idxHeader[0:2])
		n2, _ := bw.Write(entry.Key)
		n3, _ := bw.Write(idxHeader[2:10])
		currentOffset += uint64(n1 + n2 + n3)
	}

	bloomFilterOffset := currentOffset

	bloomData := bf.Encode()
	bw.Write(bloomData)

	var footer [20]byte
	binary.BigEndian.PutUint64(footer[0:8], indexBlockOffset)
	binary.BigEndian.PutUint64(footer[8:16], bloomFilterOffset)
	binary.BigEndian.PutUint32(footer[16:20], 0xF0E57DBB)
	bw.Write(footer[:])

	if err := bw.Flush(); err != nil {
		return err
	}
	return f.Sync()
}
