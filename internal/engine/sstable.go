package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// IndexEntry represents a pointer to a data block inside the SSTable.
type IndexEntry struct {
	Key    []byte
	Offset uint64
}

// FlushMemTable writes a frozen MemTable to an immutable, append-only SSTable file.
func FlushMemTable(mt *MemTable, filepath string) error {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Use a 4MB buffer to prevent excessive syscalls
	bw := bufio.NewWriterSize(f, 4*1024*1024)

	var index []IndexEntry
	// We size the Bloom Filter for ~4000 items (4MB / ~1KB per item) with a 1% false positive rate.
	bf := NewBloomFilter(4000, 0.01)

	var currentOffset uint64 = 0
	var lastIndexOffset uint64 = 0

	// 1. Write Data Blocks (Iterating MemTable lock-free)
	curr := mt.head.nexts[0].Load()
	for curr != nil {
		key := curr.key
		valPtr := curr.val.Load()
		if valPtr == nil {
			curr = curr.nexts[0].Load()
			continue
		}
		val := *valPtr

		// Add to Bloom Filter
		bf.Add(key)

		// Create Sparse Index Entry every 4KB of data
		if currentOffset-lastIndexOffset >= 4096 || currentOffset == 0 {
			index = append(index, IndexEntry{Key: key, Offset: currentOffset})
			lastIndexOffset = currentOffset
		}

		// Write Data Block Entry: [OpCode (1B) | KeyLen (2B) | ValLen (4B) | Key | Val]
		var header [7]byte
		header[0] = byte(network.OpEcho) // We use OpEcho to signify a standard Put for this phase
		binary.BigEndian.PutUint16(header[1:3], uint16(len(key)))
		binary.BigEndian.PutUint32(header[3:7], uint32(len(val)))

		n, _ := bw.Write(header[:])
		currentOffset += uint64(n)
		n, _ = bw.Write(key)
		currentOffset += uint64(n)
		n, _ = bw.Write(val)
		currentOffset += uint64(n)

		curr = curr.nexts[0].Load()
	}

	indexBlockOffset := currentOffset

	// 2. Write Sparse Index
	// NumIndexEntries (4B)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(index)))
	n, _ := bw.Write(countBuf[:])
	currentOffset += uint64(n)
	
	for _, entry := range index {
		var idxHeader [10]byte // KeyLen(2) + Offset(8)
		binary.BigEndian.PutUint16(idxHeader[0:2], uint16(len(entry.Key)))
		binary.BigEndian.PutUint64(idxHeader[2:10], entry.Offset)
		n1, _ := bw.Write(idxHeader[0:2])
		n2, _ := bw.Write(entry.Key)
		n3, _ := bw.Write(idxHeader[2:10])
		currentOffset += uint64(n1 + n2 + n3)
	}

	bloomFilterOffset := currentOffset

	// 3. Write Bloom Filter
	bloomData := bf.Encode()
	bw.Write(bloomData)

	// 4. Write Footer (Fixed 20 bytes)
	var footer [20]byte
	binary.BigEndian.PutUint64(footer[0:8], indexBlockOffset)
	binary.BigEndian.PutUint64(footer[8:16], bloomFilterOffset)
	binary.BigEndian.PutUint32(footer[16:20], 0xF0E57DBB) // Magic Number
	bw.Write(footer[:])

	// Flush bufio to disk
	if err := bw.Flush(); err != nil {
		return err
	}

	// Final fsync
	return f.Sync()
}

// ReadSSTable performs a point lookup for a key in the SSTable file.
func ReadSSTable(filepath string, searchKey []byte) ([]byte, bool, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, false, err
	}

	if stat.Size() < 20 {
		return nil, false, fmt.Errorf("file too small")
	}

	// 1. Read Footer
	var footer [20]byte
	if _, err := f.ReadAt(footer[:], stat.Size()-20); err != nil {
		return nil, false, err
	}

	magic := binary.BigEndian.Uint32(footer[16:20])
	if magic != 0xF0E57DBB {
		return nil, false, fmt.Errorf("invalid SSTable magic number")
	}

	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	bloomOffset := binary.BigEndian.Uint64(footer[8:16])

	// 2. Read Bloom Filter
	bloomLen := stat.Size() - 20 - int64(bloomOffset)
	bloomData := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomData, int64(bloomOffset)); err != nil {
		return nil, false, err
	}

	bf := DecodeBloomFilter(bloomData)
	if bf == nil || !bf.Contains(searchKey) {
		// Bloom filter says NO - skip reading the data blocks entirely!
		return nil, false, nil
	}

	// 3. Read Sparse Index
	indexLen := bloomOffset - indexOffset
	indexData := make([]byte, indexLen)
	if _, err := f.ReadAt(indexData, int64(indexOffset)); err != nil {
		return nil, false, err
	}

	if len(indexData) < 4 {
		return nil, false, fmt.Errorf("invalid index block")
	}

	numEntries := binary.BigEndian.Uint32(indexData[0:4])
	offset := 4

	var blockOffset uint64 = 0
	var nextBlockOffset uint64 = indexOffset // max end bound

	// Scan sparse index to find the ~4KB block
	for i := uint32(0); i < numEntries; i++ {
		if offset+10 > len(indexData) {
			break
		}
		keyLen := binary.BigEndian.Uint16(indexData[offset : offset+2])
		if offset+2+int(keyLen)+8 > len(indexData) {
			break
		}
		key := indexData[offset+2 : offset+2+int(keyLen)]
		currOffset := binary.BigEndian.Uint64(indexData[offset+2+int(keyLen) : offset+2+int(keyLen)+8])

		cmp := bytes.Compare(key, searchKey)
		if cmp <= 0 {
			blockOffset = currOffset
		} else {
			nextBlockOffset = currOffset
			break
		}
		
		offset += 2 + int(keyLen) + 8
	}

	// 4. Read Data Block
	if blockOffset == 0 && numEntries > 0 {
		// We are looking for a key smaller than the very first key in the index
		// This means it doesn't exist.
		firstKeyLen := binary.BigEndian.Uint16(indexData[4:6])
		firstKey := indexData[6 : 6+firstKeyLen]
		if bytes.Compare(firstKey, searchKey) > 0 {
			return nil, false, nil
		}
	}

	blockLen := nextBlockOffset - blockOffset
	blockData := make([]byte, blockLen)
	if _, err := f.ReadAt(blockData, int64(blockOffset)); err != nil {
		return nil, false, err
	}

	// 5. Linear scan within the 4KB block
	bOffset := 0
	for bOffset+7 <= len(blockData) {
		kLen := int(binary.BigEndian.Uint16(blockData[bOffset+1 : bOffset+3]))
		vLen := int(binary.BigEndian.Uint32(blockData[bOffset+3 : bOffset+7]))
		
		if bOffset+7+kLen+vLen > len(blockData) {
			break
		}
		
		currKey := blockData[bOffset+7 : bOffset+7+kLen]
		currVal := blockData[bOffset+7+kLen : bOffset+7+kLen+vLen]
		
		cmp := bytes.Compare(currKey, searchKey)
		if cmp == 0 {
			return currVal, true, nil
		} else if cmp > 0 {
			// Because it's sorted, if we pass it, it's not here.
			break
		}
		
		bOffset += 7 + kLen + vLen
	}

	return nil, false, nil
}
