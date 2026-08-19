package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/Forest_DatabaseEngine/internal/network"
)

// SSTableMagic is the validation signature for SSTable files.
const SSTableMagic uint32 = 0xF0E57DBB

// SSTableFooterSize is the fixed byte length of the trailing metadata.
// Layout: IndexOffset(8) + BloomOffset(8) + Magic(4) = 20 bytes
const SSTableFooterSize = 20

// IndexEntry represents a sparse index pointer mapping a key to a 4KB data block offset.
type IndexEntry struct {
	Key    []byte
	Offset uint64
}

// FlushMemTable writes a frozen MemTable to an immutable, append-only SSTable file.
// The file layout is heavily optimized for minimal disk I/O:
// [Data Blocks] -> [Sparse Index] -> [Bloom Filter] -> [Footer]
func FlushMemTable(mt *MemTable, filepath string) error {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create SSTable file %s: %w", filepath, err)
	}
	defer file.Close()

	// Buffer syscalls to aggressively minimize write overhead.
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)

	var index []IndexEntry
	// We size the Bloom Filter for ~4000 items (4MB / ~1KB per item) with a 1% false positive rate.
	// This mathematically eliminates >99% of disk reads for non-existent keys.
	bf := NewBloomFilter(4000, 0.01)

	var currentOffset uint64 = 0
	var lastIndexOffset uint64 = 0

	// 1. Write Data Blocks (Iterating MemTable lock-free)
	curr := mt.head.nexts[0].Load()
	for curr != nil {
		key := curr.key
		valPtr := curr.val.Load()
		if valPtr == nil { // Nil pointer shouldn't happen by design, but we guard.
			curr = curr.nexts[0].Load()
			continue
		}
		val := *valPtr

		// Note: We write tombstones (len(val)==0) to disk so they can shadow older values in L1+.
		bf.Add(key)

		// Create Sparse Index Entry roughly every 4KB of data to bound linear scan time.
		if currentOffset-lastIndexOffset >= 4096 || currentOffset == 0 {
			index = append(index, IndexEntry{Key: key, Offset: currentOffset})
			lastIndexOffset = currentOffset
		}

		// Write Data Block Entry: [OpCode(1B) | KeyLen(2B) | ValLen(4B) | Key | Val]
		var header [7]byte
		header[0] = byte(network.OpPut) // Standardize on OpPut for disk entries.
		binary.BigEndian.PutUint16(header[1:3], uint16(len(key)))
		binary.BigEndian.PutUint32(header[3:7], uint32(len(val)))

		n, _ := bufWriter.Write(header[:])
		currentOffset += uint64(n)
		n, _ = bufWriter.Write(key)
		currentOffset += uint64(n)
		n, _ = bufWriter.Write(val)
		currentOffset += uint64(n)

		curr = curr.nexts[0].Load()
	}

	indexBlockOffset := currentOffset

	// 2. Write Sparse Index
	// NumIndexEntries (4B)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(index)))
	n, _ := bufWriter.Write(countBuf[:])
	currentOffset += uint64(n)
	
	for _, entry := range index {
		var idxHeader [10]byte // KeyLen(2) + Offset(8)
		binary.BigEndian.PutUint16(idxHeader[0:2], uint16(len(entry.Key)))
		binary.BigEndian.PutUint64(idxHeader[2:10], entry.Offset)
		n1, _ := bufWriter.Write(idxHeader[0:2])
		n2, _ := bufWriter.Write(entry.Key)
		n3, _ := bufWriter.Write(idxHeader[2:10])
		currentOffset += uint64(n1 + n2 + n3)
	}

	bloomFilterOffset := currentOffset

	// 3. Write Bloom Filter
	bloomData := bf.Encode()
	bufWriter.Write(bloomData)

	// 4. Write Footer (Fixed Size)
	var footer [SSTableFooterSize]byte
	binary.BigEndian.PutUint64(footer[0:8], indexBlockOffset)
	binary.BigEndian.PutUint64(footer[8:16], bloomFilterOffset)
	binary.BigEndian.PutUint32(footer[16:20], SSTableMagic)
	bufWriter.Write(footer[:])

	// Flush bufio to disk
	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush SSTable buffers: %w", err)
	}

	// Final fsync guarantees durability before updating the manifest.
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync SSTable: %w", err)
	}
	return nil
}

// ReadSSTable performs a point lookup for a key in the SSTable file.
// It aggressively avoids disk I/O by checking the Bloom Filter first.
func ReadSSTable(filepath string, searchKey []byte) ([]byte, bool, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open SSTable for read: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("failed to stat SSTable: %w", err)
	}

	if stat.Size() < SSTableFooterSize {
		return nil, false, fmt.Errorf("SSTable %s is too small (corrupt)", filepath)
	}

	// 1. Read Footer
	var footer [SSTableFooterSize]byte
	if _, err := file.ReadAt(footer[:], stat.Size()-SSTableFooterSize); err != nil {
		return nil, false, fmt.Errorf("failed to read SSTable footer: %w", err)
	}

	magic := binary.BigEndian.Uint32(footer[16:20])
	if magic != SSTableMagic {
		return nil, false, fmt.Errorf("invalid SSTable magic number: expected %X, got %X", SSTableMagic, magic)
	}

	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	bloomOffset := binary.BigEndian.Uint64(footer[8:16])

	// 2. Read Bloom Filter
	bloomLen := stat.Size() - SSTableFooterSize - int64(bloomOffset)
	bloomData := make([]byte, bloomLen)
	if _, err := file.ReadAt(bloomData, int64(bloomOffset)); err != nil {
		return nil, false, fmt.Errorf("failed to read Bloom filter: %w", err)
	}

	bf, err := DecodeBloomFilter(bloomData)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decode Bloom filter: %w", err)
	}
	if !bf.Contains(searchKey) {
		// Bloom filter says NO - skip reading the data blocks entirely!
		return nil, false, nil
	}

	// 3. Read Sparse Index
	indexLen := bloomOffset - indexOffset
	indexData := make([]byte, indexLen)
	if _, err := file.ReadAt(indexData, int64(indexOffset)); err != nil {
		return nil, false, fmt.Errorf("failed to read Sparse Index: %w", err)
	}

	if len(indexData) < 4 {
		return nil, false, fmt.Errorf("invalid sparse index block size")
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
		// We are looking for a key smaller than the very first key in the index.
		// Because the SSTable is sorted, this means the key definitely does not exist.
		firstKeyLen := binary.BigEndian.Uint16(indexData[4:6])
		firstKey := indexData[6 : 6+firstKeyLen]
		if bytes.Compare(firstKey, searchKey) > 0 {
			return nil, false, nil
		}
	}

	blockLen := nextBlockOffset - blockOffset
	blockData := make([]byte, blockLen)
	if _, err := file.ReadAt(blockData, int64(blockOffset)); err != nil {
		return nil, false, fmt.Errorf("failed to read Data Block: %w", err)
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
			// Because the block is sorted, if we pass the target key, it's not here.
			break
		}
		
		bOffset += 7 + kLen + vLen
	}

	return nil, false, nil
}
