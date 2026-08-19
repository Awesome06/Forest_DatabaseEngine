package engine

import (
	"sync/atomic"
)

// Manifest tracks all active SSTable files comprising a specific point-in-time state of the database.
// By isolating the file lists inside an immutable Manifest, background compaction workers can freely 
// generate a new Manifest and atomically swap the engine pointer, leaving concurrent readers completely unaffected.
type Manifest struct {
	L0Files []string
	L1Files []string
}

// Version represents an immutable, reference-counted snapshot of the Manifest.
// This is the core of our Multi-Version Concurrency Control (MVCC) and RCU design.
type Version struct {
	Manifest Manifest
	Refs     atomic.Int32
}

// Acquire increments the reference count for this specific version.
// It must be called by readers (e.g., Get operations) before they begin traversing the SSTables
// to ensure the underlying files are not physically unlinked by a background compaction worker.
func (v *Version) Acquire() {
	v.Refs.Add(1)
}

// Release decrements the reference count.
// It must be called via defer by readers when their operation completes.
// Once a Version is detached from the active database pointer and its Refs drop to 0, 
// the compaction workers are safely permitted to unlink its obsolete files from disk.
func (v *Version) Release() {
	v.Refs.Add(-1)
}
