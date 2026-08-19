package engine

import (
	"sync/atomic"
)

// Manifest tracks active files for an Engine version.
// Using this structure allows readers to lock-free access files, while 
// writers (compactors) create a new Version and atomically swap the pointer.
type Manifest struct {
	L0Files []string
	L1Files []string
}

// Version represents an immutable point-in-time view of the Manifest.
type Version struct {
	Manifest Manifest
	Refs     atomic.Int32
}

// Acquire increments the reference count.
// Called by TCP readers before reading from SSTables.
func (v *Version) Acquire() {
	v.Refs.Add(1)
}

// Release decrements the reference count.
// Called by TCP readers when they are done. When Refs drops to 0,
// obsolete files from this version can be safely purged.
func (v *Version) Release() {
	v.Refs.Add(-1)
}
