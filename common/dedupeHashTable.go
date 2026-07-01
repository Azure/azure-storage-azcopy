// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

// BlockEntry is a single block recorded in the dedupe hash table. Each entry
// describes a block of content that already exists at some target location, so
// that a later candidate block with identical content can reference it instead
// of being transferred again.
type BlockEntry struct {
	// JobID identifies the migration job that recorded this block.
	JobID JobID
	// CRC64 is a fast, weak checksum of the block content. It is used as the
	// first-pass bucketing key: cheap to compute for every candidate, but not
	// collision-free, so a match must always be confirmed with SHA256.
	CRC64 uint64
	// SHA256 is the strong content hash of the block. Two blocks are considered
	// identical (a dedupe hit) only when both CRC64 and SHA256 match.
	SHA256 [32]byte
	// TargetURI is the location of the already-stored block content that a hit
	// can be served from / referenced against.
	TargetURI string
	// TargetOffset and TargetLength locate the block's content within TargetURI.
	// Because Azure block IDs are blob-scoped, a hit cannot reuse another blob's
	// block by ID; instead it is served by staging from this sub-range of the
	// already-migrated target blob (Put Block From URL over
	// [TargetOffset, TargetOffset+TargetLength)). Both are therefore required to
	// address content inside an existing blob.
	TargetOffset int64
	TargetLength int64
	// ETag is the concurrency/version token of the target, used to detect whether
	// the referenced content has changed since it was recorded.
	ETag azcore.ETag
	// TTL is the lifetime of this entry relative to CreatedAt. A non-positive
	// TTL means the entry never expires.
	TTL time.Duration
	// CreatedAt is the time the entry was recorded. It is set automatically on
	// insert when left as the zero value.
	CreatedAt time.Time
	// RefCount is the number of candidate blocks currently referencing this entry.
	RefCount int64
}

// IsExpired reports whether the entry has outlived its TTL. A non-positive TTL
// is treated as "never expires".
func (e *BlockEntry) IsExpired() bool {
	if e.TTL <= 0 {
		return false
	}
	return time.Since(e.CreatedAt) > e.TTL
}

// DedupeHashTable is an in-memory, concurrency-safe table of recorded blocks
// used for dedupe candidate lookup during a migration.
//
// Lifecycle: create one table per migration with NewDedupeHashTable, use it for
// the duration of the migration, then drop the reference (or call Clear) once the
// migration completes so the memory is released until the next migration starts.
//
// Entries are bucketed by their CRC64 so that a caller can do a cheap first-pass
// check (LookupByCRC64) using only the weak checksum, and confirm a true match
// with the strong SHA256 hash (Lookup). Within a bucket, entries are uniquely
// identified by their SHA256, so CRC64 collisions are handled correctly.
type DedupeHashTable struct {
	mu sync.RWMutex
	// buckets maps a CRC64 checksum to the entries sharing it. The slice is
	// almost always of length one; it only grows when distinct blocks happen to
	// share a CRC64 value.
	buckets map[uint64][]*BlockEntry
}

// NewDedupeHashTable returns an empty hash table ready for a single migration.
func NewDedupeHashTable() *DedupeHashTable {
	return &DedupeHashTable{
		buckets: make(map[uint64][]*BlockEntry),
	}
}

// findLocked returns the entry matching both crc64 and sha256 along with its
// index inside its bucket, or (nil, -1) if not present. Callers must hold the
// appropriate lock.
func (t *DedupeHashTable) findLocked(crc64 uint64, sha256 [32]byte) (*BlockEntry, int) {
	for i, e := range t.buckets[crc64] {
		if e.SHA256 == sha256 {
			return e, i
		}
	}
	return nil, -1
}

// removeAtLocked removes the entry at index idx from the bucket for crc64,
// deleting the bucket entirely once it becomes empty. Callers must hold the
// write lock.
func (t *DedupeHashTable) removeAtLocked(crc64 uint64, idx int) {
	bucket := t.buckets[crc64]
	bucket = append(bucket[:idx], bucket[idx+1:]...)
	if len(bucket) == 0 {
		delete(t.buckets, crc64)
	} else {
		t.buckets[crc64] = bucket
	}
}

// Insert records a block in the table.
//
// If a block with the same CRC64 and SHA256 is already present, its RefCount is
// incremented and the existing (now updated) entry is returned with inserted set
// to false. Otherwise the supplied entry is stored — defaulting CreatedAt to the
// current time and RefCount to 1 when left unset — and returned with inserted set
// to true.
func (t *DedupeHashTable) Insert(entry BlockEntry) (stored BlockEntry, inserted bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, _ := t.findLocked(entry.CRC64, entry.SHA256); existing != nil {
		existing.RefCount++
		return *existing, false
	}

	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.RefCount < 1 {
		entry.RefCount = 1
	}

	e := entry // store a copy so the caller cannot mutate table state by reference
	t.buckets[entry.CRC64] = append(t.buckets[entry.CRC64], &e)
	return e, true
}

// Lookup reports whether a candidate block with the given CRC64 and SHA256 has a
// matching, non-expired entry in the table. The returned BlockEntry is a snapshot
// copy; use IncrementRefCount / DecrementRefCount to mutate reference counts.
func (t *DedupeHashTable) Lookup(crc64 uint64, sha256 [32]byte) (BlockEntry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	e, _ := t.findLocked(crc64, sha256)
	if e == nil || e.IsExpired() {
		return BlockEntry{}, false
	}
	return *e, true
}

// LookupByCRC64 returns snapshot copies of all non-expired entries that share the
// given CRC64. It is intended as a cheap first-pass filter: compute the weak
// checksum for a candidate, and only compute the strong SHA256 (and call Lookup)
// when this returns one or more potential matches.
func (t *DedupeHashTable) LookupByCRC64(crc64 uint64) []BlockEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var out []BlockEntry
	for _, e := range t.buckets[crc64] {
		if !e.IsExpired() {
			out = append(out, *e)
		}
	}
	return out
}

// IncrementRefCount increases the reference count of the matching entry and
// returns the new count. The boolean is false if no matching entry exists.
func (t *DedupeHashTable) IncrementRefCount(crc64 uint64, sha256 [32]byte) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, _ := t.findLocked(crc64, sha256)
	if e == nil {
		return 0, false
	}
	e.RefCount++
	return e.RefCount, true
}

// DecrementRefCount decreases the reference count of the matching entry. When the
// count reaches zero the entry is evicted and (0, true) is returned. The boolean
// is false if no matching entry exists.
func (t *DedupeHashTable) DecrementRefCount(crc64 uint64, sha256 [32]byte) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, idx := t.findLocked(crc64, sha256)
	if e == nil {
		return 0, false
	}
	e.RefCount--
	if e.RefCount <= 0 {
		t.removeAtLocked(crc64, idx)
		return 0, true
	}
	return e.RefCount, true
}

// Remove deletes the matching entry regardless of its reference count. It returns
// true if an entry was removed.
func (t *DedupeHashTable) Remove(crc64 uint64, sha256 [32]byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, idx := t.findLocked(crc64, sha256); idx >= 0 {
		t.removeAtLocked(crc64, idx)
		return true
	}
	return false
}

// EvictExpired removes every entry whose TTL has elapsed and returns the number
// of entries removed.
func (t *DedupeHashTable) EvictExpired() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	removed := 0
	for crc64, bucket := range t.buckets {
		kept := bucket[:0]
		for _, e := range bucket {
			if e.IsExpired() {
				removed++
				continue
			}
			kept = append(kept, e)
		}
		if len(kept) == 0 {
			delete(t.buckets, crc64)
		} else {
			t.buckets[crc64] = kept
		}
	}
	return removed
}

// Len returns the number of entries currently stored in the table.
func (t *DedupeHashTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	n := 0
	for _, bucket := range t.buckets {
		n += len(bucket)
	}
	return n
}

// Clear removes all entries from the table. Call this when a migration finishes
// to release the table's memory until the next migration begins.
func (t *DedupeHashTable) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.buckets = make(map[uint64][]*BlockEntry)
}
