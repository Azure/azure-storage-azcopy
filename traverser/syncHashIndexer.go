// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package traverser

import (
	"encoding/hex"
	"sync"
	"sync/atomic"
)

// HashIndexer builds a secondary index of destination objects by their content hash (MD5).
// This enables the sync command to detect when identical content already exists
// at a different path on the destination, allowing a server-side copy instead of re-upload.
// It is safe for concurrent use.
type HashIndexer struct {
	// hashToObject maps hex-encoded MD5 -> first StoredObject found with that hash.
	// Only the first occurrence is stored; subsequent duplicates are ignored since
	// any one of them is sufficient as a copy source.
	hashToObject map[string]StoredObject

	// mu protects hashToObject for concurrent access.
	mu sync.RWMutex

	// missingHashCount tracks the number of destination objects that lacked a content hash.
	// This is reported to the user as a warning since these objects cannot participate in dedup.
	missingHashCount int64
}

// NewHashIndexer creates a new HashIndexer ready to index objects by content hash.
func NewHashIndexer() *HashIndexer {
	return &HashIndexer{
		hashToObject: make(map[string]StoredObject),
	}
}

// Store indexes a StoredObject by its MD5 hash. Objects without an MD5 are counted
// as missing and skipped. Only the first object with a given hash is stored.
func (h *HashIndexer) Store(obj StoredObject) {
	if obj.Md5 == nil || len(obj.Md5) == 0 {
		atomic.AddInt64(&h.missingHashCount, 1)
		return
	}
	key := hex.EncodeToString(obj.Md5)
	h.mu.Lock()
	if _, exists := h.hashToObject[key]; !exists {
		h.hashToObject[key] = obj
	}
	h.mu.Unlock()
}

// Lookup finds a destination object with the given MD5 hash.
// Returns the StoredObject and true if found, or an empty object and false otherwise.
func (h *HashIndexer) Lookup(md5 []byte) (StoredObject, bool) {
	if md5 == nil || len(md5) == 0 {
		return StoredObject{}, false
	}
	key := hex.EncodeToString(md5)
	h.mu.RLock()
	obj, ok := h.hashToObject[key]
	h.mu.RUnlock()
	return obj, ok
}

// MissingHashCount returns the number of destination objects that had no content hash
// and thus could not participate in deduplication.
func (h *HashIndexer) MissingHashCount() int64 {
	return atomic.LoadInt64(&h.missingHashCount)
}

// IndexedCount returns the number of unique hashes indexed.
func (h *HashIndexer) IndexedCount() int {
	h.mu.RLock()
	n := len(h.hashToObject)
	h.mu.RUnlock()
	return n
}
