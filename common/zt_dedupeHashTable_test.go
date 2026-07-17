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
	"crypto/sha256"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/stretchr/testify/assert"
)

func sha256Of(s string) [32]byte {
	return sha256.Sum256([]byte(s))
}

func newTestEntry(crc64 uint64, content string) BlockEntry {
	return BlockEntry{
		JobID:        NewJobID(),
		CRC64:        crc64,
		SHA256:       sha256Of(content),
		TargetURI:    "https://acct.blob.core.windows.net/c/" + content,
		TargetOffset: 100,
		TargetLength: 200,
		ETag:         azcore.ETag("etag-" + content),
		RefCount:     1,
	}
}

func TestDedupeHashTable_InsertAndLookup(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	e := newTestEntry(100, "alpha")
	stored, inserted := tbl.Insert(e)
	a.True(inserted)
	a.Equal(int64(1), stored.RefCount)
	a.False(stored.CreatedAt.IsZero()) // defaulted on insert
	a.Equal(1, tbl.Len())

	// Hit: both CRC64 and SHA256 match.
	got, ok := tbl.Lookup(e.CRC64, e.SHA256)
	a.True(ok)
	a.Equal(e.TargetURI, got.TargetURI)
	a.Equal(e.TargetOffset, got.TargetOffset)
	a.Equal(e.TargetLength, got.TargetLength)
	a.Equal(e.ETag, got.ETag)

	// Miss: SHA256 differs even though CRC64 matches.
	_, ok = tbl.Lookup(e.CRC64, sha256Of("different"))
	a.False(ok)

	// Miss: unknown CRC64.
	_, ok = tbl.Lookup(999, e.SHA256)
	a.False(ok)
}

func TestDedupeHashTable_InsertDuplicateIncrementsRefCount(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	e := newTestEntry(100, "alpha")
	_, inserted := tbl.Insert(e)
	a.True(inserted)

	stored, inserted := tbl.Insert(e)
	a.False(inserted) // already present
	a.Equal(int64(2), stored.RefCount)
	a.Equal(1, tbl.Len()) // still a single entry
}

func TestDedupeHashTable_CRC64Collision(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	// Two distinct blocks that share a CRC64 bucket but differ in SHA256.
	e1 := newTestEntry(100, "alpha")
	e2 := newTestEntry(100, "beta")
	tbl.Insert(e1)
	tbl.Insert(e2)
	a.Equal(2, tbl.Len())

	// First-pass filter returns both candidates.
	candidates := tbl.LookupByCRC64(100)
	a.Len(candidates, 2)

	// Confirmation by SHA256 resolves to the correct entry.
	got1, ok := tbl.Lookup(100, e1.SHA256)
	a.True(ok)
	a.Equal(e1.TargetURI, got1.TargetURI)

	got2, ok := tbl.Lookup(100, e2.SHA256)
	a.True(ok)
	a.Equal(e2.TargetURI, got2.TargetURI)
}

func TestDedupeHashTable_RefCounting(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	e := newTestEntry(100, "alpha")
	tbl.Insert(e) // RefCount = 1

	count, ok := tbl.IncrementRefCount(e.CRC64, e.SHA256)
	a.True(ok)
	a.Equal(int64(2), count)

	count, ok = tbl.DecrementRefCount(e.CRC64, e.SHA256)
	a.True(ok)
	a.Equal(int64(1), count)

	// Final decrement drops to zero and evicts the entry.
	count, ok = tbl.DecrementRefCount(e.CRC64, e.SHA256)
	a.True(ok)
	a.Equal(int64(0), count)
	a.Equal(0, tbl.Len())

	_, ok = tbl.Lookup(e.CRC64, e.SHA256)
	a.False(ok)
}

func TestDedupeHashTable_Remove(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	e := newTestEntry(100, "alpha")
	tbl.Insert(e)

	a.True(tbl.Remove(e.CRC64, e.SHA256))
	a.Equal(0, tbl.Len())
	a.False(tbl.Remove(e.CRC64, e.SHA256)) // already gone
}

func TestDedupeHashTable_Expiry(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	expired := newTestEntry(100, "alpha")
	expired.TTL = time.Millisecond
	expired.CreatedAt = time.Now().Add(-time.Hour) // already past its TTL
	tbl.Insert(expired)

	// Expired entries are treated as a miss.
	_, ok := tbl.Lookup(expired.CRC64, expired.SHA256)
	a.False(ok)
	a.Empty(tbl.LookupByCRC64(expired.CRC64))

	// A non-positive TTL never expires.
	permanent := newTestEntry(200, "beta")
	permanent.TTL = 0
	permanent.CreatedAt = time.Now().Add(-time.Hour)
	tbl.Insert(permanent)
	_, ok = tbl.Lookup(permanent.CRC64, permanent.SHA256)
	a.True(ok)

	// EvictExpired purges only the expired entry.
	removed := tbl.EvictExpired()
	a.Equal(1, removed)
	a.Equal(1, tbl.Len())
}

func TestDedupeHashTable_Clear(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	tbl.Insert(newTestEntry(100, "alpha"))
	tbl.Insert(newTestEntry(200, "beta"))
	a.Equal(2, tbl.Len())

	tbl.Clear()
	a.Equal(0, tbl.Len())

	// Table is reusable after clearing (e.g. for the next migration).
	_, inserted := tbl.Insert(newTestEntry(300, "gamma"))
	a.True(inserted)
	a.Equal(1, tbl.Len())
}
