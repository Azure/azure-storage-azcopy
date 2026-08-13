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
	"fmt"
	"sync"
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
	a.True(stored.HasSHA256)           // inferred for backward-compatible callers
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

func TestDedupeHashTable_LookupByCRC64AndLength(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	matching := newTestEntry(100, "matching")
	matching.SHA256 = [32]byte{}
	matching.TargetLength = 512
	stored, inserted := tbl.Insert(matching)
	a.True(inserted)
	a.False(stored.HasSHA256)

	wrongLength := newTestEntry(100, "wrong-length")
	wrongLength.SHA256 = [32]byte{}
	wrongLength.TargetLength = 1024
	tbl.Insert(wrongLength)

	candidates := tbl.LookupByCRC64AndLength(100, 512)
	if a.Len(candidates, 1) {
		a.Equal(matching.TargetURI, candidates[0].TargetURI)
		a.False(candidates[0].HasSHA256)
	}
	a.Empty(tbl.LookupByCRC64AndLength(100, 256))
	a.Empty(tbl.LookupByCRC64AndLength(100, 0))

	// A CRC-only entry must not look like a known all-zero SHA256.
	_, ok := tbl.Lookup(100, [32]byte{})
	a.False(ok)
}

func TestDedupeHashTable_DistinctTargetOccurrencesWithSameHash(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	base := newTestEntry(100, "same-content")
	occurrences := []BlockEntry{base}

	differentURI := base
	differentURI.TargetURI += "-other"
	occurrences = append(occurrences, differentURI)

	differentETag := base
	differentETag.ETag = azcore.ETag("other-etag")
	occurrences = append(occurrences, differentETag)

	differentOffset := base
	differentOffset.TargetOffset++
	occurrences = append(occurrences, differentOffset)

	differentLength := base
	differentLength.TargetLength++
	occurrences = append(occurrences, differentLength)

	for _, entry := range occurrences {
		_, inserted := tbl.Insert(entry)
		a.True(inserted)
	}
	a.Equal(len(occurrences), tbl.Len())
	a.Len(tbl.LookupByCRC64(base.CRC64), len(occurrences))

	stored, inserted := tbl.Insert(base)
	a.False(inserted)
	a.Equal(int64(2), stored.RefCount)
	a.Equal(len(occurrences), tbl.Len())
}

func TestDedupeHashTable_EnrichesExactOccurrenceSHA256(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	entry := newTestEntry(100, "crc-only")
	entry.SHA256 = [32]byte{}
	tbl.Insert(entry)

	a.False(tbl.SetSHA256(
		entry.TargetURI,
		entry.TargetOffset+1,
		entry.TargetLength,
		entry.ETag,
		[32]byte{},
	))

	sameRangeOtherCRC := entry
	sameRangeOtherCRC.CRC64++
	tbl.Insert(sameRangeOtherCRC)
	a.True(tbl.SetSHA256(
		entry.TargetURI,
		entry.TargetOffset,
		entry.TargetLength,
		entry.ETag,
		[32]byte{},
	))

	got, ok := tbl.Lookup(entry.CRC64, [32]byte{})
	a.True(ok)
	a.True(got.HasSHA256)
	a.Equal(entry.TargetURI, got.TargetURI)
	got, ok = tbl.Lookup(sameRangeOtherCRC.CRC64, [32]byte{})
	a.True(ok)
	a.True(got.HasSHA256)

	explicitZero := newTestEntry(200, "explicit-zero")
	explicitZero.SHA256 = [32]byte{}
	explicitZero.HasSHA256 = true
	stored, inserted := tbl.Insert(explicitZero)
	a.True(inserted)
	a.True(stored.HasSHA256)
	got, ok = tbl.Lookup(explicitZero.CRC64, [32]byte{})
	a.True(ok)
	a.Equal(explicitZero.TargetURI, got.TargetURI)

	another := newTestEntry(300, "another-crc-only")
	another.SHA256 = [32]byte{}
	tbl.Insert(another)
	enriched := another
	enriched.SHA256 = sha256Of("now-known")
	stored, inserted = tbl.Insert(enriched)
	a.False(inserted)
	a.True(stored.HasSHA256)
	a.Equal(enriched.SHA256, stored.SHA256)
	a.Equal(int64(2), stored.RefCount)
}

func TestDedupeHashTable_SetSHA256ForCRC64(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()
	entry := newTestEntry(100, "crc-only-bucket")
	entry.SHA256 = [32]byte{}
	tbl.Insert(entry)

	hash := sha256Of("known")
	a.False(tbl.SetSHA256ForCRC64(
		200,
		entry.TargetURI,
		entry.TargetOffset,
		entry.TargetLength,
		entry.ETag,
		hash,
	))
	a.True(tbl.SetSHA256ForCRC64(
		entry.CRC64,
		entry.TargetURI,
		entry.TargetOffset,
		entry.TargetLength,
		entry.ETag,
		hash,
	))
	got, ok := tbl.Lookup(entry.CRC64, hash)
	a.True(ok)
	a.Equal(entry.TargetURI, got.TargetURI)
}

func TestDedupeHashTable_ConcurrentInsertAndLookup(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	const entries = 100
	var wg sync.WaitGroup
	for i := 0; i < entries; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry := newTestEntry(uint64(i%10), fmt.Sprintf("block-%d", i))
			tbl.Insert(entry)
			got, ok := tbl.Lookup(entry.CRC64, entry.SHA256)
			a.True(ok)
			a.Equal(entry.TargetURI, got.TargetURI)
		}()
	}
	wg.Wait()

	a.Equal(entries, tbl.Len())
}

func TestDedupeHashTable_ConcurrentCRCEnrichmentAndEpochEviction(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	const entries = 100
	items := make([]BlockEntry, entries)
	var wg sync.WaitGroup
	for i := range items {
		items[i] = newTestEntry(uint64(i%10), fmt.Sprintf("candidate-%d", i))
		items[i].SHA256 = [32]byte{}
		items[i].TargetLength = int64(i + 1)
		wg.Add(1)
		go func(entry BlockEntry) {
			defer wg.Done()
			tbl.Insert(entry)
		}(items[i])
	}
	wg.Wait()
	a.Equal(entries, tbl.Len())

	for i := range items {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry := items[i]
			sha := sha256Of(fmt.Sprintf("candidate-%d", i))
			a.True(tbl.SetSHA256(
				entry.TargetURI,
				entry.TargetOffset,
				entry.TargetLength,
				entry.ETag,
				sha,
			))
			candidates := tbl.LookupByCRC64AndLength(entry.CRC64, entry.TargetLength)
			if a.Len(candidates, 1) {
				a.Equal(entry.TargetURI, candidates[0].TargetURI)
			}
			_, ok := tbl.Lookup(entry.CRC64, sha)
			a.True(ok)
		}()
	}
	wg.Wait()

	for _, entry := range items {
		entry := entry
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.Equal(1, tbl.RemoveTargetEpoch(entry.TargetURI, entry.ETag))
		}()
	}
	wg.Wait()
	a.Equal(0, tbl.Len())
}

func TestDedupeHashTable_StatsForCRC64(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	tbl.Insert(newTestEntry(100, "alpha"))
	tbl.Insert(newTestEntry(100, "beta"))
	tbl.Insert(newTestEntry(200, "gamma"))

	stats := tbl.StatsForCRC64(100)
	a.Equal(3, stats.Entries)
	a.Equal(2, stats.Buckets)
	a.Equal(2, stats.BucketEntries)

	stats = tbl.StatsForCRC64(999)
	a.Equal(3, stats.Entries)
	a.Equal(2, stats.Buckets)
	a.Equal(0, stats.BucketEntries)
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
	a.Empty(tbl.LookupByCRC64AndLength(expired.CRC64, expired.TargetLength))

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

func TestDedupeHashTable_InsertReplacesExpiredEntry(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	expired := newTestEntry(100, "alpha")
	expired.TargetURI = "target"
	expired.TTL = time.Millisecond
	expired.CreatedAt = time.Now().Add(-time.Hour)
	tbl.Insert(expired)

	replacement := newTestEntry(100, "alpha")
	replacement.TargetURI = "target"
	stored, inserted := tbl.Insert(replacement)

	a.True(inserted)
	a.Equal("target", stored.TargetURI)
	a.Equal(1, tbl.Len())
	got, ok := tbl.Lookup(replacement.CRC64, replacement.SHA256)
	a.True(ok)
	a.Equal("target", got.TargetURI)
}

func TestDedupeHashTable_RemoveTargetEpoch(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()

	targetURI := "https://acct.blob.core.windows.net/c/target"
	etag := azcore.ETag("target-etag")

	first := newTestEntry(100, "first")
	first.SHA256 = [32]byte{}
	first.TargetURI = targetURI
	first.ETag = etag
	second := newTestEntry(200, "second")
	second.SHA256 = [32]byte{}
	second.TargetURI = targetURI
	second.ETag = etag
	second.TargetOffset = 300

	otherEpoch := newTestEntry(300, "other-epoch")
	otherEpoch.TargetURI = targetURI
	otherEpoch.ETag = azcore.ETag("new-etag")
	otherTarget := newTestEntry(400, "other-target")
	otherTarget.ETag = etag

	for _, entry := range []BlockEntry{first, second, otherEpoch, otherTarget} {
		tbl.Insert(entry)
	}

	a.Equal(2, tbl.RemoveTargetEpoch(targetURI, etag))
	a.Equal(2, tbl.Len())
	a.Equal(0, tbl.RemoveTargetEpoch(targetURI, etag))
	_, ok := tbl.Lookup(otherEpoch.CRC64, otherEpoch.SHA256)
	a.True(ok)
	_, ok = tbl.Lookup(otherTarget.CRC64, otherTarget.SHA256)
	a.True(ok)
}

func TestDedupeHashTable_RemoveTargetOccurrence(t *testing.T) {
	a := assert.New(t)
	tbl := NewDedupeHashTable()
	first := newTestEntry(100, "first-occurrence")
	second := newTestEntry(100, "second-occurrence")
	second.SHA256 = first.SHA256
	second.TargetLength = first.TargetLength
	tbl.Insert(first)
	tbl.Insert(second)

	a.True(tbl.RemoveTargetOccurrence(
		first.TargetURI,
		first.TargetOffset,
		first.TargetLength,
		first.ETag,
	))
	a.False(tbl.RemoveTargetOccurrence(
		first.TargetURI,
		first.TargetOffset,
		first.TargetLength,
		first.ETag,
	))
	a.Equal(1, tbl.Len())
	got, ok := tbl.Lookup(second.CRC64, second.SHA256)
	a.True(ok)
	a.Equal(second.TargetURI, got.TargetURI)
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
