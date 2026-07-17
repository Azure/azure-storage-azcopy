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

package ste

import (
	"crypto/sha256"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// hashedBlock builds a PlannedBlock whose hashes are derived from content, so identical content
// produces identical CRC64+SHA256 (and therefore a dedupe hit).
func hashedBlock(content string, offset, size int64) PlannedBlock {
	sum := sha256.Sum256([]byte(content))
	// Derive a stable, content-dependent CRC64 surrogate from the first 8 bytes of the SHA256.
	var crc uint64
	for i := 0; i < 8; i++ {
		crc = crc<<8 | uint64(sum[i])
	}
	return PlannedBlock{
		Offset:       offset,
		Size:         size,
		SrcBlockName: content,
		CRC64:        crc,
		SHA256:       sum,
	}
}

func TestBlockHasHashes(t *testing.T) {
	a := assert.New(t)

	a.True(blockHasHashes(hashedBlock("alpha", 0, 10)))
	a.True(blockHasHashes(PlannedBlock{CRC64: 1}))
	a.True(blockHasHashes(PlannedBlock{SHA256: [32]byte{0: 1}}))
	a.False(blockHasHashes(PlannedBlock{Offset: 0, Size: 100})) // no hashes -> not eligible
}

func TestMeasureAndRecord_SkipsBlocksWithoutHashes(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	blocks := []PlannedBlock{
		{Offset: 0, Size: 100},   // no hashes
		{Offset: 100, Size: 200}, // no hashes
	}
	hashed, hits, bytes := measureAndRecord(tbl, common.NewJobID(), "https://acct.blob.core.windows.net/c/b", blocks)

	a.EqualValues(0, hashed)
	a.EqualValues(0, hits)
	a.EqualValues(0, bytes)
	a.Equal(0, tbl.Len())
}

func TestMeasureAndRecord_DistinctBlocksProduceNoHits(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	blocks := []PlannedBlock{
		hashedBlock("a", 0, 10),
		hashedBlock("b", 10, 20),
		hashedBlock("c", 30, 30),
	}
	hashed, hits, bytes := measureAndRecord(tbl, common.NewJobID(), "uri", blocks)

	a.EqualValues(3, hashed)
	a.EqualValues(0, hits)
	a.EqualValues(0, bytes)
	a.Equal(3, tbl.Len())
}

func TestMeasureAndRecord_IntraBlobDuplicate(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	// The third block repeats the content of the first -> one would-be hit of size 10.
	blocks := []PlannedBlock{
		hashedBlock("dup", 0, 10),
		hashedBlock("other", 10, 20),
		hashedBlock("dup", 30, 10),
	}
	hashed, hits, bytes := measureAndRecord(tbl, common.NewJobID(), "uri", blocks)

	a.EqualValues(3, hashed)
	a.EqualValues(1, hits)
	a.EqualValues(10, bytes)
	a.Equal(2, tbl.Len()) // only two distinct contents stored
}

func TestMeasureAndRecord_CrossBlobAccumulation(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()
	job := common.NewJobID()

	// First blob: two distinct blocks, no hits.
	h1, hit1, b1 := measureAndRecord(tbl, job, "uri-blob1", []PlannedBlock{
		hashedBlock("shared", 0, 50),
		hashedBlock("unique1", 50, 25),
	})
	a.EqualValues(2, h1)
	a.EqualValues(0, hit1)
	a.EqualValues(0, b1)

	// Second blob: re-uses "shared" content from the first blob -> one cross-blob would-be hit.
	h2, hit2, b2 := measureAndRecord(tbl, job, "uri-blob2", []PlannedBlock{
		hashedBlock("shared", 0, 50),
		hashedBlock("unique2", 50, 25),
	})
	a.EqualValues(2, h2)
	a.EqualValues(1, hit2)
	a.EqualValues(50, b2)

	a.Equal(3, tbl.Len()) // shared + unique1 + unique2
}

func TestMeasureAndRecord_PopulatesTargetRange(t *testing.T) {
	a := assert.New(t)
	tbl := common.NewDedupeHashTable()

	// A recorded block must carry the target sub-range so Phase 2 can later stage it
	// from [TargetOffset, TargetOffset+TargetLength) of the already-migrated dest blob.
	b := hashedBlock("x", 4096, 1000)
	measureAndRecord(tbl, common.NewJobID(), "https://acct.blob.core.windows.net/c/b", []PlannedBlock{b})

	got, ok := tbl.Lookup(b.CRC64, b.SHA256)
	a.True(ok)
	a.Equal("https://acct.blob.core.windows.net/c/b", got.TargetURI)
	a.EqualValues(4096, got.TargetOffset)
	a.EqualValues(1000, got.TargetLength)
}

func TestSanitizedDestForDedupe_StripsSAS(t *testing.T) {
	a := assert.New(t)

	got := sanitizedDestForDedupe("https://acct.blob.core.windows.net/c/b.bin?sv=2021&sig=SECRET")
	a.Equal("https://acct.blob.core.windows.net/c/b.bin", got)

	// No query string is left unchanged.
	plain := "https://acct.blob.core.windows.net/c/b.bin"
	a.Equal(plain, sanitizedDestForDedupe(plain))
}

func TestDedupePercent(t *testing.T) {
	a := assert.New(t)

	a.EqualValues(0, dedupePercent(0, 0))
	a.EqualValues(0, dedupePercent(0, 10))
	a.EqualValues(50, dedupePercent(5, 10))
	a.EqualValues(100, dedupePercent(10, 10))
}

func TestDedupeStateForJob_IsolatesAndClears(t *testing.T) {
	a := assert.New(t)

	jobA := common.NewJobID()
	jobB := common.NewJobID()

	stA := dedupeStateForJob(jobA)
	stB := dedupeStateForJob(jobB)
	a.NotSame(stA, stB)                  // distinct jobs get distinct tables
	a.Same(stA, dedupeStateForJob(jobA)) // same job returns the same state

	measureAndRecord(stA.table, jobA, "uri", []PlannedBlock{hashedBlock("x", 0, 10)})
	a.Equal(1, stA.table.Len())

	clearDedupeStateForJob(jobA)
	clearDedupeStateForJob(jobB)

	// A fresh state is created after clearing, with an empty table.
	a.Equal(0, dedupeStateForJob(jobA).table.Len())
	clearDedupeStateForJob(jobA)
}

func TestBuildSourceBlockHashIndex(t *testing.T) {
	a := assert.New(t)

	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		hashedBlock("a", 0, 100),
		hashedBlock("b", 100, 200),
		{Offset: 300, Size: 50}, // no hashes -> excluded from the index
	}}
	idx := buildSourceBlockHashIndex(plan)

	a.Len(idx, 2)
	_, ok := idx[srcBlockKey{offset: 0, size: 100}]
	a.True(ok)
	_, ok = idx[srcBlockKey{offset: 100, size: 200}]
	a.True(ok)
	_, ok = idx[srcBlockKey{offset: 300, size: 50}]
	a.False(ok)
}

func TestDecideStaging_NoHashForChunk(t *testing.T) {
	a := assert.New(t)

	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{hashedBlock("a", 0, 100)}})
	committed := common.NewDedupeHashTable()

	// A chunk whose (offset,size) matches no source block has no known hash -> stage from source.
	_, reference := decideStaging(idx, committed, 0, 64)
	a.False(reference)
}

func TestDecideStaging_HashButNotYetCommitted(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})
	committed := common.NewDedupeHashTable() // nothing migrated yet

	_, reference := decideStaging(idx, committed, 0, 100)
	a.False(reference) // hash known, but content not present at the destination -> stage from source
}

func TestDecideStaging_Hit(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})

	committed := common.NewDedupeHashTable()
	committed.Insert(common.BlockEntry{
		CRC64:        b.CRC64,
		SHA256:       b.SHA256,
		TargetURI:    "https://acct.blob.core.windows.net/c/already-migrated",
		TargetOffset: 0,
		TargetLength: 100,
		ETag:         azcore.ETag("etag-1"),
	})

	target, reference := decideStaging(idx, committed, 0, 100)
	a.True(reference)
	a.Equal("https://acct.blob.core.windows.net/c/already-migrated", target.TargetURI)
	a.EqualValues(0, target.TargetOffset)
	a.EqualValues(100, target.TargetLength)
	a.Equal(azcore.ETag("etag-1"), target.ETag)
}
