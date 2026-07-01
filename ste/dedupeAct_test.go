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
	"encoding/binary"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func ptrTo[T any](v T) *T { return &v }

func TestParseDedupeActMode(t *testing.T) {
	a := assert.New(t)

	a.Equal(dedupeActShadow, parseDedupeActMode("shadow"))
	a.Equal(dedupeActShadow, parseDedupeActMode("  SHADOW ")) // trimmed + case-insensitive
	a.Equal(dedupeActEnforce, parseDedupeActMode("enforce"))
	a.Equal(dedupeActEnforce, parseDedupeActMode("Enforce"))
	a.Equal(dedupeActEnforce, parseDedupeActMode("true")) // convenience alias

	a.Equal(dedupeActOff, parseDedupeActMode(""))
	a.Equal(dedupeActOff, parseDedupeActMode("false"))
	a.Equal(dedupeActOff, parseDedupeActMode("on"))
	a.Equal(dedupeActOff, parseDedupeActMode("1"))
}

func TestDedupeActModeString(t *testing.T) {
	a := assert.New(t)

	a.Equal("off", dedupeActOff.String())
	a.Equal("shadow", dedupeActShadow.String())
	a.Equal("enforce", dedupeActEnforce.String())
}

func TestChunkSpecsFromPlan(t *testing.T) {
	a := assert.New(t)

	a.Nil(chunkSpecsFromPlan(nil))

	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		{Offset: 0, Size: 100},
		{Offset: 100, Size: 250},
		{Offset: 350, Size: 50},
	}}
	specs := chunkSpecsFromPlan(plan)

	a.Equal([]chunkSpec{{0, 100}, {100, 250}, {350, 50}}, specs)
}

func TestDedupeChunkSpecs_OffReturnsNil(t *testing.T) {
	a := assert.New(t)

	plan := &SourceGridPlan{Blocks: []PlannedBlock{{Offset: 0, Size: 100}}}

	// dedupeActOff (the zero value) must never expose a source-grid plan, even if one is set.
	off := &blockBlobSenderBase{dedupeMode: dedupeActOff, dedupePlan: plan}
	a.Nil(off.dedupeChunkSpecs())

	shadow := &blockBlobSenderBase{dedupeMode: dedupeActShadow, dedupePlan: plan}
	a.Equal([]chunkSpec{{0, 100}}, shadow.dedupeChunkSpecs())
}

func TestRawCommittedBlocksFromResponse(t *testing.T) {
	a := assert.New(t)

	var crc [8]byte
	binary.LittleEndian.PutUint64(crc[:], 0x0102030405060708)
	sha := make([]byte, 32)
	sha[0], sha[31] = 0xAA, 0xBB

	resp := blockblob.GetBlockListResponse{}
	resp.CommittedBlocks = []*blockblob.Block{
		{Name: ptrTo("blk-0"), Size: ptrTo(int64(100)), Crc64: crc[:], Sha256: sha},
		{Name: ptrTo("blk-1"), Size: ptrTo(int64(200))}, // no hashes (GetHash off for this block)
	}

	raw := rawCommittedBlocksFromResponse(resp)

	a.Len(raw, 2)
	a.Equal("blk-0", raw[0].Name)
	a.EqualValues(100, raw[0].Size)
	a.Equal(uint64(0x0102030405060708), raw[0].CRC64) // decoded little-endian
	a.Equal(byte(0xAA), raw[0].SHA256[0])
	a.Equal(byte(0xBB), raw[0].SHA256[31])

	a.Equal("blk-1", raw[1].Name)
	a.EqualValues(200, raw[1].Size)
	a.EqualValues(0, raw[1].CRC64)
	a.Equal([32]byte{}, raw[1].SHA256)
}

func TestRecordCommittedBlocks_PopulatesCommittedTableForReference(t *testing.T) {
	a := assert.New(t)

	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)

	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		hashedBlock("alpha", 0, 100),
		hashedBlock("beta", 100, 200),
		{Offset: 300, Size: 50}, // no hashes -> must be skipped
	}}
	const dest = "https://acct.blob.core.windows.net/c/migrated?sig=secret"

	recorded := recordCommittedBlocks(jobID, dest, "etag-xyz", plan)
	a.Equal(2, recorded) // only the two hashed blocks

	committed := dedupeStateForJob(jobID).committed
	a.Equal(2, committed.Len())

	// A later identical block must now resolve to a reference against the recorded target, with the
	// SAS stripped from the stored TargetURI and the destination ETag preserved.
	idx := buildSourceBlockHashIndex(plan)
	target, reference := decideStaging(idx, committed, 0, 100)
	a.True(reference)
	a.Equal("https://acct.blob.core.windows.net/c/migrated", target.TargetURI)
	a.EqualValues(0, target.TargetOffset)
	a.EqualValues(100, target.TargetLength)
	a.EqualValues("etag-xyz", target.ETag)
}

func TestRecordCommittedBlocks_NilPlanIsNoOp(t *testing.T) {
	a := assert.New(t)

	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)

	a.Equal(0, recordCommittedBlocks(jobID, "uri", "etag", nil))
}

func TestDedupeJobStateCounters(t *testing.T) {
	a := assert.New(t)
	st := &dedupeJobState{}

	st.addReferenced(100)
	st.addReferenced(50)
	st.addWouldReference(200)
	st.addSourceStaged(30)
	st.addSourceStaged(70)
	st.addFallback()

	a.EqualValues(2, st.referencedBlocks)
	a.EqualValues(150, st.referencedBytes) // source-read bytes avoided under enforce
	a.EqualValues(1, st.wouldReferenceBlocks)
	a.EqualValues(200, st.wouldReferenceBytes)
	a.EqualValues(2, st.sourceStagedBlocks)
	a.EqualValues(100, st.sourceStagedBytes)
	a.EqualValues(1, st.fallbackBlocks)
}
