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
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

func TestSourceBlockBlobClientForTransferPreservesVersionOrSnapshot(t *testing.T) {
	a := assert.New(t)
	base, err := blockblob.NewClientWithNoCredential("https://acct.blob.core.windows.net/c/blob", nil)
	a.NoError(err)

	versioned, err := sourceBlockBlobClientForTransfer(base, &TransferInfo{
		VersionID:  "2026-07-15T12:00:00.0000000Z",
		SnapshotID: "ignored-when-version-is-present",
	})
	a.NoError(err)
	versionedURL, err := url.Parse(versioned.URL())
	a.NoError(err)
	a.Equal("2026-07-15T12:00:00.0000000Z", versionedURL.Query().Get("versionid"))
	a.Empty(versionedURL.Query().Get("snapshot"))

	snapshot, err := sourceBlockBlobClientForTransfer(base, &TransferInfo{
		SnapshotID: "2026-07-15T12:30:00.0000000Z",
	})
	a.NoError(err)
	snapshotURL, err := url.Parse(snapshot.URL())
	a.NoError(err)
	a.Equal("2026-07-15T12:30:00.0000000Z", snapshotURL.Query().Get("snapshot"))
	a.Empty(snapshotURL.Query().Get("versionid"))

	unversioned, err := sourceBlockBlobClientForTransfer(base, &TransferInfo{})
	a.NoError(err)
	a.Equal(base.URL(), unversioned.URL())
}

func TestDedupeActDestinationReadyRequiresSASForEnforce(t *testing.T) {
	a := assert.New(t)

	a.True(dedupeActDestinationReady(dedupeActOff, ""))
	a.True(dedupeActDestinationReady(dedupeActShadow, ""))
	a.False(dedupeActDestinationReady(dedupeActEnforce, ""))
	a.False(dedupeActDestinationReady(dedupeActEnforce, "  ?  "))
	a.True(dedupeActDestinationReady(dedupeActEnforce, "?sv=2026&sig=secret"))
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
	a.True(raw[0].HasHashes)

	a.Equal("blk-1", raw[1].Name)
	a.EqualValues(200, raw[1].Size)
	a.EqualValues(0, raw[1].CRC64)
	a.False(raw[1].HasHashes)
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

	recorded := recordCommittedBlocks(jobID, "https://acct.blob.core.windows.net/c/migrated", "?sig=secret", "etag-xyz", plan)
	a.Equal(2, recorded) // only the two hashed blocks

	committed := dedupeStateForJob(jobID).committed
	a.Equal(2, committed.Len())

	// A later identical block must now resolve to a reference against the recorded target, with the
	// executable TargetURI (including SAS, when present) and the destination ETag preserved.
	idx := buildSourceBlockHashIndex(plan)
	target, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
	a.True(reference)
	a.Equal(dest, target.TargetURI)
	a.EqualValues(0, target.TargetOffset)
	a.EqualValues(100, target.TargetLength)
	a.EqualValues("etag-xyz", target.ETag)
}

func TestRecordCommittedBlocks_NilPlanIsNoOp(t *testing.T) {
	a := assert.New(t)

	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)

	a.Equal(0, recordCommittedBlocks(jobID, "uri", "", "etag", nil))
}

func TestRecordCommittedBlocks_MissingETagIsNoOp(t *testing.T) {
	a := assert.New(t)

	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)

	plan := &SourceGridPlan{Blocks: []PlannedBlock{hashedBlock("alpha", 0, 100)}}
	a.Equal(0, recordCommittedBlocks(jobID, "https://acct.blob.core.windows.net/c/migrated", "?sig=secret", "", plan))

	_, exists := dedupeStateForJobIfExists(jobID)
	a.False(exists)
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
	st.addFileStarted()
	st.addFileCommitted()

	a.EqualValues(2, st.referencedBlocks)
	a.EqualValues(150, st.referencedBytes) // source-read bytes avoided under enforce
	a.EqualValues(1, st.wouldReferenceBlocks)
	a.EqualValues(200, st.wouldReferenceBytes)
	a.EqualValues(2, st.sourceStagedBlocks)
	a.EqualValues(100, st.sourceStagedBytes)
	a.EqualValues(1, st.fallbackBlocks)
	a.EqualValues(1, st.filesStarted)
	a.EqualValues(1, st.filesCommitted)
}

func TestDedupeJobSummaryMessageEnforce(t *testing.T) {
	a := assert.New(t)
	st := &dedupeJobState{}
	st.addReferenced(100)
	st.addReferenced(50)
	st.addSourceStaged(30)
	st.addSourceStaged(70)
	st.addFallback()

	a.Equal(
		"dedupe-job-summary(enforce): totalBlocks=4 targetURIBlocks=2 sourceURIBlocks=2 fallbackBlocks=1 avoidedSourceReadBytes=150 totalStagedBytes=250 wanSavingsPercent=60.0",
		dedupeJobSummaryMessage(dedupeActEnforce, st))
}

func TestDedupeJobSummaryMessageShadow(t *testing.T) {
	a := assert.New(t)
	st := &dedupeJobState{}
	st.addWouldReference(150)
	st.addSourceStaged(100)
	st.addSourceStaged(200)

	a.Equal(
		"dedupe-job-summary(shadow): totalBlocks=2 wouldTargetURIBlocks=1 sourceURIBlocks=2 fallbackBlocks=0 potentialAvoidedSourceReadBytes=150 totalStagedBytes=300 potentialWanSavingsPercent=50.0",
		dedupeJobSummaryMessage(dedupeActShadow, st))
}

func TestFinalizeDedupeJobLogsAndClearsState(t *testing.T) {
	a := assert.New(t)
	originalLogFolder := common.AzcopyLogFolder
	common.AzcopyLogFolder = t.TempDir()
	t.Cleanup(func() {
		common.AzcopyLogFolder = originalLogFolder
	})

	jobID := common.NewJobID()
	st := dedupeStateForJob(jobID)
	st.addReferenced(100)
	setDedupeActModeForJob(jobID, dedupeActEnforce)

	var messages []string
	finalizeDedupeJob(
		jobID,
		common.EJobStatus.Failed(),
		2,
		1,
		1,
		func(level common.LogLevel, value string) {
			a.Equal(common.LogInfo, level)
			messages = append(messages, value)
		},
	)

	a.Contains(strings.Join(messages, "\n"), "dedupe-job-summary(enforce)")
	logPath := filepath.Join(common.AzcopyLogFolder, dedupeLogDirectoryName, jobID.String()+".log")
	logBytes, err := os.ReadFile(logPath)
	a.NoError(err)
	a.Contains(string(logBytes), "event=job_complete")
	a.Contains(string(logBytes), "jobId="+jobID.String())
	a.Contains(string(logBytes), `status="Failed"`)
	a.Contains(string(logBytes), "transfersCompleted=2")
	a.Contains(string(logBytes), "transfersSkipped=1")
	a.Contains(string(logBytes), "transfersFailed=1")

	_, exists := dedupeStateForJobIfExists(jobID)
	a.False(exists)
}
