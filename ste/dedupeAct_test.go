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
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
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

func TestSourceStageBlockOptionsPinsDedupeSourceETag(t *testing.T) {
	manager := &testJobPartTransferManager{}
	token := "token"
	copier := &urlToBlockBlobCopier{blockBlobSenderBase: blockBlobSenderBase{
		jptm:       manager,
		dedupeMode: dedupeActEnforce,
		dedupeETag: azcore.ETag(`"source-etag"`),
	}}

	options := copier.sourceStageBlockOptions(7, 13, &token)
	assert.Equal(t, blob.HTTPRange{Offset: 7, Count: 13}, options.Range)
	assert.Equal(t, &token, options.CopySourceAuthorization)
	if assert.NotNil(t, options.SourceModifiedAccessConditions) &&
		assert.NotNil(t, options.SourceModifiedAccessConditions.SourceIfMatch) {
		assert.Equal(t, azcore.ETag(`"source-etag"`), *options.SourceModifiedAccessConditions.SourceIfMatch)
	}

	copier.dedupeMode = dedupeActOff
	options = copier.sourceStageBlockOptions(7, 13, &token)
	assert.Nil(t, options.SourceModifiedAccessConditions)
}

func TestStageDedupeTargetCandidatesUsesAlternative(t *testing.T) {
	targets := []common.BlockEntry{
		{TargetURI: "first"},
		{TargetURI: "second"},
		{TargetURI: "third"},
	}
	var called []string
	selected, attempts, staged := stageDedupeTargetCandidates(
		targets,
		func(target common.BlockEntry) error {
			called = append(called, target.TargetURI)
			if target.TargetURI == "second" {
				return nil
			}
			return fmt.Errorf("unusable")
		},
	)

	assert.True(t, staged)
	assert.Equal(t, "second", selected.TargetURI)
	assert.Equal(t, []string{"first", "second"}, called)
	if assert.Len(t, attempts, 2) {
		assert.Error(t, attempts[0].err)
		assert.NoError(t, attempts[1].err)
	}
}

func TestStageDedupeTargetCandidatesAllFail(t *testing.T) {
	targets := []common.BlockEntry{{TargetURI: "first"}, {TargetURI: "second"}}
	selected, attempts, staged := stageDedupeTargetCandidates(
		targets,
		func(common.BlockEntry) error { return fmt.Errorf("unusable") },
	)

	assert.False(t, staged)
	assert.Equal(t, common.BlockEntry{}, selected)
	assert.Len(t, attempts, 2)
}

func TestSourceBlockListOptionsRequestsOnlyCRC64(t *testing.T) {
	a := assert.New(t)

	options := sourceBlockListOptions()
	a.Equal(
		[]blockblob.BlockListIncludeItem{blockblob.BlockListIncludeItemCrc64},
		options.Include,
	)
	a.NotContains(options.Include, blockblob.BlockListIncludeItemSha256)
}

func TestRawCommittedBlocksFromResponse_CRC64Only(t *testing.T) {
	a := assert.New(t)
	var crc [8]byte
	binary.LittleEndian.PutUint64(crc[:], 0x0102030405060708)

	resp := blockblob.GetBlockListResponse{}
	resp.CommittedBlocks = []*blockblob.Block{
		{Name: ptrTo("crc-only"), Size: ptrTo(int64(100)), Crc64: crc[:]},
		{Name: ptrTo("zero-crc"), Size: ptrTo(int64(200)), Crc64: make([]byte, 8)},
	}

	raw := rawCommittedBlocksFromResponse(resp)

	a.Len(raw, 2)
	a.Equal("crc-only", raw[0].Name)
	a.EqualValues(100, raw[0].Size)
	a.Equal(uint64(0x0102030405060708), raw[0].CRC64) // decoded little-endian
	a.True(raw[0].HasCRC64)
	a.False(raw[0].HasSHA256)
	a.False(raw[0].HasHashes)

	a.Equal("zero-crc", raw[1].Name)
	a.EqualValues(0, raw[1].CRC64)
	a.True(raw[1].HasCRC64) // an all-zero CRC64 is still present
	a.False(raw[1].HasHashes)
}

func TestRawCommittedBlocksFromResponse_MalformedCRC64(t *testing.T) {
	sha := make([]byte, 32)
	sha[0], sha[31] = 0xAA, 0xBB

	for _, length := range []int{7, 9} {
		t.Run(fmt.Sprintf("%d_bytes", length), func(t *testing.T) {
			resp := blockblob.GetBlockListResponse{}
			resp.CommittedBlocks = []*blockblob.Block{
				{Name: ptrTo("malformed-crc"), Size: ptrTo(int64(100)), Crc64: make([]byte, length), Sha256: sha},
			}

			raw := rawCommittedBlocksFromResponse(resp)

			a := assert.New(t)
			a.Len(raw, 1)
			a.False(raw[0].HasCRC64)
			a.EqualValues(0, raw[0].CRC64)
			a.True(raw[0].HasSHA256)
			a.False(raw[0].HasHashes)
		})
	}
}

func TestRawCommittedBlocksFromResponse_LegacyCRC64AndSHA256(t *testing.T) {
	a := assert.New(t)
	var crc [8]byte
	binary.LittleEndian.PutUint64(crc[:], 0x0102030405060708)
	sha := make([]byte, 32)
	sha[0], sha[31] = 0xAA, 0xBB

	resp := blockblob.GetBlockListResponse{}
	resp.CommittedBlocks = []*blockblob.Block{
		{Name: ptrTo("legacy"), Size: ptrTo(int64(100)), Offset: ptrTo(int64(42)), Crc64: crc[:], Sha256: sha},
	}

	raw := rawCommittedBlocksFromResponse(resp)

	a.Len(raw, 1)
	a.Equal("legacy", raw[0].Name)
	a.EqualValues(100, raw[0].Size)
	a.EqualValues(42, raw[0].Offset)
	a.True(raw[0].HasOffset)
	a.Equal(uint64(0x0102030405060708), raw[0].CRC64)
	a.True(raw[0].HasCRC64)
	a.Equal(byte(0xAA), raw[0].SHA256[0])
	a.Equal(byte(0xBB), raw[0].SHA256[31])
	a.True(raw[0].HasSHA256)
	a.True(raw[0].HasHashes)
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

func TestRecordCommittedBlocks_RecordsCRCOnlyCandidates(t *testing.T) {
	a := assert.New(t)
	jobID := common.NewJobID()
	defer clearDedupeStateForJob(jobID)

	plan := &SourceGridPlan{Blocks: []PlannedBlock{
		{Offset: 0, Size: 37, CRC64: 10, HasCRC64: true},
		{Offset: 37, Size: 513, CRC64: 20, HasCRC64: true},
		{Offset: 550, Size: 19},
	}}
	recorded := recordCommittedBlocks(
		jobID,
		"https://acct.blob.core.windows.net/c/migrated",
		"?sig=secret",
		azcore.ETag(`"etag-crc-only"`),
		plan,
	)

	a.Equal(2, recorded)
	committed := dedupeStateForJob(jobID).committed
	a.Equal(2, committed.Len())

	first := committed.LookupByCRC64AndLength(10, 37)
	if a.Len(first, 1) {
		a.False(first[0].HasSHA256)
		a.EqualValues(0, first[0].TargetOffset)
		a.EqualValues(37, first[0].TargetLength)
		a.Contains(first[0].TargetURI, "sig=secret")
	}
	second := committed.LookupByCRC64AndLength(20, 513)
	if a.Len(second, 1) {
		a.False(second[0].HasSHA256)
		a.EqualValues(37, second[0].TargetOffset)
	}
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

func TestDedupeJobStateCRCDiscoveryCounters(t *testing.T) {
	st := &dedupeJobState{}
	blocks, bytes := st.addCRCDiscovery(&SourceGridPlan{Blocks: []PlannedBlock{
		{Offset: 0, Size: 3, CRC64: 1, HasCRC64: true},
		{Offset: 3, Size: 7},
		{Offset: 10, Size: 2, CRC64: 2, HasCRC64: true},
	}})

	assert.EqualValues(t, 2, blocks)
	assert.EqualValues(t, 5, bytes)
	snapshot := st.progressSnapshot()
	assert.EqualValues(t, 2, snapshot.crcDiscoveredBlocks)
	assert.EqualValues(t, 5, snapshot.crcDiscoveredBytes)
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
	cpuResponse := blockblob.GetBlockListResponse{
		CRC64CPUTimeUS:  ptrTo(int64(math.MaxInt64)),
		SHA256CPUTimeUS: ptrTo(int64(1)),
		RequestID:       ptrTo("request-final"),
	}
	cpuResponse.CommittedBlocks = []*blockblob.Block{
		{Size: ptrTo(int64(64)), Crc64: make([]byte, 8), Sha256: make([]byte, 32)},
	}
	_, accepted := st.hashCPU.record(hashCPUResponseDelta(cpuResponse))
	a.True(accepted)
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
	a.Contains(string(logBytes), "crc64CpuTimeUs="+fmt.Sprint(math.MaxInt64))
	a.Contains(string(logBytes), "sha256CpuTimeUs=1")
	a.Contains(string(logBytes), "hashCpuTimeUs="+fmt.Sprint(math.MaxInt64))
	a.Contains(string(logBytes), "hashedBlocks=1")
	a.Contains(string(logBytes), "hashedBytes=64")
	a.Contains(string(logBytes), "hashCpuTimeResponses=1")
	a.Contains(string(logBytes), "crc64CpuTimeMissingResponses=0")
	a.Contains(string(logBytes), "sha256CpuTimeMissingResponses=0")
	a.Contains(string(logBytes), "requestIdMissingResponses=0")
	a.Contains(string(logBytes), "crc64CpuTimeInvalidResponses=0")
	a.Contains(string(logBytes), "sha256CpuTimeInvalidResponses=0")
	a.Contains(string(logBytes), "crc64CpuTimeOverflowed=false")
	a.Contains(string(logBytes), "sha256CpuTimeOverflowed=false")
	a.Contains(string(logBytes), "hashCpuTimeOverflowed=true")
	a.Contains(string(logBytes), "hashedBlocksOverflowed=false")
	a.Contains(string(logBytes), "hashedBytesOverflowed=false")
	a.Contains(string(logBytes), "hashMetricsOverflowed=true")
	a.NotContains(string(logBytes), "Delta=")
	a.NotContains(string(logBytes), " requestId=")

	_, exists := dedupeStateForJobIfExists(jobID)
	a.False(exists)
}
