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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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
		HasHashes:    true,
	}
}

func TestDedupeProgressSnapshotEnforce(t *testing.T) {
	a := assert.New(t)
	st := &dedupeJobState{}
	st.addFileStarted()
	st.addFileStarted()
	st.addFileCommitted()
	st.addReferenced(100)
	st.addReferenced(50)
	st.addSourceStaged(100)
	st.addFallback()
	st.addSmallFileStarted(1024)
	st.addSmallFileResult(common.ETransferStatus.Success(), 1024)

	a.Equal(
		"filesStarted=2 filesCommitted=1 targetURIBlocks=2 targetURIBytes=150 "+
			"sourceURIBlocks=1 sourceURIBytes=100 fallbackBlocks=1 transferredBlocks=3 "+
			"transferredBytes=250 wanSavingsPercent=60.0 smallFilesStarted=1 "+
			"smallFilesCompleted=1 smallFilesFailed=0 smallFilesSkipped=0 "+
			"smallFilesCanceled=0 smallFilesInProgress=0 smallFileBytesStarted=1024 "+
			"smallFileBytesCompleted=1024 smallFileBytesFailed=0 smallFileBytesSkipped=0 "+
			"smallFileBytesCanceled=0 smallFileBytesInProgress=0",
		st.progressSnapshot().fields(dedupeActEnforce))
}

func TestDedupeProgressSmallFileThreshold(t *testing.T) {
	jptm := &testJobPartTransferManager{
		info: &TransferInfo{
			SourceSize: smallFileProgressThresholdBytes - 1,
			EntityType: common.EEntityType.File(),
		},
		fromTo: common.EFromTo.BlobBlob(),
	}
	assert.True(t, isSmallFileProgressTransfer(jptm))

	jptm.info.SourceSize = 0
	assert.True(t, isSmallFileProgressTransfer(jptm))

	jptm.info.SourceSize = smallFileProgressThresholdBytes
	assert.False(t, isSmallFileProgressTransfer(jptm))

	jptm.info.SourceSize = smallFileProgressThresholdBytes - 1
	jptm.fromTo = common.EFromTo.LocalBlob()
	assert.False(t, isSmallFileProgressTransfer(jptm))

	jptm.fromTo = common.EFromTo.BlobBlob()
	jptm.info.EntityType = common.EEntityType.Folder()
	assert.False(t, isSmallFileProgressTransfer(jptm))
}

func TestDedupeProgressSmallFileResultCounters(t *testing.T) {
	st := &dedupeJobState{}
	st.addSmallFileStarted(100)
	st.addSmallFileStarted(200)
	st.addSmallFileStarted(300)
	st.addSmallFileStarted(400)
	st.addSmallFileStarted(500)

	assert.Equal(
		t,
		"small_file_transfer_complete",
		st.addSmallFileResult(common.ETransferStatus.Success(), 100),
	)
	assert.Equal(
		t,
		"small_file_transfer_failed",
		st.addSmallFileResult(common.ETransferStatus.Failed(), 200),
	)
	assert.Equal(
		t,
		"small_file_transfer_skipped",
		st.addSmallFileResult(common.ETransferStatus.SkippedEntityAlreadyExists(), 300),
	)
	assert.Equal(
		t,
		"small_file_transfer_canceled",
		st.addSmallFileResult(common.ETransferStatus.Cancelled(), 400),
	)

	snapshot := st.progressSnapshot()
	assert.EqualValues(t, 5, snapshot.smallFilesStarted)
	assert.EqualValues(t, 1, snapshot.smallFilesCompleted)
	assert.EqualValues(t, 1, snapshot.smallFilesFailed)
	assert.EqualValues(t, 1, snapshot.smallFilesSkipped)
	assert.EqualValues(t, 1, snapshot.smallFilesCanceled)
	assert.EqualValues(t, 1500, snapshot.smallFileBytesStarted)
	assert.EqualValues(t, 100, snapshot.smallFileBytesCompleted)
	assert.EqualValues(t, 200, snapshot.smallFileBytesFailed)
	assert.EqualValues(t, 300, snapshot.smallFileBytesSkipped)
	assert.EqualValues(t, 400, snapshot.smallFileBytesCanceled)
	assert.Contains(t, snapshot.fields(dedupeActEnforce), "smallFilesInProgress=1")
	assert.Contains(t, snapshot.fields(dedupeActEnforce), "smallFileBytesInProgress=500")
}

func TestDedupeProgressSmallFileStartMarkerIsExactlyOnce(t *testing.T) {
	jptm := &jobPartTransferMgr{}

	assert.False(t, jptm.smallFileProgressStarted())
	assert.True(t, jptm.markSmallFileProgressStarted())
	assert.True(t, jptm.smallFileProgressStarted())
	assert.False(t, jptm.markSmallFileProgressStarted())
}

func TestDedupeProgressSmallFileCancellationNormalizesStatus(t *testing.T) {
	assert.Equal(
		t,
		common.ETransferStatus.Cancelled(),
		normalizeSmallFileResultStatus(common.ETransferStatus.Started(), true),
	)
	assert.Equal(
		t,
		common.ETransferStatus.Started(),
		normalizeSmallFileResultStatus(common.ETransferStatus.Started(), false),
	)
	assert.Equal(
		t,
		common.ETransferStatus.Success(),
		normalizeSmallFileResultStatus(common.ETransferStatus.Success(), true),
	)
}

func TestDedupeJobPartCompletionTracker(t *testing.T) {
	tracker := make(dedupeJobPartCompletionTracker)
	part0 := PartNumber(0)
	part1 := PartNumber(1)

	assert.False(t, tracker.record(nil))
	assert.True(t, tracker.record(&part0))
	assert.False(t, tracker.record(&part0))
	assert.False(t, tracker.allKnownPartsReported(2))
	assert.True(t, tracker.record(&part1))
	assert.True(t, tracker.allKnownPartsReported(2))

	tracker.reset()
	assert.False(t, tracker.allKnownPartsReported(2))
}

func TestDedupePausedRunResetsOnlyAfterAllPartsReport(t *testing.T) {
	assert.True(
		t,
		shouldResetDedupeRunProgress(common.EJobStatus.Paused(), true),
	)
	assert.False(
		t,
		shouldResetDedupeRunProgress(common.EJobStatus.Paused(), false),
	)
	assert.False(
		t,
		shouldResetDedupeRunProgress(common.EJobStatus.Completed(), true),
	)
}

func TestDedupeProgressMessageSanitizesSASAndIsExtractable(t *testing.T) {
	a := assert.New(t)
	jobID := common.NewJobID()
	info := &TransferInfo{
		JobID:       jobID,
		DstFilePath: "folder/file.bin",
		Destination: "https://acct.blob.core.windows.net/c/file.bin?sv=2026&sig=CURRENT_SECRET",
	}
	targetURI := sanitizedDestForDedupe(
		"https://acct.blob.core.windows.net/c/previous.bin?sv=2026&sig=TARGET_SECRET")
	message := dedupeProgressMessage(
		"target_reuse",
		dedupeActEnforce,
		info,
		"blockIndex=3 sourceOffset=4096 blockBytes=1024 targetURI="+fmt.Sprintf("%q", targetURI)+
			" targetOffset=8192 targetLength=1024",
		dedupeProgressSnapshot{
			referencedBlocks:   1,
			referencedBytes:    1024,
			sourceStagedBlocks: 1,
			sourceStagedBytes:  1024,
			filesStarted:       2,
			filesCommitted:     1,
		})

	a.True(strings.HasPrefix(message, dedupeProgressPrefix+" "))
	a.Contains(message, "event=target_reuse")
	a.Contains(message, "targetURI=\"https://acct.blob.core.windows.net/c/previous.bin\"")
	a.Contains(message, "wanSavingsPercent=50.0")
	a.NotContains(message, "CURRENT_SECRET")
	a.NotContains(message, "TARGET_SECRET")
	a.NotContains(message, "sig=")

	root := t.TempDir()
	sink := newDedupeProgressFileSink(func() string { return root })
	a.NoError(sink.write(jobID, message))

	logPath, err := sink.logPath(jobID)
	a.NoError(err)
	a.Equal(filepath.Join(root, dedupeLogDirectoryName, jobID.String()+".log"), logPath)
	logBytes, err := os.ReadFile(logPath)
	a.NoError(err)
	a.Equal(message+"\n", string(logBytes))

	a.NoError(sink.write(jobID, dedupeProgressPrefix+" event=test sig=SHOULD_NOT_LEAK"))
	logBytes, err = os.ReadFile(logPath)
	a.NoError(err)
	a.NotContains(string(logBytes), "SHOULD_NOT_LEAK")
	a.Contains(string(logBytes), "sig=-REDACTED-")
	a.NoError(sink.close(jobID))

	queue := make(chan dedupeProgressEntry, 1)
	a.True(tryEnqueueDedupeProgress(queue, dedupeProgressEntry{jobID: jobID, message: "first"}))
	a.False(tryEnqueueDedupeProgress(queue, dedupeProgressEntry{jobID: jobID, message: "second"}))
}

func TestBlockHasHashes(t *testing.T) {
	a := assert.New(t)

	a.True(blockHasHashes(hashedBlock("alpha", 0, 10)))
	a.True(blockHasHashes(PlannedBlock{HasHashes: true})) // valid hashes may contain all zero bytes
	a.False(blockHasHashes(PlannedBlock{CRC64: 1}))
	a.False(blockHasHashes(PlannedBlock{SHA256: [32]byte{0: 1}}))
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

func TestDestinationWithSASForDedupe_MergesDestinationSAS(t *testing.T) {
	a := assert.New(t)

	got := destinationWithSASForDedupe("https://acct.blob.core.windows.net/c/b.bin", "?sv=2021&sig=SECRET")
	u, err := url.Parse(got)
	a.NoError(err)
	a.Equal("2021", u.Query().Get("sv"))
	a.Equal("SECRET", u.Query().Get("sig"))

	got = destinationWithSASForDedupe("https://acct.blob.core.windows.net/c/b.bin?existing=true", "sv=2021&sig=SECRET")
	u, err = url.Parse(got)
	a.NoError(err)
	a.Equal("true", u.Query().Get("existing"))
	a.Equal("2021", u.Query().Get("sv"))
	a.Equal("SECRET", u.Query().Get("sig"))

	alreadyAuthenticated := "https://acct.blob.core.windows.net/c/b.bin?sv=2021&sig=SECRET"
	u, err = url.Parse(destinationWithSASForDedupe(alreadyAuthenticated, "sv=2021&sig=SECRET"))
	a.NoError(err)
	a.Len(u.Query()["sig"], 1)

	u, err = url.Parse(destinationWithSASForDedupe(
		"https://acct.blob.core.windows.net/c/b.bin?sv=old&sig=OLD",
		"sv=2021&sig=NEW"))
	a.NoError(err)
	a.Equal("2021", u.Query().Get("sv"))
	a.Equal("NEW", u.Query().Get("sig"))
	a.Len(u.Query()["sig"], 1)

	plain := "https://acct.blob.core.windows.net/c/b.bin"
	a.Equal(plain, destinationWithSASForDedupe(plain, ""))
}

func TestSanitizedDestForDedupe_StripsSAS(t *testing.T) {
	a := assert.New(t)

	got := sanitizedDestForDedupe("https://acct.blob.core.windows.net/c/b.bin?sv=2021&sig=SECRET")
	a.Equal("https://acct.blob.core.windows.net/c/b.bin", got)

	// No query string is left unchanged.
	plain := "https://acct.blob.core.windows.net/c/b.bin"
	a.Equal(plain, sanitizedDestForDedupe(plain))

	// Even malformed URLs must not leak their query or fragment to progress output.
	a.Equal("://malformed", sanitizedDestForDedupe("://malformed?sig=SECRET#fragment"))
}

func TestDedupePercent(t *testing.T) {
	a := assert.New(t)

	a.EqualValues(0, dedupePercent(0, 0))
	a.EqualValues(0, dedupePercent(0, 10))
	a.EqualValues(50, dedupePercent(5, 10))
	a.EqualValues(100, dedupePercent(10, 10))
	a.EqualValues(100, dedupePercent(15, 10))
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
	_, reference := decideStaging(idx, committed, 0, 64, "https://acct.blob.core.windows.net/c/current")
	a.False(reference)
}

func TestDecideStaging_HashButNotYetCommitted(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})
	committed := common.NewDedupeHashTable() // nothing migrated yet

	_, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
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

	target, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
	a.True(reference)
	a.Equal("https://acct.blob.core.windows.net/c/already-migrated", target.TargetURI)
	a.EqualValues(0, target.TargetOffset)
	a.EqualValues(100, target.TargetLength)
	a.Equal(azcore.ETag("etag-1"), target.ETag)
}

func TestDecideStaging_RejectsUnsafeTarget(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})

	for _, entry := range []common.BlockEntry{
		{
			CRC64: b.CRC64, SHA256: b.SHA256,
			TargetURI:    "https://acct.blob.core.windows.net/c/missing-etag",
			TargetLength: 100,
		},
		{
			CRC64: b.CRC64, SHA256: b.SHA256,
			TargetURI:    "https://acct.blob.core.windows.net/c/wrong-size",
			TargetLength: 50, ETag: azcore.ETag("etag-1"),
		},
	} {
		committed := common.NewDedupeHashTable()
		committed.Insert(entry)
		_, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current")
		a.False(reference)
	}
}

func TestDecideStaging_SameTargetIsNotReusable(t *testing.T) {
	a := assert.New(t)

	b := hashedBlock("a", 0, 100)
	idx := buildSourceBlockHashIndex(&SourceGridPlan{Blocks: []PlannedBlock{b}})

	committed := common.NewDedupeHashTable()
	committed.Insert(common.BlockEntry{
		CRC64:        b.CRC64,
		SHA256:       b.SHA256,
		TargetURI:    "https://acct.blob.core.windows.net/c/current",
		TargetOffset: 0,
		TargetLength: 100,
		ETag:         azcore.ETag("etag-1"),
	})

	_, reference := decideStaging(idx, committed, 0, 100, "https://acct.blob.core.windows.net/c/current?sig=secret")
	a.False(reference)
}
