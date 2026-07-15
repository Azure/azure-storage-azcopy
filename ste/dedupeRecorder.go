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
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// This file owns the per-job state shared by the block-level dedupe prototype. Observe mode
// records and measures potential hits without changing transfers; act mode records only committed
// destination blocks and uses them to make safe staging decisions.

// dedupeJobState holds a single job's dedupe tables plus cumulative would-be-hit counters.
type dedupeJobState struct {
	// table is the Phase 1 measurement table: blocks are recorded pre-write purely to count the
	// would-be-hit rate, so it must NOT be used to decide a real reference (the content may not yet
	// exist at the destination).
	table *common.DedupeHashTable

	// committed is the Phase 2 table: it holds only blocks confirmed committed at the destination
	// (recorded in the sender Epilogue after a successful CommitBlockList). A reference is only ever
	// served from an entry in this table, so the target content is guaranteed to exist.
	committed *common.DedupeHashTable

	// Counters are cumulative across every blob processed for the job, and are updated
	// atomically because transfers (and therefore observeSourceGrid calls) run concurrently.
	hashedBlocks   int64 // blocks that carried crc64+sha256 (i.e. were eligible for dedupe)
	wouldBeHits    int64 // eligible blocks whose content was already recorded by an earlier block
	dedupableBytes int64 // total size of would-be-hit blocks: bytes that need not be re-transferred

	// Phase 2 "act" counters (also cumulative + atomic). Together they quantify how many source
	// reads dedupe actually avoided (enforce) or would avoid (shadow).
	referencedBlocks     int64 // enforce: blocks staged from the destination instead of the source
	referencedBytes      int64 // enforce: source-read bytes avoided
	wouldReferenceBlocks int64 // shadow: blocks that would be referenced under enforce
	wouldReferenceBytes  int64 // shadow: source-read bytes that would be avoided under enforce
	sourceStagedBlocks   int64 // blocks staged from the source (real source reads)
	sourceStagedBytes    int64 // bytes staged from the source
	fallbackBlocks       int64 // enforce: hits whose target reference failed and fell back to source

	actMode int32 // active dedupeActMode for this job
}

// addReferenced records a block that enforce mode staged from the destination (a source read avoided).
func (s *dedupeJobState) addReferenced(size int64) {
	atomic.AddInt64(&s.referencedBlocks, 1)
	atomic.AddInt64(&s.referencedBytes, size)
}

// addWouldReference records a block that shadow mode would have referenced under enforce.
func (s *dedupeJobState) addWouldReference(size int64) {
	atomic.AddInt64(&s.wouldReferenceBlocks, 1)
	atomic.AddInt64(&s.wouldReferenceBytes, size)
}

// addSourceStaged records a block that was actually staged from the source (a real source read).
func (s *dedupeJobState) addSourceStaged(size int64) {
	atomic.AddInt64(&s.sourceStagedBlocks, 1)
	atomic.AddInt64(&s.sourceStagedBytes, size)
}

// addFallback records an enforce hit whose target reference failed and fell back to the source.
func (s *dedupeJobState) addFallback() {
	atomic.AddInt64(&s.fallbackBlocks, 1)
}

var (
	dedupeJobsMu sync.Mutex
	dedupeJobs   = make(map[common.JobID]*dedupeJobState)
)

// dedupeStateForJob returns the dedupe state for a job, creating it on first use. Callers are
// limited to the observe and act paths, so the default transfer path allocates no dedupe state.
func dedupeStateForJob(jobID common.JobID) *dedupeJobState {
	dedupeJobsMu.Lock()
	defer dedupeJobsMu.Unlock()

	st, ok := dedupeJobs[jobID]
	if !ok {
		st = &dedupeJobState{
			table:     common.NewDedupeHashTable(),
			committed: common.NewDedupeHashTable(),
		}
		dedupeJobs[jobID] = st
	}
	return st
}

func dedupeStateForJobIfExists(jobID common.JobID) (*dedupeJobState, bool) {
	dedupeJobsMu.Lock()
	defer dedupeJobsMu.Unlock()

	st, ok := dedupeJobs[jobID]
	return st, ok
}

func setDedupeActModeForJob(jobID common.JobID, mode dedupeActMode) {
	if mode == dedupeActOff {
		return
	}
	atomic.StoreInt32(&dedupeStateForJob(jobID).actMode, int32(mode))
}

// clearDedupeStateForJob drops a job's dedupe state to release its memory. It is safe to call
// when no state exists and is invoked after a job reaches a terminal status.
func clearDedupeStateForJob(jobID common.JobID) {
	dedupeJobsMu.Lock()
	defer dedupeJobsMu.Unlock()

	if st, ok := dedupeJobs[jobID]; ok {
		st.table.Clear()
		st.committed.Clear()
		delete(dedupeJobs, jobID)
	}
}

// recordCommittedBlocks records a freshly-committed destination blob's hashed blocks into the job's
// committed table, so that later blocks with identical content can be served from this blob. It is
// called from the sender Epilogue after CommitBlockList succeeds, with the destination's real ETag.
// Because the destination is a byte-identical copy of the source, each block lives at the same
// offset/length in the target blob as in the source.
func recordCommittedBlocks(jobID common.JobID, destURI, destinationSAS string, etag azcore.ETag, plan *SourceGridPlan) (recorded int) {
	return recordCommittedBlocksWithObserver(jobID, destURI, destinationSAS, etag, plan, nil)
}

type dedupeTableRecordEvent struct {
	Block       PlannedBlock
	Stored      common.BlockEntry
	Inserted    bool
	PreHit      bool
	TableStats  common.DedupeHashTableStats
	RecordIndex int
}

func recordCommittedBlocksWithObserver(jobID common.JobID, destURI, destinationSAS string, etag azcore.ETag, plan *SourceGridPlan, observer func(dedupeTableRecordEvent)) (recorded int) {
	if plan == nil || etag == "" {
		return 0
	}
	st := dedupeStateForJob(jobID)
	target := destinationWithSASForDedupe(destURI, destinationSAS)
	for _, b := range plan.Blocks {
		if !blockHasHashes(b) {
			continue
		}
		stored, inserted := st.committed.Insert(common.BlockEntry{
			JobID:        jobID,
			CRC64:        b.CRC64,
			SHA256:       b.SHA256,
			TargetURI:    target,
			TargetOffset: b.Offset,
			TargetLength: b.Size,
			ETag:         etag,
		})
		recorded++
		if observer != nil {
			observer(dedupeTableRecordEvent{
				Block:       b,
				Stored:      stored,
				Inserted:    inserted,
				TableStats:  st.committed.StatsForCRC64(b.CRC64),
				RecordIndex: recorded,
			})
		}
	}
	return recorded
}

// destinationWithSASForDedupe returns an executable destination URL for later use as
// x-ms-copy-source. The destination client URL commonly already contains the SAS; merge any
// missing SAS fields without duplicating query parameters.
func destinationWithSASForDedupe(destURI, destinationSAS string) string {
	if destinationSAS == "" {
		return destURI
	}
	u, err := url.Parse(destURI)
	if err != nil {
		return destURI
	}
	sas := destinationSAS
	if sas[0] == '?' {
		sas = sas[1:]
	}
	if sas == "" {
		return destURI
	}

	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return destURI
	}
	sasQuery, err := url.ParseQuery(sas)
	if err != nil {
		return destURI
	}

	for key, values := range sasQuery {
		query[key] = append([]string(nil), values...)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

// logDedupeActSummary logs a running view of the job's cumulative Phase 2 savings. The terminal
// job summary is emitted separately when the job completes.
func logDedupeActSummary(jptm IJobPartTransferMgr, mode dedupeActMode) {
	st := dedupeStateForJob(jptm.Info().JobID)

	referencedBlocks := atomic.LoadInt64(&st.referencedBlocks)
	referencedBytes := atomic.LoadInt64(&st.referencedBytes)
	wouldRefBlocks := atomic.LoadInt64(&st.wouldReferenceBlocks)
	wouldRefBytes := atomic.LoadInt64(&st.wouldReferenceBytes)
	sourceBlocks := atomic.LoadInt64(&st.sourceStagedBlocks)
	sourceBytes := atomic.LoadInt64(&st.sourceStagedBytes)
	fallbacks := atomic.LoadInt64(&st.fallbackBlocks)

	switch mode {
	case dedupeActEnforce:
		// Referenced blocks were staged from the destination; source-staged blocks were read from the
		// source. Total = referenced + source; avoided source-read bytes = referencedBytes.
		totalStagedBytes := referencedBytes + sourceBytes
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-summary(enforce): dedupe-target blocks=%d bytes=%d; sourceURI blocks=%d bytes=%d; "+
				"WAN savings=%d bytes (%.1f%% of %d staged bytes); fallback blocks=%d",
			referencedBlocks, referencedBytes, sourceBlocks, sourceBytes,
			referencedBytes, dedupePercent(referencedBytes, totalStagedBytes), totalStagedBytes,
			fallbacks))
	case dedupeActShadow:
		// Every block is staged from the source in shadow mode, so sourceBytes is the total; the
		// would-reference blocks are the subset that enforce would have avoided.
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-summary(shadow): would-dedupe-target blocks=%d bytes=%d; sourceURI blocks=%d bytes=%d; "+
				"potential WAN savings=%d bytes (%.1f%% of %d staged bytes); all bytes currently staged from source",
			wouldRefBlocks, wouldRefBytes, sourceBlocks, sourceBytes,
			wouldRefBytes, dedupePercent(wouldRefBytes, sourceBytes), sourceBytes))
	}
}

func logDedupeJobSummary(jobID common.JobID, log func(common.LogLevel, string)) {
	st, ok := dedupeStateForJobIfExists(jobID)
	if !ok {
		return
	}
	mode := dedupeActMode(atomic.LoadInt32(&st.actMode))
	if mode == dedupeActOff {
		return
	}
	log(common.LogInfo, dedupeJobSummaryMessage(mode, st))
}

func finalizeDedupeJob(jobID common.JobID, log func(common.LogLevel, string)) {
	logDedupeJobSummary(jobID, log)
	clearDedupeStateForJob(jobID)
}

func dedupeJobSummaryMessage(mode dedupeActMode, st *dedupeJobState) string {
	referencedBlocks := atomic.LoadInt64(&st.referencedBlocks)
	referencedBytes := atomic.LoadInt64(&st.referencedBytes)
	wouldRefBlocks := atomic.LoadInt64(&st.wouldReferenceBlocks)
	wouldRefBytes := atomic.LoadInt64(&st.wouldReferenceBytes)
	sourceBlocks := atomic.LoadInt64(&st.sourceStagedBlocks)
	sourceBytes := atomic.LoadInt64(&st.sourceStagedBytes)
	fallbacks := atomic.LoadInt64(&st.fallbackBlocks)

	switch mode {
	case dedupeActShadow:
		totalStagedBytes := sourceBytes
		return fmt.Sprintf(
			"dedupe-job-summary(shadow): totalBlocks=%d wouldTargetURIBlocks=%d sourceURIBlocks=%d fallbackBlocks=%d potentialAvoidedSourceReadBytes=%d totalStagedBytes=%d potentialWanSavingsPercent=%.1f",
			sourceBlocks, wouldRefBlocks, sourceBlocks, fallbacks, wouldRefBytes, totalStagedBytes,
			dedupePercent(wouldRefBytes, totalStagedBytes))
	case dedupeActEnforce:
		totalStagedBytes := referencedBytes + sourceBytes
		return fmt.Sprintf(
			"dedupe-job-summary(enforce): totalBlocks=%d targetURIBlocks=%d sourceURIBlocks=%d fallbackBlocks=%d avoidedSourceReadBytes=%d totalStagedBytes=%d wanSavingsPercent=%.1f",
			referencedBlocks+sourceBlocks, referencedBlocks, sourceBlocks, fallbacks, referencedBytes, totalStagedBytes,
			dedupePercent(referencedBytes, totalStagedBytes))
	default:
		return "dedupe-job-summary(off): disabled"
	}
}

// blockHasHashes reports whether a planned block carried both content hashes in the extended
// GetBlockList response. Presence is tracked separately because an all-zero hash is valid.
func blockHasHashes(b PlannedBlock) bool {
	return b.HasHashes
}

// measureAndRecord performs the Phase 1 lookup-then-record over a set of planned blocks against
// the given table. For each block that carries hashes it (1) looks the content up, counting a
// would-be hit when identical content was already recorded, then (2) records the block keyed to
// where it is being migrated (targetURI). It returns the number of eligible (hashed) blocks, the
// number of would-be hits, and the total size of those hit blocks.
//
// It is deliberately free of any jptm/logging dependency so it can be unit tested directly.
//
// Note: between the Lookup and Insert of a given block another goroutine may insert identical
// content, so concurrent identical blocks can be under-counted as misses. That is acceptable for a
// measurement phase — the reported hit rate is conservative (never over-counted).
func measureAndRecord(table *common.DedupeHashTable, jobID common.JobID, targetURI string, blocks []PlannedBlock) (hashed, hits, dedupableBytes int64) {
	return measureAndRecordWithObserver(table, jobID, targetURI, blocks, nil)
}

func measureAndRecordWithObserver(table *common.DedupeHashTable, jobID common.JobID, targetURI string, blocks []PlannedBlock, observer func(dedupeTableRecordEvent)) (hashed, hits, dedupableBytes int64) {
	for _, b := range blocks {
		if !blockHasHashes(b) {
			continue
		}
		hashed++

		_, hit := table.Lookup(b.CRC64, b.SHA256)
		if hit {
			hits++
			dedupableBytes += b.Size
		}

		stored, inserted := table.Insert(common.BlockEntry{
			JobID:     jobID,
			CRC64:     b.CRC64,
			SHA256:    b.SHA256,
			TargetURI: targetURI,
			// The destination is a byte-identical copy of the source, so the block lives
			// at the same offset/length in the target blob as in the source.
			TargetOffset: b.Offset,
			TargetLength: b.Size,
			// ETag is populated in Phase 2, where recording happens after a successful
			// destination write; Phase 1 records pre-write, for measurement only.
		})
		if observer != nil {
			observer(dedupeTableRecordEvent{
				Block:       b,
				Stored:      stored,
				Inserted:    inserted,
				PreHit:      hit,
				TableStats:  table.StatsForCRC64(b.CRC64),
				RecordIndex: int(hashed),
			})
		}
	}
	return hashed, hits, dedupableBytes
}

// sanitizedDestForDedupe strips the query string from a destination URL before logging or
// comparing it. The committed table intentionally retains an authenticated URL for reuse.
func sanitizedDestForDedupe(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.RawQuery = ""
	return u.String()
}

// dedupePercent returns hits/total as a percentage, treating total==0 as 0%.
func dedupePercent(hits, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(hits) / float64(total)
}

// recordSourceGridForDedupe implements Phase 1 for a single transfer (blob). It records the
// source's committed blocks into the per-job table, accumulates the would-be-hit counters, and
// logs both this blob's contribution and the running job-wide hit rate. It changes no transfer
// behavior.
func recordSourceGridForDedupe(jptm IJobPartTransferMgr, plan *SourceGridPlan) {
	info := jptm.Info()
	st := dedupeStateForJob(info.JobID)
	targetURI := sanitizedDestForDedupe(info.Destination)

	hashed, hits, dedupableBytes := measureAndRecordWithObserver(st.table, info.JobID, targetURI, plan.Blocks, func(event dedupeTableRecordEvent) {
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf(
			"dedupe-table(observe): record=%d inserted=%t wouldBeHit=%t entries=%d buckets=%d bucketEntries=%d refCount=%d "+
				"crc64=%016x sha256=%x offset=%d size=%d target=%s",
			event.RecordIndex, event.Inserted, event.PreHit, event.TableStats.Entries, event.TableStats.Buckets,
			event.TableStats.BucketEntries, event.Stored.RefCount, event.Block.CRC64, event.Block.SHA256,
			event.Block.Offset, event.Block.Size, sanitizedDestForDedupe(event.Stored.TargetURI)))
	})
	if hashed == 0 {
		return // nothing eligible (service GetHash feature off, or include not honored)
	}

	totalHashed := atomic.AddInt64(&st.hashedBlocks, hashed)
	totalHits := atomic.AddInt64(&st.wouldBeHits, hits)
	totalBytes := atomic.AddInt64(&st.dedupableBytes, dedupableBytes)

	jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
		"dedupe-phase1: blob %q would-be-hits=%d/%d blocks; job cumulative would-be-hits=%d/%d blocks (%.1f%%), dedupable bytes=%d, table entries=%d",
		info.SrcFilePath, hits, hashed,
		totalHits, totalHashed, dedupePercent(totalHits, totalHashed),
		totalBytes, st.table.Len()))
}

// --- Phase 2 core: staging-time hit decision ---

// srcBlockKey identifies a source committed block by its position and length. A uniform AzCopy
// chunk can be matched to a source block (and so to its content hashes) only when the two share the
// same offset and size — until source-grid chunking (Phase 3) makes every chunk a source block, the
// index therefore only resolves chunks that already align with the source's committed boundaries.
type srcBlockKey struct {
	offset int64
	size   int64
}

// srcBlockHashes are the content hashes of a single source committed block.
type srcBlockHashes struct {
	crc64  uint64
	sha256 [32]byte
}

// buildSourceBlockHashIndex turns a source-grid plan into a lookup from a block's (offset,size) to
// its content hashes, including only blocks that actually carry hashes. The staging path consults it
// to discover whether the chunk it is about to send has a known hash to look up.
func buildSourceBlockHashIndex(plan *SourceGridPlan) map[srcBlockKey]srcBlockHashes {
	idx := make(map[srcBlockKey]srcBlockHashes, len(plan.Blocks))
	for _, b := range plan.Blocks {
		if !blockHasHashes(b) {
			continue
		}
		idx[srcBlockKey{offset: b.Offset, size: b.Size}] = srcBlockHashes{crc64: b.CRC64, sha256: b.SHA256}
	}
	return idx
}

// decideStaging is the core Phase 2 "act on a hit" decision for a single block about to be staged at
// [offset, offset+size) of the source. It returns the matching target entry with reference=true when
// (a) the chunk exactly matches a hashed source block, and (b) that content is already recorded in the
// destination hash index as committed destination content — meaning the block can be staged from the
// target blob (Put Block From URL over the target's sub-range) instead of re-read from the source.
// Otherwise it returns reference=false and the caller stages from the source as normal.
//
// currentTargetURI is the blob currently being written. A matching entry for that same target is not
// a cross-blob reuse candidate and must be ignored; same-blob hits are intentionally observe-only in
// this targetURI design because the current blob's destination ranges are not committed/readable yet.
func decideStaging(index map[srcBlockKey]srcBlockHashes, committed *common.DedupeHashTable, offset, size int64, currentTargetURI string) (target common.BlockEntry, reference bool) {
	h, ok := index[srcBlockKey{offset: offset, size: size}]
	if !ok {
		return common.BlockEntry{}, false // no known hash for this chunk (not aligned to a source block)
	}
	entry, hit := committed.Lookup(h.crc64, h.sha256)
	if !hit {
		return common.BlockEntry{}, false // identical content not yet migrated to the destination
	}
	if entry.TargetURI == "" || entry.TargetOffset < 0 || entry.ETag == "" || entry.TargetLength != size {
		return common.BlockEntry{}, false // never reuse an unversioned or differently-sized target range
	}
	if sameDedupeTarget(entry.TargetURI, currentTargetURI) {
		return common.BlockEntry{}, false // avoid unsafe same-blob/self reuse; wait for a committed target blob
	}
	return entry, true
}

func sameDedupeTarget(a, b string) bool {
	return sanitizedDestForDedupe(a) == sanitizedDestForDedupe(b)
}
