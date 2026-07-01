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

// This file implements "Phase 1" of the block-level dedupe prototype: "record + measure".
//
// Building on the read-only Phase 0 observer (sourceGridObserver.go), it records each
// source committed block that carries content hashes into a per-job DedupeHashTable, and
// measures the "would-be" dedupe hit rate: how many migrated blocks have content identical
// to a block already recorded earlier in the same job (within a blob or across blobs).
//
// Like Phase 0, it is gated on AZCOPY_DEDUPE_OBSERVE=true and NEVER changes transfer
// behavior. Its only job is to prove the real, end-to-end dedupe potential with hard numbers
// before any bytes are actually skipped (Phase 2) or the chunk grid is changed (Phase 3).

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

// dedupeStateForJob returns the dedupe state for a job, creating it on first use. The map is
// only ever populated when the prototype flag is on (observeSourceGrid is the sole caller), so
// no per-job memory is allocated in the default code path.
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

// clearDedupeStateForJob drops a job's dedupe table to release its memory. It is safe to call
// when no state exists. Wiring this to a job-teardown hook is a follow-up; for the opt-in
// prototype (one job per process invocation) the table is released at process exit.
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
func recordCommittedBlocks(jobID common.JobID, destURI string, etag azcore.ETag, plan *SourceGridPlan) (recorded int) {
	if plan == nil {
		return 0
	}
	st := dedupeStateForJob(jobID)
	target := sanitizedDestForDedupe(destURI)
	for _, b := range plan.Blocks {
		if !blockHasHashes(b) {
			continue
		}
		st.committed.Insert(common.BlockEntry{
			JobID:        jobID,
			CRC64:        b.CRC64,
			SHA256:       b.SHA256,
			TargetURI:    target,
			TargetOffset: b.Offset,
			TargetLength: b.Size,
			ETag:         etag,
		})
		recorded++
	}
	return recorded
}

// logDedupeActSummary logs the job's cumulative Phase 2 savings. It is emitted once per committed
// blob (from the sender Epilogue), so the final line for a job is the job total — mirroring the
// Phase 1 running-total pattern and avoiding the need for a job-teardown hook. It reports how many
// source reads dedupe avoided (enforce) or would avoid (shadow).
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
			"dedupe-summary(enforce): avoided %d source-read bytes across %d block(s) = %.1f%% of %d staged bytes; "+
				"staged %d block(s)/%d bytes from source; %d fallback(s)",
			referencedBytes, referencedBlocks, dedupePercent(referencedBytes, totalStagedBytes), totalStagedBytes,
			sourceBlocks, sourceBytes, fallbacks))
	case dedupeActShadow:
		// Every block is staged from the source in shadow mode, so sourceBytes is the total; the
		// would-reference blocks are the subset that enforce would have avoided.
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-summary(shadow): %d block(s)/%d bytes WOULD be avoided under enforce = %.1f%% of %d staged bytes "+
				"(all currently staged from source)",
			wouldRefBlocks, wouldRefBytes, dedupePercent(wouldRefBytes, sourceBytes), sourceBytes))
	}
}

// blockHasHashes reports whether a planned block carries content hashes from the extended
// GetBlockList response. When the service GetHash feature is off (or include was not honored),
// both hashes are left zero and the block is not eligible for dedupe measurement.
func blockHasHashes(b PlannedBlock) bool {
	return b.CRC64 != 0 || b.SHA256 != ([32]byte{})
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
	for _, b := range blocks {
		if !blockHasHashes(b) {
			continue
		}
		hashed++

		if _, hit := table.Lookup(b.CRC64, b.SHA256); hit {
			hits++
			dedupableBytes += b.Size
		}

		table.Insert(common.BlockEntry{
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
	}
	return hashed, hits, dedupableBytes
}

// sanitizedDestForDedupe returns the destination blob URL with any query string (e.g. a SAS
// token) stripped, so credentials are never stored in the table. If the URL cannot be parsed it
// is returned unchanged (the table is in-memory and prototype-only).
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

	hashed, hits, dedupableBytes := measureAndRecord(st.table, info.JobID, targetURI, plan.Blocks)
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

// --- Phase 2 core: staging-time hit decision (pure; not yet wired into the transfer path) ---

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
// (a) the chunk exactly matches a hashed source block, and (b) that content is already recorded as
// committed at the destination — meaning the block can be staged from the target blob (Put Block
// From URL over the target's sub-range) instead of re-read from the source. Otherwise it returns
// reference=false and the caller stages from the source as normal.
//
// It is pure (no I/O, no jptm) so the decision can be unit tested exhaustively before being wired
// into generatePutBlockFromURL.
func decideStaging(index map[srcBlockKey]srcBlockHashes, committed *common.DedupeHashTable, offset, size int64) (target common.BlockEntry, reference bool) {
	h, ok := index[srcBlockKey{offset: offset, size: size}]
	if !ok {
		return common.BlockEntry{}, false // no known hash for this chunk (not aligned to a source block)
	}
	entry, hit := committed.Lookup(h.crc64, h.sha256)
	if !hit {
		return common.BlockEntry{}, false // identical content not yet migrated to the destination
	}
	return entry, true
}
