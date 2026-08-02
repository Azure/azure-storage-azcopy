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

package ste

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const dedupeHashResolutionTimeout = 30 * time.Second

// urlToBlockBlobCopier extends blockBlobSenderBase parent to include URL-specific functionality
type urlToBlockBlobCopier struct {
	blockBlobSenderBase

	srcURL               string
	addFileRequestIntent bool // Necessary for FileBlob Oauth copies
}

func newURLToBlockBlobCopier(jptm IJobPartTransferMgr, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	// Get blob tier, by default set none.
	var destBlobTier *blob.AccessTier
	// If the source is block blob, preserve source's blob tier.
	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == blob.BlobTypeBlockBlob {
			destBlobTier = blobSrcInfoProvider.BlobTier()
		}
	}

	senderBase, err := newBlockBlobSenderBase(jptm, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	// Check if source is Files
	intentBool := false
	if _, ok := srcInfoProvider.(*fileSourceInfoProvider); ok {
		sUrl, _ := file.ParseURL(srcURL)
		intentBool = sUrl.SAS.Signature() == "" // No SAS means using OAuth
	}
	copier := &urlToBlockBlobCopier{
		blockBlobSenderBase:  *senderBase,
		srcURL:              srcURL,
		addFileRequestIntent: intentBool,
	}

	// Block-level dedupe prototype (AZCOPY_DEDUPE_ACT): for an eligible block-blob -> block-blob S2S
	// copy, chunk on the source's committed block boundaries and arm the per-block hit decision used in
	// generatePutBlockFromURL. A no-op (uniform grid) when the flag is unset or the source is not an
	// eligible block blob.
	configureBlockBlobDedupe(jptm, copier, srcInfoProvider)

	return copier, nil
}

// configureBlockBlobDedupe arms source-grid chunking + the dedupe hit decision on a block-blob S2S
// copier when AZCOPY_DEDUPE_ACT is set and the source is a block blob with a committed block list. It
// overrides the sender's chunk count so every chunk lines up with a source block. Any problem
// leaves dedupe off and the copy proceeds on the uniform grid, so it can never break a transfer.
func configureBlockBlobDedupe(jptm IJobPartTransferMgr, c *urlToBlockBlobCopier, srcInfoProvider IRemoteSourceInfoProvider) {
	mode := dedupeActModeFromEnv()
	if mode == dedupeActOff {
		return
	}
	info := jptm.Info()
	setDedupeActModeForJob(info.JobID, mode)
	st := dedupeStateForJob(info.JobID)
	emitDedupeProgress(jptm, dedupeProgressMessage(
		"preflight_start",
		mode,
		info,
		"stage=\"source_grid\"",
		st.progressSnapshot()))

	preflightFailed := func(stage, reason string) {
		emitDedupeProgress(jptm, dedupeProgressMessage(
			"preflight_failed",
			mode,
			info,
			fmt.Sprintf(
				"stage=%q reason=%q action=%q",
				stage,
				reason,
				"uniform_grid",
			),
			st.progressSnapshot()))
	}

	blobSrc, ok := srcInfoProvider.(IBlobSourceInfoProvider)
	if !ok || blobSrc.BlobType() != blob.BlobTypeBlockBlob {
		preflightFailed("source_type", "source is not a block blob")
		return
	}
	_, destinationSAS := jptm.SAS()
	if !dedupeActDestinationReady(mode, destinationSAS) {
		preflightFailed("destination_auth", "destination SAS is unavailable")
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug,
			"dedupe-act(enforce): destination SAS is unavailable, using uniform grid")
		return
	}

	resp, err := getSourceBlockList(jptm)
	if err != nil {
		preflightFailed("source_block_list", err.Error())
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "dedupe-act: GetBlockList failed, using uniform grid: "+err.Error())
		return
	}
	emitHashCPUTime(jptm, mode, resp)

	if len(resp.CommittedBlocks) == 0 {
		preflightFailed("source_block_list", "source has no committed named blocks")
		return // single-PutBlob or empty source: no committed blocks to align to
	}
	if resp.ETag == nil || *resp.ETag == "" {
		preflightFailed("source_block_list", "source block list response has no ETag")
		return
	}
	plan, err := buildSourceGridPlan(rawCommittedBlocksFromResponse(resp))
	if err != nil {
		preflightFailed("source_block_list", err.Error())
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "dedupe-act: GetBlockList failed, using uniform grid: "+err.Error())
		return
	}
	if plan.TotalSize != info.SourceSize {
		preflightFailed(
			"source_grid",
			fmt.Sprintf("planned bytes %d do not match source bytes %d", plan.TotalSize, info.SourceSize),
		)
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf(
			"dedupe-act: source-grid total %d != source size %d, using uniform grid", plan.TotalSize, info.SourceSize))
		return
	}
	if len(plan.Blocks) > common.MaxNumberOfBlocksPerBlob {
		preflightFailed(
			"source_grid",
			fmt.Sprintf("source block count %d exceeds maximum %d", len(plan.Blocks), common.MaxNumberOfBlocksPerBlob),
		)
		return
	}

	crcBlocks := countSourceGridCRCBlocks(plan)
	if crcBlocks == 0 {
		preflightFailed("source_hashes", "source block list contains no valid CRC64 values")
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug,
			"dedupe-act: source block list contained no valid CRC64 values, using uniform grid")
		return
	}
	discoveredBlocks, discoveredBytes := st.addCRCDiscovery(plan)
	emitDedupeProgress(jptm, dedupeProgressMessage(
		"crc_only_discovery",
		mode,
		info,
		fmt.Sprintf(
			"sourceBlocks=%d crcBlocks=%d crcBytes=%d sourceETag=%q",
			len(plan.Blocks),
			discoveredBlocks,
			discoveredBytes,
			*resp.ETag,
		),
		st.progressSnapshot(),
	))
	emitDedupeProgress(jptm, dedupeProgressMessage(
		"chunk_plan_validated",
		mode,
		info,
		fmt.Sprintf(
			"sourceBlocks=%d crcBlocks=%d plannedChunks=%d plannedBytes=%d sourceBytes=%d",
			len(plan.Blocks),
			crcBlocks,
			len(plan.Blocks),
			plan.TotalSize,
			info.SourceSize,
		),
		st.progressSnapshot()))

	// One chunk per source committed block.
	c.numChunks = uint32(len(plan.Blocks))
	c.blockIDs = make([]string, c.numChunks)
	c.dedupeMode = mode
	c.dedupePlan = plan
	c.dedupeSourceCache = cloneDedupeSourceHashCache(dedupeSourceHashCache{})
	c.dedupeETag = *resp.ETag
	st.addFileStarted()

	jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
		"dedupe-act(%s): source-grid chunking armed: %d block(s), %d with CRC64, totalSize=%d",
		mode, len(plan.Blocks), crcBlocks, plan.TotalSize))
	emitDedupeProgress(jptm, dedupeProgressMessage(
		"file_start",
		mode,
		jptm.Info(),
		fmt.Sprintf("source=%q sourceBlocks=%d crcBlocks=%d fileBytes=%d",
			sanitizedDestForDedupe(jptm.Info().Source), len(plan.Blocks), crcBlocks, plan.TotalSize),
		st.progressSnapshot()))
}

func countSourceGridCRCBlocks(plan *SourceGridPlan) int {
	if plan == nil {
		return 0
	}
	count := 0
	for _, block := range plan.Blocks {
		if blockHasCRC64(block) {
			count++
		}
	}
	return count
}

// Returns a chunk-func for blob copies
func (c *urlToBlockBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	/*
	 * There was a optimization here to use PutBlob for zero-byte blobs instead of PutBlobFromURL.
	 * It was removed because of these reasons:
	 * 1. Both apis are different in some aspects. For put blob service verifies the content md5.
	 * This is not required if check-md5 is false. Using same calls helps us be consistent.
	 * 2. If the source only has list (and no read) permissions, we will still put the blob here
	 * While it is arguable that content can be inferred from size, it is better to fail transfer
	 * for blobs of all sizes.
	 */
	// Small blobs from all sources will be copied over to destination using PutBlobFromUrl
	if c.dedupeMode == dedupeActOff && c.NumChunks() == 1 && adjustedChunkSize <= int64(common.MaxPutBlobSize) {
		/*
		 * siminsavani: FYI: For GCP, if the blob is the entirety of the file, GCP still returns
		 * invalid error from service due to PutBlockFromUrl.
		 */
		setPutListNeed(&c.atomicPutListIndicator, putListNotNeeded)
		return c.generateStartPutBlobFromURL(id, blockIndex, adjustedChunkSize)

	}
	setPutListNeed(&c.atomicPutListIndicator, putListNeeded)
	return c.generatePutBlockFromURL(id, blockIndex, adjustedChunkSize)
}

// generatePutBlockFromURL generates a func to copy the block of src data from given startIndex till the given chunkSize.
func (c *urlToBlockBlobCopier) generatePutBlockFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		// step 1: generate block ID
		encodedBlockID := c.generateEncodedBlockID(blockIndex)

		// step 2: save the block ID into the list of block IDs
		c.setBlockID(blockIndex, encodedBlockID)

		// In dedupe mode the chunk grid is content-defined, so the resume "already transferred" map
		// (keyed by the uniform-grid block names) does not apply; always (re)stage the block.
		if c.dedupeMode == dedupeActOff && c.ChunkAlreadyTransferred(blockIndex) {
			c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf("Skipping chunk %d as it was already transferred.", blockIndex))
			atomic.AddInt32(&c.atomicChunksWritten, 1)
			return
		}

		// step 3: put block to remote
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		// Block-level dedupe prototype: if this chunk's content already exists at the destination, either
		// log it (shadow) or stage it from there instead of the source (enforce, with fallback to source).
		if c.dedupeMode != dedupeActOff && c.tryDedupeStage(id, blockIndex, encodedBlockID, adjustedChunkSize) {
			atomic.AddInt32(&c.atomicChunksWritten, 1)
			return
		}

		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}

		options := c.sourceStageBlockOptions(
			id.OffsetInFile(),
			adjustedChunkSize,
			token,
		)

		// Informs SDK to add xms-file-request-intent header
		if c.addFileRequestIntent {
			fileIntent := blob.FileRequestIntentTypeBackup
			options.FileRequestIntent = &fileIntent
		}

		_, err = c.destBlockBlobClient.StageBlockFromURL(c.jptm.Context(), encodedBlockID, c.srcURL, options)

		if err != nil {
			c.jptm.FailActiveSend("Staging block from URL", err)
			return
		}

		if c.dedupeMode != dedupeActOff {
			st := dedupeStateForJob(c.jptm.Info().JobID)
			st.addSourceStaged(adjustedChunkSize)
			emitDedupeProgress(c.jptm, dedupeProgressMessage(
				"source_block_transferred",
				c.dedupeMode,
				c.jptm.Info(),
				fmt.Sprintf("blockIndex=%d sourceOffset=%d blockBytes=%d",
					blockIndex, id.OffsetInFile(), adjustedChunkSize),
				st.progressSnapshot()))
		}

		atomic.AddInt32(&c.atomicChunksWritten, 1)
	})
}

func (c *urlToBlockBlobCopier) sourceStageBlockOptions(
	offset int64,
	count int64,
	token *string,
) *blockblob.StageBlockFromURLOptions {
	options := &blockblob.StageBlockFromURLOptions{
		Range:                   blob.HTTPRange{Offset: offset, Count: count},
		CPKInfo:                 c.jptm.CpkInfo(),
		CPKScopeInfo:            c.jptm.CpkScopeInfo(),
		CopySourceAuthorization: token,
	}
	if c.dedupeMode != dedupeActOff && c.dedupeETag != "" {
		etag := c.dedupeETag
		options.SourceModifiedAccessConditions = &blob.SourceModifiedAccessConditions{
			SourceIfMatch: &etag,
		}
	}
	return options
}

// tryDedupeStage applies the block-level dedupe decision for one chunk. It returns true only when the
// block was fully handled by staging it from an already-migrated destination block (enforce mode on a
// successful reference). In shadow mode, or on any miss/failure, it returns false so the caller stages
// the block from the source as usual.
func (c *urlToBlockBlobCopier) tryDedupeStage(id common.ChunkID, blockIndex int32, encodedBlockID string, size int64) (handled bool) {
	st := dedupeStateForJob(c.jptm.Info().JobID)
	resolutionReady := c.ensureDedupeHashesResolved(st)
	if !resolutionReady && c.dedupeMode == dedupeActShadow {
		done := c.dedupeResolutionDone()
		if done != nil {
			select {
			case <-done:
				resolutionReady = true
			case <-c.jptm.Context().Done():
				return false
			}
		}
	}
	index, resolveErr := c.dedupeResolutionSnapshot()
	if _, resolved := index[srcBlockKey{offset: id.OffsetInFile(), size: size}]; !resolved {
		if !resolutionReady {
			c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug,
				"dedupe-act: GetBlobHash resolution is in progress; staging this block from source")
			return false
		}
		if resolveErr == nil {
			return false
		}
		if isDedupePreconditionFailure(resolveErr) {
			c.jptm.LogAtLevelForCurrentTransfer(common.LogInfo,
				"dedupe-act: source changed before GetBlobHash; staging from source")
		} else {
			c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug,
				"dedupe-act: GetBlobHash did not resolve this source range; staging from source: "+resolveErr.Error())
		}
		return false
	}
	targets := matchingDedupeTargets(
		index,
		st.committed,
		id.OffsetInFile(),
		size,
		c.jptm.Info().Destination,
	)
	if len(targets) == 0 {
		if hasDedupeSHAMismatch(
			index,
			st.committed,
			id.OffsetInFile(),
			size,
			c.jptm.Info().Destination,
		) {
			st.addSHAMismatch()
			emitDedupeProgress(c.jptm, dedupeProgressMessage(
				"sha_mismatch",
				c.dedupeMode,
				c.jptm.Info(),
				fmt.Sprintf("blockIndex=%d sourceOffset=%d blockBytes=%d",
					blockIndex, id.OffsetInFile(), size),
				st.progressSnapshot(),
			))
		}
		c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf(
			"dedupe-act(%s): no committed destination hash hit for offset=%d size=%d; staging from sourceURI",
			c.dedupeMode, id.OffsetInFile(), size))
		return false
	}
	target := targets[0]
	st.addSHAConfirmed()
	emitDedupeProgress(c.jptm, dedupeProgressMessage(
		"sha_confirmed",
		c.dedupeMode,
		c.jptm.Info(),
		fmt.Sprintf(
			"blockIndex=%d sourceOffset=%d blockBytes=%d targetURI=%q targetOffset=%d targetLength=%d",
			blockIndex,
			id.OffsetInFile(),
			size,
			sanitizedDestForDedupe(target.TargetURI),
			target.TargetOffset,
			target.TargetLength,
		),
		st.progressSnapshot(),
	))

	if c.dedupeMode == dedupeActShadow {
		st.addWouldReference(size)
		c.jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-act(shadow): block at offset=%d size=%d WOULD be referenced from %s [%d,%d) (staging from source instead)",
			id.OffsetInFile(), size, sanitizedDestForDedupe(target.TargetURI), target.TargetOffset, target.TargetLength))
		emitDedupeProgress(c.jptm, fmt.Sprintf(
			"%s event=target_reuse_candidate mode=%s jobId=%s file=%q destination=%q "+
				"blockIndex=%d sourceOffset=%d blockBytes=%d targetURI=%q targetOffset=%d targetLength=%d",
			dedupeProgressPrefix, c.dedupeMode, c.jptm.Info().JobID.String(), c.jptm.Info().DstFilePath,
			sanitizedDestForDedupe(c.jptm.Info().Destination), blockIndex, id.OffsetInFile(), size,
			sanitizedDestForDedupe(target.TargetURI), target.TargetOffset, target.TargetLength))
		return false
	}

	// Enforce: try every safe matching occurrence before falling back to the source.
	selected, attempts, staged := stageDedupeTargetCandidates(
		targets,
		func(candidate common.BlockEntry) error {
			c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf(
				"dedupe-act(enforce): committed destination hash hit for offset=%d size=%d; staging from targetURI=%s [%d,%d)",
				id.OffsetInFile(), size, sanitizedDestForDedupe(candidate.TargetURI),
				candidate.TargetOffset, candidate.TargetOffset+candidate.TargetLength))
			return c.stageBlockFromTarget(encodedBlockID, candidate)
		},
	)
	for targetIndex, attempt := range attempts {
		candidate := attempt.target
		if attempt.err != nil {
			st.committed.RemoveTargetOccurrence(
				candidate.TargetURI,
				candidate.TargetOffset,
				candidate.TargetLength,
				candidate.ETag,
			)
			c.jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
				"dedupe-act(enforce): reference attempt %d/%d to %s failed (%v)",
				targetIndex+1, len(targets), sanitizedDestForDedupe(candidate.TargetURI), attempt.err))
			emitDedupeProgress(c.jptm, dedupeProgressMessage(
				"target_reuse_attempt_failed",
				c.dedupeMode,
				c.jptm.Info(),
				fmt.Sprintf(
					"blockIndex=%d sourceOffset=%d blockBytes=%d targetIndex=%d targetCount=%d "+
						"targetURI=%q targetOffset=%d targetLength=%d",
					blockIndex, id.OffsetInFile(), size, targetIndex, len(targets),
					sanitizedDestForDedupe(candidate.TargetURI),
					candidate.TargetOffset, candidate.TargetLength),
				st.progressSnapshot(),
			))
			if isDedupePreconditionFailure(attempt.err) {
				removed := st.committed.RemoveTargetEpoch(candidate.TargetURI, candidate.ETag)
				st.addHashResolution(dedupeHashResolutionStats{targetEpochInvalidations: 1})
				emitDedupeProgress(c.jptm, dedupeProgressMessage(
					"epoch_invalidated_412",
					c.dedupeMode,
					c.jptm.Info(),
					fmt.Sprintf(
						"role=%q targetURI=%q removedEntries=%d",
						"target_stage",
						sanitizedDestForDedupe(candidate.TargetURI),
						removed,
					),
					st.progressSnapshot(),
				))
			}
		}
	}
	if staged {
		st.addReferenced(size)
		c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf(
			"dedupe-act(enforce): block at offset=%d size=%d staged from %s [%d,%d)",
			id.OffsetInFile(), size, sanitizedDestForDedupe(selected.TargetURI),
			selected.TargetOffset, selected.TargetLength))
		emitDedupeProgress(c.jptm, dedupeProgressMessage(
			"target_reuse",
			c.dedupeMode,
			c.jptm.Info(),
			fmt.Sprintf("blockIndex=%d sourceOffset=%d blockBytes=%d targetURI=%q targetOffset=%d targetLength=%d",
				blockIndex, id.OffsetInFile(), size, sanitizedDestForDedupe(selected.TargetURI),
				selected.TargetOffset, selected.TargetLength),
			st.progressSnapshot()))
		return true
	}

	st.addFallback()
	c.jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
		"dedupe-act(enforce): all %d target reference attempts failed; falling back to staging from source",
		len(targets)))
	emitDedupeProgress(c.jptm, dedupeProgressMessage(
		"target_reuse_fallback",
		c.dedupeMode,
		c.jptm.Info(),
		fmt.Sprintf("blockIndex=%d sourceOffset=%d blockBytes=%d attemptedTargets=%d",
			blockIndex, id.OffsetInFile(), size, len(targets)),
		st.progressSnapshot()))
	return false
}

func (c *urlToBlockBlobCopier) ensureDedupeHashesResolved(st *dedupeJobState) bool {
	if st == nil {
		return true
	}

	generation := st.committedGenerationValue()
	c.dedupeResolveMu.Lock()
	if c.dedupeResolveInProgress {
		c.dedupeResolveMu.Unlock()
		return false
	}
	if c.dedupeResolveAttempted && c.dedupeResolvedGeneration >= generation {
		c.dedupeResolveMu.Unlock()
		return true
	}
	if c.dedupeSourceCache.invalid {
		c.dedupeResolveAttempted = true
		c.dedupeResolvedGeneration = generation
		c.dedupeResolveMu.Unlock()
		return true
	}

	c.dedupeResolveInProgress = true
	c.dedupeResolveDone = make(chan struct{})
	cache := cloneDedupeSourceHashCache(c.dedupeSourceCache)
	hasher := c.dedupeHasher
	c.dedupeResolveMu.Unlock()

	var stats dedupeHashResolutionStats
	var err error
	if hasher == nil {
		hasher, err = newSDKDedupeRangeHasher(c.jptm, func(role string, response blockblob.GetBlobHashResponse) {
			emitGetBlobHashCPUTime(c.jptm, c.dedupeMode, role, response)
		})
	}
	if err == nil {
		ctx, cancel := context.WithTimeout(c.jptm.Context(), dedupeHashResolutionTimeout)
		cache, stats, err = resolveDedupeCandidateHashesIncremental(
			ctx,
			st,
			c.dedupePlan,
			c.dedupeETag,
			c.jptm.Info().Destination,
			hasher,
			cache,
		)
		cancel()
		applyResolvedSourceHashes(c.dedupePlan, cache.hashes)
	}

	c.dedupeResolveMu.Lock()
	c.dedupeSourceCache = cache
	c.dedupeResolveStats = stats
	c.dedupeResolveErr = err
	c.dedupeResolveAttempted = true
	c.dedupeResolvedGeneration = generation
	c.dedupeResolveInProgress = false
	close(c.dedupeResolveDone)
	c.dedupeResolveMu.Unlock()

	emitDedupeHashResolutionProgress(c.jptm, c.dedupeMode, stats, err)
	return true
}

func (c *urlToBlockBlobCopier) dedupeResolutionSnapshot() (map[srcBlockKey]srcBlockHashes, error) {
	c.dedupeResolveMu.Lock()
	defer c.dedupeResolveMu.Unlock()
	return c.dedupeSourceCache.hashes, c.dedupeResolveErr
}

func (c *urlToBlockBlobCopier) dedupeResolutionDone() <-chan struct{} {
	c.dedupeResolveMu.Lock()
	defer c.dedupeResolveMu.Unlock()
	return c.dedupeResolveDone
}

type dedupeTargetStageAttempt struct {
	target common.BlockEntry
	err    error
}

func stageDedupeTargetCandidates(
	targets []common.BlockEntry,
	stage func(common.BlockEntry) error,
) (selected common.BlockEntry, attempts []dedupeTargetStageAttempt, staged bool) {
	for _, target := range targets {
		err := stage(target)
		attempts = append(attempts, dedupeTargetStageAttempt{target: target, err: err})
		if err == nil {
			return target, attempts, true
		}
	}
	return common.BlockEntry{}, attempts, false
}

// stageBlockFromTarget stages a block by copying a sub-range of an already-migrated destination blob
// (Put Block From URL) instead of re-reading the bytes from the source. The recorded ETag is sent as an
// If-Match on the copy source so a changed/replaced target fails fast (and the caller falls back to the
// source). The chunk has already been paced by the caller, so it is not re-paced here.
func (c *urlToBlockBlobCopier) stageBlockFromTarget(encodedBlockID string, target common.BlockEntry) error {
	if target.ETag == "" {
		return fmt.Errorf("dedupe target is missing an ETag")
	}
	if target.TargetLength <= 0 {
		return fmt.Errorf("dedupe target has invalid length %d", target.TargetLength)
	}
	if target.TargetOffset < 0 {
		return fmt.Errorf("dedupe target has invalid offset %d", target.TargetOffset)
	}
	etag := target.ETag
	options := &blockblob.StageBlockFromURLOptions{
		Range:        blob.HTTPRange{Offset: target.TargetOffset, Count: target.TargetLength},
		CPKInfo:      c.jptm.CpkInfo(),
		CPKScopeInfo: c.jptm.CpkScopeInfo(),
		SourceModifiedAccessConditions: &blob.SourceModifiedAccessConditions{
			SourceIfMatch: &etag,
		},
	}
	_, err := c.destBlockBlobClient.StageBlockFromURL(c.jptm.Context(), encodedBlockID, target.TargetURI, options)
	return err
}

func (c *urlToBlockBlobCopier) generateStartPutBlobFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {

		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		// Create blob and finish.
		if !ValidateTier(c.jptm, c.destBlobTier, c.destBlockBlobClient, c.jptm.Context(), false) {
			c.destBlobTier = nil
		}

		blobTags := c.blobTagsToApply
		setTags := separateSetTagsRequired(blobTags)
		if setTags || len(blobTags) == 0 {
			blobTags = nil
		}

		// TODO: Remove this snippet once service starts supporting CPK with blob tier
		destBlobTier := c.destBlobTier
		if c.jptm.IsSourceEncrypted() {
			destBlobTier = nil
		}

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}

		options := &blockblob.UploadBlobFromURLOptions{
			HTTPHeaders:             &c.headersToApply,
			Metadata:                c.metadataToApply,
			Tier:                    destBlobTier,
			Tags:                    blobTags,
			CPKInfo:                 c.jptm.CpkInfo(),
			CPKScopeInfo:            c.jptm.CpkScopeInfo(),
			CopySourceAuthorization: token,
		}
		// Informs SDK to add xms-file-request-intent header
		if c.addFileRequestIntent {
			fileRequestIntent := blob.FileRequestIntentTypeBackup
			options.FileRequestIntent = &fileRequestIntent
		}
		_, err = c.destBlockBlobClient.UploadBlobFromURL(c.jptm.Context(), c.srcURL, options)

		if err != nil {
			c.jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Committing block list (with tags)", "Committing block list"), err)
			return
		}

		atomic.AddInt32(&c.atomicChunksWritten, 1)

		if setTags {
			if _, err := c.destBlockBlobClient.SetTags(c.jptm.Context(), c.blobTagsToApply, nil); err != nil {
				c.jptm.FailActiveSend("Set blob tags", err)
			}
		}
	})
}
