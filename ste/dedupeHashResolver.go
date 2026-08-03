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
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const (
	dedupeGetBlobHashMaxRanges      = 256
	dedupeGetBlobHashMaxHeaderBytes = 8192
	dedupeGetBlobHashMaxBytes       = int64(4000 * 1024 * 1024)

	// GetBlobHash hashes ranges sequentially on the service. The 10 GiB workload
	// timed out with requests starting at 15 ranges, so cap requests at roughly
	// half that failure floor while retaining protocol limits as validation.
	dedupeGetBlobHashOperationalMaxRanges = 8
)

type dedupeRangeHasher interface {
	HashSource(context.Context, azcore.ETag, []blockblob.BlobHashRange) (dedupeRangeHashResult, error)
	HashTarget(context.Context, string, azcore.ETag, []blockblob.BlobHashRange) (dedupeRangeHashResult, error)
}

type dedupeRangeHashResult struct {
	response        blockblob.GetBlobHashResponse
	attemptedRanges []blockblob.BlobHashRange
	batches         int
}

type sdkDedupeRangeHasher struct {
	jptm       IJobPartTransferMgr
	source     *blockblob.Client
	onResponse func(string, blockblob.GetBlobHashResponse)
}

func newSDKDedupeRangeHasher(
	jptm IJobPartTransferMgr,
	onResponse func(string, blockblob.GetBlobHashResponse),
) (*sdkDedupeRangeHasher, error) {
	info := jptm.Info()
	sourceService, err := jptm.SrcServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}
	source := sourceService.NewContainerClient(info.SrcContainer).NewBlockBlobClient(info.SrcFilePath)
	source, err = sourceBlockBlobClientForTransfer(source, info)
	if err != nil {
		return nil, err
	}
	return &sdkDedupeRangeHasher{
		jptm:       jptm,
		source:     source,
		onResponse: onResponse,
	}, nil
}

func (h *sdkDedupeRangeHasher) HashSource(
	ctx context.Context,
	etag azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	return h.hashRanges(ctx, "source", h.source, etag, ranges, nil)
}

func (h *sdkDedupeRangeHasher) HashTarget(
	ctx context.Context,
	targetURI string,
	etag azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (dedupeRangeHashResult, error) {
	target, err := h.targetClient(targetURI)
	if err != nil {
		return dedupeRangeHashResult{}, err
	}
	return h.hashRanges(ctx, "target", target, etag, ranges, h.jptm.CpkInfo())
}

func (h *sdkDedupeRangeHasher) targetClient(targetURI string) (*blockblob.Client, error) {
	parts, err := blob.ParseURL(targetURI)
	if err != nil {
		return nil, fmt.Errorf("parse dedupe target URL: %w", err)
	}
	if parts.ContainerName == "" || parts.BlobName == "" {
		return nil, fmt.Errorf("dedupe target URL does not identify a blob")
	}

	destinationService, err := h.jptm.DstServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}
	return destinationService.NewContainerClient(parts.ContainerName).NewBlockBlobClient(parts.BlobName), nil
}

func (h *sdkDedupeRangeHasher) hashRanges(
	ctx context.Context,
	role string,
	client *blockblob.Client,
	etag azcore.ETag,
	ranges []blockblob.BlobHashRange,
	cpkInfo *blob.CPKInfo,
) (dedupeRangeHashResult, error) {
	batches, err := batchDedupeBlobHashRanges(ranges)
	if err != nil {
		return dedupeRangeHashResult{}, err
	}

	result := dedupeRangeHashResult{
		response: blockblob.GetBlobHashResponse{
			RangeHashes: make([]blockblob.BlobHashResult, 0, len(ranges)),
		},
		attemptedRanges: make([]blockblob.BlobHashRange, 0, len(ranges)),
	}
	for _, batch := range batches {
		result.attemptedRanges = append(result.attemptedRanges, batch...)
		result.batches++
		response, err := client.GetBlobHash(ctx, batch, &blockblob.GetBlobHashOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &etag},
			},
			CPKInfo: cpkInfo,
		})
		if err != nil {
			return result, err
		}
		if h.onResponse != nil {
			h.onResponse(role, response)
		}
		result.response.RangeHashes = append(result.response.RangeHashes, response.RangeHashes...)
		result.response.ETag = response.ETag
		result.response.LastModified = response.LastModified
		result.response.BlobContentLength = response.BlobContentLength
		result.response.HashAlgorithm = response.HashAlgorithm
		result.response.RequestID = response.RequestID
		result.response.ClientRequestID = response.ClientRequestID
		result.response.Version = response.Version
	}
	return result, nil
}

func batchDedupeBlobHashRanges(ranges []blockblob.BlobHashRange) ([][]blockblob.BlobHashRange, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	var batches [][]blockblob.BlobHashRange
	var batch []blockblob.BlobHashRange
	headerBytes := len("bytes=")
	var batchBytes int64
	var previousEnd int64

	flush := func() {
		if len(batch) == 0 {
			return
		}
		batches = append(batches, batch)
		batch = nil
		headerBytes = len("bytes=")
		batchBytes = 0
	}

	for i, rnge := range ranges {
		if rnge.Offset < 0 || rnge.Count <= 0 ||
			rnge.Count > dedupeGetBlobHashMaxBytes ||
			rnge.Offset > math.MaxInt64-(rnge.Count-1) {
			return nil, fmt.Errorf("invalid GetBlobHash range %d: offset=%d count=%d", i, rnge.Offset, rnge.Count)
		}
		end := rnge.Offset + rnge.Count - 1
		if i > 0 && rnge.Offset <= previousEnd {
			return nil, fmt.Errorf(
				"GetBlobHash range %d is unsorted or overlaps the preceding range",
				i,
			)
		}
		serializedBytes := len(strconv.FormatInt(rnge.Offset, 10)) +
			1 +
			len(strconv.FormatInt(end, 10))
		if len(batch) > 0 {
			serializedBytes++
		}

		if len(batch) == dedupeGetBlobHashOperationalMaxRanges ||
			headerBytes+serializedBytes > dedupeGetBlobHashMaxHeaderBytes ||
			batchBytes > dedupeGetBlobHashMaxBytes-rnge.Count {
			flush()
			serializedBytes = len(strconv.FormatInt(rnge.Offset, 10)) +
				1 +
				len(strconv.FormatInt(end, 10))
		}

		batch = append(batch, rnge)
		headerBytes += serializedBytes
		batchBytes += rnge.Count
		previousEnd = end
	}
	flush()
	return batches, nil
}

func attemptedDedupeHashRanges(
	attempted []blockblob.BlobHashRange,
	requested map[srcBlockKey]uint64,
) map[srcBlockKey]uint64 {
	matched := make(map[srcBlockKey]uint64, len(attempted))
	for _, rnge := range attempted {
		key := srcBlockKey{offset: rnge.Offset, size: rnge.Count}
		if crc64, ok := requested[key]; ok {
			matched[key] = crc64
		}
	}
	return matched
}

type dedupeHashResolutionStats struct {
	candidateBlocks          int
	candidateOccurrences     int
	newCandidateBlocks       int
	newCandidateOccurrences  int
	sourceHashRanges         int
	sourceHashBatches        int
	targetHashRanges         int
	targetHashBatches        int
	targetSHAIndexHits       int
	targetSHAIndexMisses     int
	targetHashFailures       int
	sourceEpochInvalidations int
	targetEpochInvalidations int
}

type dedupeTargetEpoch struct {
	targetURI string
	etag      azcore.ETag
}

type dedupeCandidateOccurrenceKey struct {
	source       srcBlockKey
	crc64        uint64
	targetURI    string
	targetOffset int64
	targetLength int64
	etag         azcore.ETag
}

func newDedupeCandidateOccurrenceKey(
	source srcBlockKey,
	candidate common.BlockEntry,
) dedupeCandidateOccurrenceKey {
	return dedupeCandidateOccurrenceKey{
		source: source,
		crc64:  candidate.CRC64,
		// SAS rotation must not turn one target occurrence into multiple telemetry entries.
		targetURI:    sanitizedDestForDedupe(candidate.TargetURI),
		targetOffset: candidate.TargetOffset,
		targetLength: candidate.TargetLength,
		etag:         candidate.ETag,
	}
}

type dedupeSourceHashResolutionState struct {
	hashes    map[srcBlockKey]srcBlockHashes
	attempted map[srcBlockKey]struct{}
	// These sets live with one source file and prevent committed-table generations from being recounted.
	seenCandidateBlocks      map[srcBlockKey]struct{}
	seenCandidateOccurrences map[dedupeCandidateOccurrenceKey]struct{}
	invalid                  bool
}

func cloneDedupeSourceHashResolutionState(
	state dedupeSourceHashResolutionState,
) dedupeSourceHashResolutionState {
	cloned := dedupeSourceHashResolutionState{
		hashes:                   make(map[srcBlockKey]srcBlockHashes, len(state.hashes)),
		attempted:                make(map[srcBlockKey]struct{}, len(state.attempted)),
		seenCandidateBlocks:      make(map[srcBlockKey]struct{}, len(state.seenCandidateBlocks)),
		seenCandidateOccurrences: make(map[dedupeCandidateOccurrenceKey]struct{}, len(state.seenCandidateOccurrences)),
		invalid:                  state.invalid,
	}
	for key, hashes := range state.hashes {
		cloned.hashes[key] = hashes
	}
	for key := range state.attempted {
		cloned.attempted[key] = struct{}{}
	}
	for key := range state.seenCandidateBlocks {
		cloned.seenCandidateBlocks[key] = struct{}{}
	}
	for key := range state.seenCandidateOccurrences {
		cloned.seenCandidateOccurrences[key] = struct{}{}
	}
	return cloned
}

func resolveDedupeCandidateHashes(
	ctx context.Context,
	state *dedupeJobState,
	plan *SourceGridPlan,
	sourceETag azcore.ETag,
	currentTargetURI string,
	hasher dedupeRangeHasher,
) (map[srcBlockKey]srcBlockHashes, dedupeHashResolutionStats, error) {
	sourceState, stats, err := resolveDedupeCandidateHashesIncremental(
		ctx,
		state,
		plan,
		sourceETag,
		currentTargetURI,
		hasher,
		dedupeSourceHashResolutionState{},
	)
	return sourceState.hashes, stats, err
}

func resolveDedupeCandidateHashesIncremental(
	ctx context.Context,
	state *dedupeJobState,
	plan *SourceGridPlan,
	sourceETag azcore.ETag,
	currentTargetURI string,
	hasher dedupeRangeHasher,
	existing dedupeSourceHashResolutionState,
) (dedupeSourceHashResolutionState, dedupeHashResolutionStats, error) {
	sourceState := cloneDedupeSourceHashResolutionState(existing)
	stats := dedupeHashResolutionStats{}
	if state == nil || plan == nil || hasher == nil {
		return sourceState, stats, nil
	}
	if sourceState.invalid {
		return sourceState, stats, nil
	}

	candidatesBySource := make(map[srcBlockKey][]common.BlockEntry)
	sourceRequested := make(map[srcBlockKey]uint64)
	sourceRanges := make([]blockblob.BlobHashRange, 0)
	for _, block := range plan.Blocks {
		if !blockHasCRC64(block) {
			continue
		}

		key := srcBlockKey{offset: block.Offset, size: block.Size}
		for _, candidate := range state.committed.LookupByCRC64AndLength(block.CRC64, block.Size) {
			if candidate.TargetURI == "" ||
				candidate.TargetOffset < 0 ||
				candidate.TargetLength != block.Size ||
				candidate.ETag == "" ||
				sameDedupeTarget(candidate.TargetURI, currentTargetURI) {
				continue
			}
			candidatesBySource[key] = append(candidatesBySource[key], candidate)
		}
		if len(candidatesBySource[key]) == 0 {
			continue
		}
		stats.candidateBlocks++
		stats.candidateOccurrences += len(candidatesBySource[key])
		if _, seen := sourceState.seenCandidateBlocks[key]; !seen {
			sourceState.seenCandidateBlocks[key] = struct{}{}
			stats.newCandidateBlocks++
		}
		for _, candidate := range candidatesBySource[key] {
			occurrence := newDedupeCandidateOccurrenceKey(key, candidate)
			if _, seen := sourceState.seenCandidateOccurrences[occurrence]; seen {
				continue
			}
			sourceState.seenCandidateOccurrences[occurrence] = struct{}{}
			stats.newCandidateOccurrences++
		}
		if _, resolved := sourceState.hashes[key]; resolved {
			continue
		}
		if _, attempted := sourceState.attempted[key]; attempted {
			continue
		}
		sourceRequested[key] = block.CRC64
		sourceRanges = append(sourceRanges, blockblob.BlobHashRange{Offset: block.Offset, Count: block.Size})
	}
	if len(candidatesBySource) == 0 {
		return sourceState, stats, nil
	}

	var resolutionErr error
	if len(sourceRanges) > 0 {
		if sourceETag == "" {
			sourceState.hashes = make(map[srcBlockKey]srcBlockHashes)
			sourceState.invalid = true
			return sourceState, stats, fmt.Errorf("source GetBlockList response did not include an ETag")
		}
		sourceResult, err := hasher.HashSource(ctx, sourceETag, sourceRanges)
		attemptedSourceRanges := attemptedDedupeHashRanges(
			sourceResult.attemptedRanges,
			sourceRequested,
		)
		stats.sourceHashRanges = len(attemptedSourceRanges)
		stats.sourceHashBatches = sourceResult.batches
		for key := range attemptedSourceRanges {
			sourceState.attempted[key] = struct{}{}
		}
		if isDedupePreconditionFailure(err) {
			stats.sourceEpochInvalidations++
			sourceState.hashes = make(map[srcBlockKey]srcBlockHashes)
			sourceState.invalid = true
			return sourceState, stats, err
		}
		sourceResponseHashes := make(map[srcBlockKey][32]byte, len(sourceResult.response.RangeHashes))
		invalidSourceRanges := make(map[srcBlockKey]struct{})
		for _, result := range sourceResult.response.RangeHashes {
			key := srcBlockKey{offset: result.Offset, size: result.Count}
			_, requested := attemptedSourceRanges[key]
			if !requested {
				if resolutionErr == nil {
					resolutionErr = fmt.Errorf(
						"source GetBlobHash returned an unrequested range offset=%d count=%d",
						result.Offset,
						result.Count,
					)
				}
				continue
			}
			if _, seen := sourceResponseHashes[key]; seen {
				delete(sourceResponseHashes, key)
				invalidSourceRanges[key] = struct{}{}
				if resolutionErr == nil {
					resolutionErr = fmt.Errorf(
						"source GetBlobHash returned duplicate results for offset=%d count=%d",
						result.Offset,
						result.Count,
					)
				}
				continue
			}
			if _, invalid := invalidSourceRanges[key]; invalid {
				continue
			}
			if len(result.SHA256) != 32 {
				invalidSourceRanges[key] = struct{}{}
				if resolutionErr == nil {
					resolutionErr = fmt.Errorf(
						"source GetBlobHash returned a %d-byte SHA256 for offset=%d count=%d",
						len(result.SHA256), result.Offset, result.Count)
				}
				continue
			}
			var sha [32]byte
			copy(sha[:], result.SHA256)
			sourceResponseHashes[key] = sha
		}
		resolvedThisCall := make(map[srcBlockKey]struct{}, len(sourceResponseHashes))
		for key, sha := range sourceResponseHashes {
			crc64 := attemptedSourceRanges[key]
			sourceState.hashes[key] = srcBlockHashes{crc64: crc64, sha256: sha}
			resolvedThisCall[key] = struct{}{}
		}
		if err != nil {
			resolutionErr = err
		} else if len(resolvedThisCall) != len(attemptedSourceRanges) && resolutionErr == nil {
			resolutionErr = fmt.Errorf(
				"source GetBlobHash omitted %d requested range(s)",
				len(attemptedSourceRanges)-len(resolvedThisCall),
			)
		}
	}

	targetRanges := make(map[dedupeTargetEpoch]map[srcBlockKey]uint64)
	for sourceKey, candidates := range candidatesBySource {
		if _, resolved := sourceState.hashes[sourceKey]; !resolved {
			continue
		}
		for _, candidate := range candidates {
			epoch := dedupeTargetEpoch{targetURI: candidate.TargetURI, etag: candidate.ETag}
			if targetRanges[epoch] == nil {
				targetRanges[epoch] = make(map[srcBlockKey]uint64)
			}
			targetRanges[epoch][srcBlockKey{
				offset: candidate.TargetOffset,
				size:   candidate.TargetLength,
			}] = candidate.CRC64
		}
	}

	epochs := make([]dedupeTargetEpoch, 0, len(targetRanges))
	for epoch := range targetRanges {
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool {
		if epochs[i].targetURI == epochs[j].targetURI {
			return epochs[i].etag < epochs[j].etag
		}
		return epochs[i].targetURI < epochs[j].targetURI
	})

	for _, epoch := range epochs {
		if ctx.Err() != nil {
			break
		}
		epochLock := state.targetHashEpochLock(epoch)
		epochLock.Lock()
		if ctx.Err() != nil {
			epochLock.Unlock()
			break
		}

		unknownRanges := make(map[srcBlockKey]uint64)
		for key, crc64 := range targetRanges[epoch] {
			candidate, found := exactDedupeTargetOccurrence(
				state.committed,
				crc64,
				epoch,
				key,
			)
			if !found {
				continue
			}
			if candidate.HasSHA256 {
				stats.targetSHAIndexHits++
				continue
			}
			if state.targetHashRangeFailed(epoch, key) {
				continue
			}
			stats.targetSHAIndexMisses++
			unknownRanges[key] = crc64
		}

		keys := make([]srcBlockKey, 0, len(unknownRanges))
		for key := range unknownRanges {
			keys = append(keys, key)
		}
		if len(keys) == 0 {
			epochLock.Unlock()
			continue
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].offset == keys[j].offset {
				return keys[i].size < keys[j].size
			}
			return keys[i].offset < keys[j].offset
		})
		ranges := make([]blockblob.BlobHashRange, len(keys))
		for i, key := range keys {
			ranges[i] = blockblob.BlobHashRange{Offset: key.offset, Count: key.size}
		}
		if ctx.Err() != nil {
			epochLock.Unlock()
			break
		}

		targetResult, err := hasher.HashTarget(ctx, epoch.targetURI, epoch.etag, ranges)
		attemptedTargetRanges := attemptedDedupeHashRanges(
			targetResult.attemptedRanges,
			unknownRanges,
		)
		stats.targetHashRanges += len(attemptedTargetRanges)
		stats.targetHashBatches += targetResult.batches
		requestAttempted := len(attemptedTargetRanges) > 0
		if requestAttempted && isDedupePreconditionFailure(err) {
			stats.targetEpochInvalidations++
			state.committed.RemoveTargetEpoch(epoch.targetURI, epoch.etag)
			state.clearTargetHashFailures(epoch)
			epochLock.Unlock()
			continue
		}

		targetResponseHashes := make(map[srcBlockKey][32]byte, len(targetResult.response.RangeHashes))
		invalidTargetRanges := make(map[srcBlockKey]struct{})
		for _, result := range targetResult.response.RangeHashes {
			key := srcBlockKey{offset: result.Offset, size: result.Count}
			_, requested := attemptedTargetRanges[key]
			if !requested {
				stats.targetHashFailures++
				continue
			}
			if _, seen := targetResponseHashes[key]; seen {
				delete(targetResponseHashes, key)
				invalidTargetRanges[key] = struct{}{}
				continue
			}
			if _, invalid := invalidTargetRanges[key]; invalid {
				continue
			}
			if len(result.SHA256) != 32 {
				invalidTargetRanges[key] = struct{}{}
				continue
			}
			var sha [32]byte
			copy(sha[:], result.SHA256)
			targetResponseHashes[key] = sha
		}

		resolvedTargetRanges := make(map[srcBlockKey]struct{}, len(targetResponseHashes))
		for key, sha := range targetResponseHashes {
			crc64 := attemptedTargetRanges[key]
			if !state.committed.SetSHA256ForCRC64(
				crc64,
				epoch.targetURI,
				key.offset,
				key.size,
				epoch.etag,
				sha,
			) {
				continue
			}
			resolvedTargetRanges[key] = struct{}{}
		}

		failedRanges := make([]srcBlockKey, 0, len(attemptedTargetRanges)-len(resolvedTargetRanges))
		for key := range attemptedTargetRanges {
			if _, resolved := resolvedTargetRanges[key]; !resolved {
				failedRanges = append(failedRanges, key)
			}
		}
		if requestAttempted {
			state.markTargetHashRangesFailed(epoch, failedRanges)
		}
		if err != nil {
			stats.targetHashFailures++
		} else {
			stats.targetHashFailures += len(failedRanges)
		}
		epochLock.Unlock()
	}

	return sourceState, stats, resolutionErr
}

func exactDedupeTargetOccurrence(
	table *common.DedupeHashTable,
	crc64 uint64,
	epoch dedupeTargetEpoch,
	key srcBlockKey,
) (common.BlockEntry, bool) {
	for _, candidate := range table.LookupByCRC64AndLength(crc64, key.size) {
		if candidate.TargetURI == epoch.targetURI &&
			candidate.ETag == epoch.etag &&
			candidate.TargetOffset == key.offset {
			return candidate, true
		}
	}
	return common.BlockEntry{}, false
}

func applyResolvedSourceHashes(plan *SourceGridPlan, index map[srcBlockKey]srcBlockHashes) {
	if plan == nil {
		return
	}
	for i := range plan.Blocks {
		key := srcBlockKey{offset: plan.Blocks[i].Offset, size: plan.Blocks[i].Size}
		hashes, ok := index[key]
		if !ok {
			plan.Blocks[i].SHA256 = [32]byte{}
			plan.Blocks[i].HasSHA256 = false
			plan.Blocks[i].HasHashes = false
			continue
		}
		plan.Blocks[i].SHA256 = hashes.sha256
		plan.Blocks[i].HasSHA256 = true
		plan.Blocks[i].HasHashes = blockHasCRC64(plan.Blocks[i])
	}
}

func isDedupePreconditionFailure(err error) bool {
	var responseError *azcore.ResponseError
	return errors.As(err, &responseError) && responseError.StatusCode == http.StatusPreconditionFailed
}
