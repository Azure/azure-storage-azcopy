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
)

type dedupeRangeHasher interface {
	HashSource(context.Context, azcore.ETag, []blockblob.BlobHashRange) (blockblob.GetBlobHashResponse, int, error)
	HashTarget(context.Context, string, azcore.ETag, []blockblob.BlobHashRange) (blockblob.GetBlobHashResponse, int, error)
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
) (blockblob.GetBlobHashResponse, int, error) {
	return h.hashRanges(ctx, "source", h.source, etag, ranges, nil)
}

func (h *sdkDedupeRangeHasher) HashTarget(
	ctx context.Context,
	targetURI string,
	etag azcore.ETag,
	ranges []blockblob.BlobHashRange,
) (blockblob.GetBlobHashResponse, int, error) {
	target, err := h.targetClient(targetURI)
	if err != nil {
		return blockblob.GetBlobHashResponse{}, 0, err
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
) (blockblob.GetBlobHashResponse, int, error) {
	batches, err := batchDedupeBlobHashRanges(ranges)
	if err != nil {
		return blockblob.GetBlobHashResponse{}, 0, err
	}

	combined := blockblob.GetBlobHashResponse{
		RangeHashes: make([]blockblob.BlobHashResult, 0, len(ranges)),
	}
	for i, batch := range batches {
		response, err := client.GetBlobHash(ctx, batch, &blockblob.GetBlobHashOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: &etag},
			},
			CPKInfo: cpkInfo,
		})
		if err != nil {
			return blockblob.GetBlobHashResponse{}, i + 1, err
		}
		if h.onResponse != nil {
			h.onResponse(role, response)
		}
		combined.RangeHashes = append(combined.RangeHashes, response.RangeHashes...)
		combined.ETag = response.ETag
		combined.LastModified = response.LastModified
		combined.BlobContentLength = response.BlobContentLength
		combined.HashAlgorithm = response.HashAlgorithm
		combined.RequestID = response.RequestID
		combined.ClientRequestID = response.ClientRequestID
		combined.Version = response.Version
	}
	return combined, len(batches), nil
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

		if len(batch) == dedupeGetBlobHashMaxRanges ||
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

type dedupeHashResolutionStats struct {
	candidateBlocks          int
	candidateOccurrences     int
	sourceHashRanges         int
	sourceHashBatches        int
	targetHashRanges         int
	targetHashBatches        int
	targetHashCacheHits      int
	targetHashCacheMisses    int
	targetHashFailures       int
	sourceEpochInvalidations int
	targetEpochInvalidations int
}

type dedupeTargetEpoch struct {
	targetURI string
	etag      azcore.ETag
}

func resolveDedupeCandidateHashes(
	ctx context.Context,
	state *dedupeJobState,
	plan *SourceGridPlan,
	sourceETag azcore.ETag,
	currentTargetURI string,
	hasher dedupeRangeHasher,
) (map[srcBlockKey]srcBlockHashes, dedupeHashResolutionStats, error) {
	index := make(map[srcBlockKey]srcBlockHashes)
	stats := dedupeHashResolutionStats{}
	if state == nil || plan == nil || hasher == nil {
		return index, stats, nil
	}

	candidatesBySource := make(map[srcBlockKey][]common.BlockEntry)
	sourceCRC := make(map[srcBlockKey]uint64)
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
		sourceCRC[key] = block.CRC64
		sourceRanges = append(sourceRanges, blockblob.BlobHashRange{Offset: block.Offset, Count: block.Size})
	}
	if len(sourceRanges) == 0 {
		return index, stats, nil
	}
	if sourceETag == "" {
		return index, stats, fmt.Errorf("source GetBlockList response did not include an ETag")
	}

	sourceResponse, batches, err := hasher.HashSource(ctx, sourceETag, sourceRanges)
	stats.sourceHashRanges = len(sourceRanges)
	stats.sourceHashBatches = batches
	if err != nil {
		if isDedupePreconditionFailure(err) {
			stats.sourceEpochInvalidations++
		}
		return index, stats, err
	}
	for _, result := range sourceResponse.RangeHashes {
		if len(result.SHA256) != 32 {
			return index, stats, fmt.Errorf(
				"source GetBlobHash returned a %d-byte SHA256 for offset=%d count=%d",
				len(result.SHA256), result.Offset, result.Count)
		}
		var sha [32]byte
		copy(sha[:], result.SHA256)
		key := srcBlockKey{offset: result.Offset, size: result.Count}
		crc64, requested := sourceCRC[key]
		if !requested {
			return index, stats, fmt.Errorf(
				"source GetBlobHash returned an unrequested range offset=%d count=%d",
				result.Offset,
				result.Count,
			)
		}
		index[key] = srcBlockHashes{crc64: crc64, sha256: sha}
	}

	targetRanges := make(map[dedupeTargetEpoch]map[srcBlockKey]uint64)
	for _, candidates := range candidatesBySource {
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
		epochLock := state.targetHashEpochLock(epoch)
		epochLock.Lock()

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
				stats.targetHashCacheHits++
				continue
			}
			stats.targetHashCacheMisses++
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

		response, batches, err := hasher.HashTarget(ctx, epoch.targetURI, epoch.etag, ranges)
		stats.targetHashRanges += len(ranges)
		stats.targetHashBatches += batches
		if err != nil {
			if isDedupePreconditionFailure(err) {
				stats.targetEpochInvalidations++
				state.committed.RemoveTargetEpoch(epoch.targetURI, epoch.etag)
			} else {
				stats.targetHashFailures++
			}
			epochLock.Unlock()
			continue
		}

		for _, result := range response.RangeHashes {
			if len(result.SHA256) != 32 {
				stats.targetHashFailures++
				continue
			}
			var sha [32]byte
			copy(sha[:], result.SHA256)
			key := srcBlockKey{offset: result.Offset, size: result.Count}
			crc64, requested := unknownRanges[key]
			if !requested {
				stats.targetHashFailures++
				continue
			}
			if !state.committed.SetSHA256ForCRC64(
				crc64,
				epoch.targetURI,
				result.Offset,
				result.Count,
				epoch.etag,
				sha,
			) {
				stats.targetHashFailures++
			}
		}
		epochLock.Unlock()
	}

	return index, stats, nil
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
