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
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// This file implements "Phase 2/3" of the block-level dedupe prototype: acting on dedupe hits.
//
// It builds on the read-only Phase 0/1 observer+recorder. When AZCOPY_DEDUPE_ACT is set, a
// block-blob -> block-blob S2S transfer is chunked on the source's committed block boundaries
// (Phase 3) instead of AzCopy's uniform grid, so that every chunk lines up with a source block
// that carries content hashes. Each such chunk is then looked up against the job's committed
// table (Phase 2): on a hit, the block can be staged from the already-migrated destination blob
// instead of re-read from the source.
//
// Everything here is gated behind AZCOPY_DEDUPE_ACT and is a no-op when it is unset.

// dedupeActMode selects how aggressively the dedupe prototype acts on a hit.
type dedupeActMode int

const (
	// dedupeActOff is the default (zero value): no dedupe behavior at all.
	dedupeActOff dedupeActMode = iota
	// dedupeActShadow chunks on source boundaries and LOGS which blocks would be referenced from
	// the destination, but still stages every block from the source (no change to the bytes written).
	dedupeActShadow
	// dedupeActEnforce additionally stages a referenced block from the destination sub-range, with
	// an If-Match ETag guard and automatic fallback to the source on any failure.
	dedupeActEnforce
)

func (m dedupeActMode) String() string {
	switch m {
	case dedupeActShadow:
		return "shadow"
	case dedupeActEnforce:
		return "enforce"
	default:
		return "off"
	}
}

// dedupeActModeFromEnv parses AZCOPY_DEDUPE_ACT. "shadow" and "enforce" select the matching mode
// (case-insensitive); "true" is treated as enforce for convenience; anything else is off.
func dedupeActModeFromEnv() dedupeActMode {
	return parseDedupeActMode(common.GetEnvironmentVariable(common.EEnvironmentVariable.DedupeAct()))
}

// parseDedupeActMode is the pure core of dedupeActModeFromEnv, split out so it can be unit tested.
func parseDedupeActMode(raw string) dedupeActMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "shadow":
		return dedupeActShadow
	case "enforce", "true":
		return dedupeActEnforce
	default:
		return dedupeActOff
	}
}

// getSourceBlockList fetches the source blob's committed block list, requesting the per-block
// content hashes (include=crc64,sha256). The hashes are only populated when the service GetHash
// feature is enabled; otherwise the fields come back empty and callers simply see zero hashes.
// It is shared by the read-only observer and the act path.
func getSourceBlockList(jptm IJobPartTransferMgr) (blockblob.GetBlockListResponse, error) {
	info := jptm.Info()
	bsc, err := jptm.SrcServiceClient().BlobServiceClient()
	if err != nil {
		return blockblob.GetBlockListResponse{}, err
	}
	srcClient := bsc.NewContainerClient(info.SrcContainer).NewBlockBlobClient(info.SrcFilePath)
	return srcClient.GetBlockList(jptm.Context(), blockblob.BlockListTypeCommitted, &blockblob.GetBlockListOptions{
		Include: []blockblob.BlockListIncludeItem{
			blockblob.BlockListIncludeItemCrc64,
			blockblob.BlockListIncludeItemSha256,
		},
	})
}

// rawCommittedBlocksFromResponse extracts the minimal per-block info (name, size, and the optional
// content hashes) from a committed block list response. Azure Storage encodes the per-block CRC64
// little-endian.
func rawCommittedBlocksFromResponse(resp blockblob.GetBlockListResponse) []rawCommittedBlock {
	raw := make([]rawCommittedBlock, 0, len(resp.CommittedBlocks))
	for _, b := range resp.CommittedBlocks {
		rb := rawCommittedBlock{
			Name: common.IffNotNil(b.Name, ""),
			Size: common.IffNotNil(b.Size, 0),
		}
		if len(b.Crc64) == 8 {
			rb.CRC64 = binary.LittleEndian.Uint64(b.Crc64)
		}
		copy(rb.SHA256[:], b.Sha256)
		raw = append(raw, rb)
	}
	return raw
}

// fetchSourceGridPlan fetches the source committed block list and turns it into a SourceGridPlan
// (boundaries plus any per-block hashes). It returns a nil plan with no error when the source has no
// committed named blocks (e.g. a single-PutBlob or empty blob), which simply means the blob is not
// eligible for source-grid chunking.
func fetchSourceGridPlan(jptm IJobPartTransferMgr) (*SourceGridPlan, error) {
	resp, err := getSourceBlockList(jptm)
	if err != nil {
		return nil, err
	}
	if len(resp.CommittedBlocks) == 0 {
		return nil, nil
	}
	return buildSourceGridPlan(rawCommittedBlocksFromResponse(resp))
}

// chunkSpec is a single source-grid chunk: a contiguous [offset, offset+size) range of the source
// blob whose boundaries match a source committed block rather than AzCopy's uniform grid.
type chunkSpec struct {
	offset int64
	size   int64
}

// sourceGridChunker is implemented by senders that can override AzCopy's uniform chunk grid with a
// content-defined one. When dedupeChunkSpecs returns a non-empty slice, scheduleSendChunks iterates
// those specs instead of the uniform startIndex loop.
type sourceGridChunker interface {
	dedupeChunkSpecs() []chunkSpec
}

// chunkSpecsFromPlan converts a SourceGridPlan into the ordered chunk specs used by the scheduler.
func chunkSpecsFromPlan(plan *SourceGridPlan) []chunkSpec {
	if plan == nil {
		return nil
	}
	specs := make([]chunkSpec, len(plan.Blocks))
	for i, b := range plan.Blocks {
		specs[i] = chunkSpec{offset: b.Offset, size: b.Size}
	}
	return specs
}
