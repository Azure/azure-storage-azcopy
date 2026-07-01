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
	"encoding/hex"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// This file implements "Phase 0" of the block-level dedupe prototype: a read-only,
// opt-in diagnostic (AZCOPY_DEDUPE_OBSERVE=true) that, for block-blob -> block-blob S2S
// transfers, builds a "source-grid" chunk plan from the source's committed block list and
// logs how those content-defined boundaries compare to AzCopy's uniform chunk grid.
//
// It never changes transfer behavior. Its purpose is to gather real alignment / would-be
// dedupe-hit numbers before any scheduling change is attempted (Phase 1+).

// PlannedBlock describes one block of a source-grid chunk plan: a contiguous range of the
// source blob whose boundaries match the source's committed block list rather than AzCopy's
// uniform chunk grid.
type PlannedBlock struct {
	Offset       int64
	Size         int64
	SrcBlockName string

	// CRC64 and SHA256 are intended to be populated from the extended GetBlockList response
	// (include=crc64,sha256). They are left zero until that service extension is available;
	// Phase 0 only needs the boundaries (offset/size), not the hashes.
	CRC64  uint64
	SHA256 [32]byte
}

// SourceGridPlan is an ordered, contiguous set of blocks covering [0, TotalSize) of a source
// blob, derived from its committed block list.
type SourceGridPlan struct {
	Blocks    []PlannedBlock
	TotalSize int64
}

// rawCommittedBlock is the minimal block info needed to build a plan: the committed block name
// and its size, plus the optional per-block content hashes returned when the service supports the
// include=crc64,sha256 extension. Offsets are derived (committed block lists are returned in order).
type rawCommittedBlock struct {
	Name   string
	Size   int64
	CRC64  uint64
	SHA256 [32]byte
}

// buildSourceGridPlan converts an ordered committed block list into a SourceGridPlan, assigning
// each block a contiguous offset equal to the prefix sum of the preceding block sizes.
func buildSourceGridPlan(blocks []rawCommittedBlock) (*SourceGridPlan, error) {
	plan := &SourceGridPlan{Blocks: make([]PlannedBlock, 0, len(blocks))}
	var offset int64
	for i, b := range blocks {
		if b.Size < 0 {
			return nil, fmt.Errorf("committed block %d (%q) has negative size %d", i, b.Name, b.Size)
		}
		plan.Blocks = append(plan.Blocks, PlannedBlock{
			Offset:       offset,
			Size:         b.Size,
			SrcBlockName: b.Name,
			CRC64:        b.CRC64,
			SHA256:       b.SHA256,
		})
		offset += b.Size
	}
	plan.TotalSize = offset
	return plan, nil
}

// GridAlignmentStats summarizes how a SourceGridPlan's content-defined boundaries compare to a
// uniform chunk grid of a given size.
type GridAlignmentStats struct {
	SourceBlockCount  int
	UniformChunkSize  int64
	UniformChunkCount int64
	TotalSize         int64

	// AlignedStarts is the number of source blocks whose Offset is a multiple of UniformChunkSize.
	AlignedStarts int

	// ExactChunkMatches is the key metric: the number of source blocks whose [Offset, Offset+Size)
	// range exactly equals a uniform-grid chunk. These are the blocks a uniform-grid hashing scheme
	// could dedupe identically; everything else needs source-grid chunking to ever produce a hit.
	ExactChunkMatches int
}

// AlignmentToUniformGrid reports how the plan lines up with AzCopy's uniform chunk grid of the
// given size. It is a pure function (no I/O), which keeps it easy to unit test.
func (p *SourceGridPlan) AlignmentToUniformGrid(uniformChunkSize int64) GridAlignmentStats {
	stats := GridAlignmentStats{
		SourceBlockCount: len(p.Blocks),
		UniformChunkSize: uniformChunkSize,
		TotalSize:        p.TotalSize,
	}
	if uniformChunkSize <= 0 {
		return stats
	}
	if p.TotalSize > 0 {
		stats.UniformChunkCount = (p.TotalSize + uniformChunkSize - 1) / uniformChunkSize
	}
	for _, b := range p.Blocks {
		if b.Offset%uniformChunkSize != 0 {
			continue
		}
		stats.AlignedStarts++

		// The uniform chunk that starts at b.Offset ends at min(b.Offset+chunk, TotalSize).
		uniformEnd := b.Offset + uniformChunkSize
		if uniformEnd > p.TotalSize {
			uniformEnd = p.TotalSize
		}
		if b.Offset+b.Size == uniformEnd {
			stats.ExactChunkMatches++
		}
	}
	return stats
}

// String renders the stats as a single log line, including the percentage of source blocks that
// would dedupe on AzCopy's current uniform grid.
func (s GridAlignmentStats) String() string {
	pct := 0.0
	if s.SourceBlockCount > 0 {
		pct = 100 * float64(s.ExactChunkMatches) / float64(s.SourceBlockCount)
	}
	return fmt.Sprintf(
		"dedupe-observe: sourceBlocks=%d uniformChunkSize=%d uniformChunks=%d totalSize=%d alignedStarts=%d exactChunkMatches=%d (%.1f%% of source blocks would dedupe on AzCopy's uniform grid)",
		s.SourceBlockCount, s.UniformChunkSize, s.UniformChunkCount, s.TotalSize,
		s.AlignedStarts, s.ExactChunkMatches, pct)
}

// dedupeObserveEnabled reports whether the read-only source-grid diagnostic is turned on.
func dedupeObserveEnabled() bool {
	return common.GetEnvironmentVariable(common.EEnvironmentVariable.DedupeObserve()) == "true"
}

// observeSourceGrid is the Phase 0 diagnostic hook. For block-blob -> block-blob S2S transfers
// (and only when AZCOPY_DEDUPE_OBSERVE=true) it fetches the source committed block list, builds a
// SourceGridPlan, and logs how the source boundaries align with AzCopy's uniform chunk grid.
//
// It is intentionally defensive: it never returns an error and swallows every failure (logging at
// debug level), so it cannot affect transfer correctness and is safe to call unconditionally.
func observeSourceGrid(jptm IJobPartTransferMgr) {
	if !dedupeObserveEnabled() {
		return
	}

	info := jptm.Info()

	// Scope to block-blob -> block-blob S2S only.
	ft := jptm.FromTo()
	if ft.From() != common.ELocation.Blob() || ft.To() != common.ELocation.Blob() {
		return
	}
	if info.SrcBlobType != blob.BlobTypeBlockBlob {
		return
	}

	// Fetch the source committed block list, requesting the per-block content hashes
	// (include=crc64,sha256). The service only returns them when the GetHash feature is enabled;
	// otherwise the fields are nil and we simply observe zero hashes.
	resp, err := getSourceBlockList(jptm)
	if err != nil {
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "dedupe-observe: GetBlockList(committed, include=crc64,sha256) failed: "+err.Error())
		return
	}

	if len(resp.CommittedBlocks) == 0 {
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo,
			"dedupe-observe: source has no committed named blocks (e.g. single-PutBlob or empty blob); not eligible for source-grid chunking")
		return
	}

	raw := make([]rawCommittedBlock, 0, len(resp.CommittedBlocks))
	hashedBlocks := 0
	for i, b := range resp.CommittedBlocks {
		rb := rawCommittedBlock{
			Name: common.IffNotNil(b.Name, ""),
			Size: common.IffNotNil(b.Size, 0),
		}
		// Azure Storage encodes CRC64 little-endian (matches crc64.Checksum(content, azure table)).
		if len(b.Crc64) == 8 {
			rb.CRC64 = binary.LittleEndian.Uint64(b.Crc64)
		}
		copy(rb.SHA256[:], b.Sha256)
		if len(b.Crc64) > 0 || len(b.Sha256) > 0 {
			hashedBlocks++
		}
		raw = append(raw, rb)

		// Phase 0 read-only log of the extended per-block fields exactly as returned by the service.
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
			"dedupe-observe: block[%d] name=%s offset=%d size=%d crc64=%s sha256=%s",
			i, rb.Name, common.IffNotNil(b.Offset, -1), rb.Size,
			hex.EncodeToString(b.Crc64), hex.EncodeToString(b.Sha256)))
	}

	jptm.LogAtLevelForCurrentTransfer(common.LogInfo, fmt.Sprintf(
		"dedupe-observe: %d/%d committed blocks carried crc64+sha256 hashes (0 means the service GetHash feature is off or include was not honored)",
		hashedBlocks, len(resp.CommittedBlocks)))

	plan, err := buildSourceGridPlan(raw)
	if err != nil {
		jptm.LogAtLevelForCurrentTransfer(common.LogDebug, "dedupe-observe: "+err.Error())
		return
	}

	stats := plan.AlignmentToUniformGrid(info.BlockSize)
	jptm.LogAtLevelForCurrentTransfer(common.LogInfo, stats.String())

	// Phase 1: record these committed blocks into the per-job dedupe table and log the running
	// would-be-hit rate. This is still read-only with respect to the transfer (no bytes skipped).
	recordSourceGridForDedupe(jptm, plan)
}
