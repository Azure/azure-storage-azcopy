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

package azcopy

import (
	"bytes"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// syncDedupProcessor wraps the normal transfer scheduler and checks whether the content
// to be transferred already exists elsewhere on the destination (by MD5 hash match).
// When a match is found, it schedules an intra-destination server-side copy instead of
// uploading from source, saving bandwidth and time.
type syncDedupProcessor struct {
	// hashIndex is the secondary index of destination objects keyed by MD5 hash.
	hashIndex *traverser.HashIndexer

	// dedupScheduler schedules server-side copies within the destination (e.g., BlobBlob).
	dedupScheduler *CopyTransferProcessor

	// normalScheduler schedules normal transfers (upload from source or S2S from source).
	normalScheduler *CopyTransferProcessor

	// dedupFromTo is the FromTo type for intra-destination copies (e.g., BlobBlob).
	dedupFromTo common.FromTo

	// Counters for reporting
	dedupCount  int64
	normalCount int64
}

// newSyncDedupProcessor creates a processor that routes transfers through either
// the dedup path (server-side copy within destination) or the normal path.
func newSyncDedupProcessor(
	hashIndex *traverser.HashIndexer,
	dedupScheduler *CopyTransferProcessor,
	normalScheduler *CopyTransferProcessor,
	dedupFromTo common.FromTo,
) *syncDedupProcessor {
	return &syncDedupProcessor{
		hashIndex:       hashIndex,
		dedupScheduler:  dedupScheduler,
		normalScheduler: normalScheduler,
		dedupFromTo:     dedupFromTo,
	}
}

// ProcessTransfer checks if the object's content already exists on the destination
// and routes to either dedup copy or normal transfer accordingly.
func (p *syncDedupProcessor) ProcessTransfer(obj traverser.StoredObject) error {
	// Only attempt dedup for files (not folders/symlinks) that have an MD5 hash
	if obj.EntityType == common.EEntityType.File() && obj.Md5 != nil && len(obj.Md5) > 0 {
		if matchingDest, found := p.hashIndex.Lookup(obj.Md5); found {
			// Verify that it's not the same file (same path already handled by comparator as skip)
			// and that the sizes match (extra safety check beyond MD5)
			if matchingDest.RelativePath != obj.RelativePath &&
				matchingDest.Size == obj.Size &&
				bytes.Equal(matchingDest.Md5, obj.Md5) {
				return p.scheduleDedupCopy(obj, matchingDest)
			}
		}
	}

	// Fall through to normal transfer
	atomic.AddInt64(&p.normalCount, 1)
	return p.normalScheduler.ScheduleSyncRemoveSetPropertiesTransfer(obj)
}

// scheduleDedupCopy schedules a server-side copy from an existing destination blob
// to the target path. The source of the copy is matchingDest (already on destination),
// and the target is where obj should end up.
func (p *syncDedupProcessor) scheduleDedupCopy(obj, matchingDest traverser.StoredObject) error {
	atomic.AddInt64(&p.dedupCount, 1)

	// Build source relative path (the existing blob on the destination)
	srcRelativePath := PathEncodeRules(matchingDest.RelativePath, p.dedupFromTo, false, true)
	if srcRelativePath != "" {
		srcRelativePath = "/" + srcRelativePath
	}

	// Build destination relative path (where we want the file to end up)
	dstRelativePath := PathEncodeRules(obj.RelativePath, p.dedupFromTo, false, false)
	if dstRelativePath != "" {
		dstRelativePath = "/" + dstRelativePath
	}

	// Create a StoredObject that represents the transfer with proper metadata from the source.
	// We use the original source object's metadata (content-type, etc.) so the destination
	// gets the correct properties, but size and MD5 from the matching destination blob.
	dedupObj := traverser.StoredObject{
		Name:               obj.Name,
		EntityType:         obj.EntityType,
		LastModifiedTime:   matchingDest.LastModifiedTime,
		Size:               matchingDest.Size,
		Md5:                matchingDest.Md5,
		RelativePath:       obj.RelativePath,
		ContainerName:      matchingDest.ContainerName,
		DstContainerName:   obj.DstContainerName,
		BlobType:           matchingDest.BlobType,
		BlobAccessTier:     matchingDest.BlobAccessTier,
		ContentType:        obj.ContentType,
		ContentEncoding:    obj.ContentEncoding,
		ContentDisposition: obj.ContentDisposition,
		ContentLanguage:    obj.ContentLanguage,
		CacheControl:       obj.CacheControl,
		Metadata:           obj.Metadata,
		BlobTags:           obj.BlobTags,
	}

	copyTransfer, shouldSendToSte := dedupObj.ToNewCopyTransfer(
		false,
		srcRelativePath,
		dstRelativePath,
		p.dedupScheduler.preserveAccessTier,
		p.dedupScheduler.folderPropertiesOption,
		p.dedupScheduler.symlinkHandlingType,
		p.dedupScheduler.hardlinkHandlingType,
	)

	if !shouldSendToSte {
		return nil
	}

	// Log the dedup action
	if common.AzcopyScanningLogger != nil {
		common.AzcopyScanningLogger.Log(common.LogInfo,
			"File "+obj.RelativePath+" will be server-side copied from existing destination path "+matchingDest.RelativePath+" (content dedup)")
	}

	return p.dedupScheduler.scheduleTransfer(copyTransfer.Source, copyTransfer.Destination, dedupObj)
}

// DedupCount returns the number of transfers that were handled via server-side copy dedup.
func (p *syncDedupProcessor) DedupCount() int64 {
	return atomic.LoadInt64(&p.dedupCount)
}

// NormalCount returns the number of transfers that went through the normal upload path.
func (p *syncDedupProcessor) NormalCount() int64 {
	return atomic.LoadInt64(&p.normalCount)
}
