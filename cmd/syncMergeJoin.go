//go:build smslidingwindow
// +build smslidingwindow

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

package cmd

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// incrementSourceScanned increments the source files or folders scanned counter
// in the merge-join path, matching the traverser's IncrementEnumeration behavior.
// Virtual prefixes (FNS blob directories) are NOT counted, matching the traverser
// which only counts real blobs/folders (HNS).
func incrementSourceScanned(cca *cookedSyncCmdArgs, entityType common.EntityType, isVirtualPrefix bool) {
	if isVirtualPrefix {
		return
	}
	if entityType == common.EEntityType.File() {
		atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
	} else if entityType == common.EEntityType.Folder() {
		atomic.AddUint64(&cca.atomicSourceFoldersScanned, 1)
	}
}

// activeMergeJoinDirs tracks how many mergeJoinSyncDir calls are in-flight.
var activeMergeJoinDirs atomic.Int64

// usePageLevelMergeJoin returns true when AZCOPY_USE_PAGE_LEVEL_MERGE_JOIN is "true" or "1".
// Default is false (channel-based).
func usePageLevelMergeJoin() bool {
	v := strings.ToLower(strings.TrimSpace(
		common.GetEnvironmentVariable(common.EEnvironmentVariable.UsePageLevelMergeJoin())))
	return v == "true" || v == "1"
}

// mergeJoinChannelBufferSize controls the buffer size of channels used to bridge
// push-based traversers into pull-based iteration for the channel-based merge-join.
// Each slot holds one StoredObject (~430 bytes), so 4K slots ≈ 1.7MB per channel.
// Sized to roughly one ListBlobs page (5K) so the consumer can keep draining while
// the producer is blocked fetching the next page over the network.
const mergeJoinChannelBufferSize = 4_000

// Channel-based merge-join diagnostics (baseline instrumentation).
// mergeJoinChanFullEvents increments each time a producer's send finds the channel
// full (back-pressure: the merge/consumer is slower than the listing/producer —
// batching sends would help).
// mergeJoinChanDryEvents increments each time a consumer's receive finds the channel
// empty while still open (starvation: the listing/producer/network is slower than the
// merge/consumer — batching would NOT help; the wall is listing/XML/network).
var (
	mergeJoinChanFullEvents atomic.Int64
	mergeJoinChanDryEvents  atomic.Int64
)

// traverserToChannel starts a goroutine that runs a ResourceTraverser and bridges
// its push-based callback into a pull-based channel. The caller receives objects
// one at a time via the returned channel. The traverser goroutine blocks when the
// channel buffer is full, providing natural back-pressure.
//
// The error channel receives at most one error if the traversal fails.
// Both channels are closed when the traverser goroutine exits.
func traverserToChannel(
	ctx context.Context,
	t ResourceTraverser,
	filters []ObjectFilter,
	label string,
) (<-chan StoredObject, <-chan error) {

	objCh := make(chan StoredObject, mergeJoinChannelBufferSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(objCh)
		defer close(errCh)

		start := time.Now()
		firstObj := true
		count := 0

		err := t.Traverse(noPreProccessor, func(obj StoredObject) error {
			if firstObj {
				firstObj = false
				elapsed := time.Since(start)
				if elapsed > 30*time.Second {
					mergeJoinSyncOneDirLog(common.LogWarning,
						fmt.Sprintf("[SLOW] %s first object took %v (goroutines=%d)", label, elapsed, runtime.NumGoroutine()), true)
				}
			}
			count++
			// Non-blocking send first; if the channel is full, record back-pressure
			// (consumer slower than producer) then fall back to a blocking send.
			select {
			case objCh <- obj:
				return nil
			default:
				mergeJoinChanFullEvents.Add(1)
			}
			select {
			case objCh <- obj:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, filters)

		if firstObj {
			elapsed := time.Since(start)
			if elapsed > 30*time.Second {
				mergeJoinSyncOneDirLog(common.LogWarning,
					fmt.Sprintf("[SLOW] %s returned 0 objects in %v (goroutines=%d)", label, elapsed, runtime.NumGoroutine()), true)
			}
		}

		if err != nil {
			mergeJoinSyncOneDirLog(common.LogError,
				fmt.Sprintf("%s traversal error after %d objects: %v", label, count, err), true)
			errCh <- err
		}
	}()

	return objCh, errCh
}

// mergeJoinRecv receives one object from ch for the channel-based merge-join. It records
// a "channel dry" event when the channel is empty but still open at the moment of receive,
// indicating the consumer (merge) is outpacing the producer (listing/network). A closed
// channel is NOT counted as dry, because the receive is immediately ready.
func mergeJoinRecv(ch <-chan StoredObject) (StoredObject, bool) {
	select {
	case obj, ok := <-ch:
		return obj, ok
	default:
		mergeJoinChanDryEvents.Add(1)
	}
	obj, ok := <-ch
	return obj, ok
}

// mergeJoinSyncDirChannelBased performs a streaming merge-join using traverser-based
// channels. Both source and destination traversers are bridged into pull-based channels
// with a 4000-item buffer, enabling natural back-pressure and concurrent listing.
func mergeJoinSyncDirChannelBased(
	ctx context.Context,
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	dir string,
	pt ResourceTraverser,
	st ResourceTraverser,
	isDestinationPresent bool,
) (subDirs []minimalStoredObject, err error) {

	subDirs = make([]minimalStoredObject, 0, 64)
	activeMergeJoinDirs.Add(1)
	defer activeMergeJoinDirs.Add(-1)

	// Bridge both traversers into channels
	srcLabel := fmt.Sprintf("SRC[%s]", dir)
	dstLabel := fmt.Sprintf("DST[%s]", dir)
	srcCh, srcErrCh := traverserToChannel(ctx, pt, enumerator.filters, srcLabel)
	var dstCh <-chan StoredObject
	var dstErrCh <-chan error

	if isDestinationPresent {
		dstCh, dstErrCh = traverserToChannel(ctx, st, enumerator.filters, dstLabel)
	} else {
		emptyCh := make(chan StoredObject)
		close(emptyCh)
		emptyErrCh := make(chan error)
		close(emptyErrCh)
		dstCh = emptyCh
		dstErrCh = emptyErrCh
	}

	// Build the comparator for property comparison (size, LWT, changeTime)
	comparator := &syncDestinationComparator{
		sourceIndex:             enumerator.objectIndexer,
		copyTransferScheduler:   enumerator.ctp.scheduleCopyTransfer,
		destinationCleaner:      enumerator.objectComparator,
		deleteDestination:       cca.deleteDestination,
		incrementNotTransferred: enumerator.primaryTraverserTemplate.options.IncrementNotTransferred,
		orchestratorOptions:     enumerator.orchestratorOptions,
	}

	// Pull first object from each side
	srcObj, srcOk := mergeJoinRecv(srcCh)
	dstObj, dstOk := mergeJoinRecv(dstCh)

	for srcOk && dstOk {
		srcOrigPath := srcObj.relativePath
		srcPath := buildChildPath(dir, srcObj.relativePath, srcObj.entityType == common.EEntityType.Folder())
		dstPath := buildChildPath(dir, dstObj.relativePath, dstObj.entityType == common.EEntityType.Folder())

		cmp := strings.Compare(srcPath, dstPath)

		switch {
		case cmp < 0:
			srcObj.relativePath = srcPath
			subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcOrigPath, subDirs)
			if err != nil {
				return subDirs, err
			}
			srcObj, srcOk = mergeJoinRecv(srcCh)

		case cmp > 0:
			dstObj.relativePath = dstPath
			err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
			if err != nil {
				return subDirs, err
			}
			dstObj, dstOk = mergeJoinRecv(dstCh)

		default:
			srcObj.relativePath = srcPath
			dstObj.relativePath = dstPath
			subDirs, err = mergeJoinHandleBothExist(enumerator, cca, comparator, srcObj, dstObj, srcOrigPath, subDirs)
			if err != nil {
				return subDirs, err
			}
			srcObj, srcOk = mergeJoinRecv(srcCh)
			dstObj, dstOk = mergeJoinRecv(dstCh)
		}
	}

	// Drain remaining source-only objects
	for srcOk {
		srcOrigPath := srcObj.relativePath
		srcObj.relativePath = buildChildPath(dir, srcObj.relativePath, srcObj.entityType == common.EEntityType.Folder())
		subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcOrigPath, subDirs)
		if err != nil {
			return subDirs, err
		}
		srcObj, srcOk = mergeJoinRecv(srcCh)
	}

	// Drain remaining dest-only objects
	for dstOk {
		dstObj.relativePath = buildChildPath(dir, dstObj.relativePath, dstObj.entityType == common.EEntityType.Folder())
		err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
		if err != nil {
			return subDirs, err
		}
		dstObj, dstOk = mergeJoinRecv(dstCh)
	}

	// Check for traversal errors from either side
	if srcErr := <-srcErrCh; srcErr != nil {
		return subDirs, fmt.Errorf("source traversal error during merge-join: %w", srcErr)
	}
	if dstErr := <-dstErrCh; dstErr != nil {
		return subDirs, fmt.Errorf("destination traversal error during merge-join: %w", dstErr)
	}

	return subDirs, nil
}

// mergeJoinContext holds cached container clients for the page-level merge-join path.
// Created once per sync job and shared across all directory workers to avoid
// creating new HTTP service clients per directory (which is expensive).
type mergeJoinContext struct {
	srcContainerClient *container.Client
	srcContainerName   string
	dstContainerClient *container.Client
	dstContainerName   string
	includeTags        bool
}

// newMergeJoinContext creates the shared container clients for source and destination.
// Called once before the crawl loop starts.
func newMergeJoinContext(enumerator *syncEnumerator, srcBaseURL, dstBaseURL string) (*mergeJoinContext, error) {
	ptt := enumerator.primaryTraverserTemplate
	stt := enumerator.secondaryTraverserTemplate

	srcCC, srcCN, _, err := getContainerClientAndPrefix(srcBaseURL, &ptt.options)
	if err != nil {
		return nil, fmt.Errorf("failed to create source container client: %w", err)
	}

	dstCC, dstCN, _, err := getContainerClientAndPrefix(dstBaseURL, &stt.options)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination container client: %w", err)
	}

	return &mergeJoinContext{
		srcContainerClient: srcCC,
		srcContainerName:   srcCN,
		dstContainerClient: dstCC,
		dstContainerName:   dstCN,
		includeTags:        ptt.options.PreserveBlobTags,
	}, nil
}

// searchPrefixForURL extracts the blob name from a URL and ensures it has a trailing slash.
func searchPrefixForURL(rawURL string) (string, error) {
	_, blobName, _, err := parseBlobURL(rawURL)
	if err != nil {
		return "", err
	}
	if blobName != "" && !strings.HasSuffix(blobName, common.AZCOPY_PATH_SEPARATOR_STRING) {
		blobName += common.AZCOPY_PATH_SEPARATOR_STRING
	}
	return blobName, nil
}

// useStreamingMergeJoin returns true if the source type guarantees lexicographic
// listing order, which is required for the streaming merge-join algorithm.
// Local filesystem (ext4/XFS) does NOT guarantee sorted order, so it stays on
// the existing indexMap-based flow.
func useStreamingMergeJoin(fromTo common.FromTo) bool {
	switch fromTo.From() {
	case common.ELocation.S3(), common.ELocation.Blob(), common.ELocation.BlobFS():
		return true
	default:
		return false
	}
}

// mergeJoinSyncDirPageLevel performs a streaming merge-join of source and destination
// object listings for a single directory. Instead of creating full StoredObject
// for every blob, this reads directly from SDK listing page items via pageCursors.
// StoredObject is only created for the <1% of items that actually need a transfer.
//
// Both sides are prefetched concurrently: each side has a goroutine fetching
// pages from the storage API, with a buffer=4 channel. This ensures source and
// destination listing are always happening in parallel, not sequentially.
//
// For the match case (99.99% of resync), NO StoredObject is created — we just
// compare fields directly from the SDK listing response pages.
func mergeJoinSyncDirPageLevel(
	ctx context.Context,
	mjCtx *mergeJoinContext,
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	dir string,
	srcURL string,
	dstURL string,
	isDestinationPresent bool,
) (subDirs []minimalStoredObject, err error) {

	subDirs = make([]minimalStoredObject, 0, 64)
	activeMergeJoinDirs.Add(1)
	defer activeMergeJoinDirs.Add(-1)

	srcContainerName := mjCtx.srcContainerName
	dstContainerName := mjCtx.dstContainerName

	// Extract search prefix from URL (only parse, no new HTTP client)
	srcSearchPrefix, err := searchPrefixForURL(srcURL)
	if err != nil {
		return subDirs, fmt.Errorf("failed to parse source URL for dir '%s': %w", dir, err)
	}

	var dstPageCh <-chan rawPage
	var dstErrCh <-chan error

	if isDestinationPresent {
		dstSearchPrefix, dstErr := searchPrefixForURL(dstURL)
		if dstErr != nil {
			return subDirs, fmt.Errorf("failed to parse destination URL for dir '%s': %w", dir, dstErr)
		}
		// Start dst prefetch BEFORE blocking on src — both listings run in parallel
		dstPageCh, dstErrCh = startBlobHierarchyPrefetch(ctx, mjCtx.dstContainerClient, dstSearchPrefix, false)
	}

	// Start src prefetch (goroutine launched immediately, non-blocking)
	srcPageCh, srcErrCh := startBlobHierarchyPrefetch(ctx, mjCtx.srcContainerClient, srcSearchPrefix, mjCtx.includeTags)

	// Now block on first pages — both HTTP requests are already in-flight
	src := newPageCursor(srcPageCh, srcErrCh)

	var dst *pageCursor
	if isDestinationPresent {
		dst = newPageCursor(dstPageCh, dstErrCh)
	} else {
		dst = newEmptyCursor()
	}

	// ── Merge-join loop ──
	// Read name, size, LMT directly from raw SDK page items.
	// Only create StoredObject when a transfer is actually needed.
	for !src.done && !dst.done {
		srcRelPath := src.name()
		dstRelPath := dst.name()

		srcIsFolder := src.itemEntityType() == common.EEntityType.Folder()
		dstIsFolder := dst.itemEntityType() == common.EEntityType.Folder()

		srcPath := buildChildPath(dir, srcRelPath, srcIsFolder)
		dstPath := buildChildPath(dir, dstRelPath, dstIsFolder)

		cmp := strings.Compare(srcPath, dstPath)

		switch {
		case cmp < 0:
			// Source-only: create StoredObject and schedule transfer
			srcObj := src.toStoredObject(srcContainerName)
			srcObj.relativePath = srcPath
			srcObj.isVirtualPrefix = src.itemIsVirtualPrefix()
			incrementSourceScanned(cca, src.itemEntityType(), src.itemIsVirtualPrefix())
			subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcRelPath, subDirs)
			if err != nil {
				return subDirs, err
			}
			src.advance()

		case cmp > 0:
			// Dest-only: create StoredObject for deletion handling
			dstObj := dst.toStoredObject(dstContainerName)
			dstObj.relativePath = dstPath
			err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
			if err != nil {
				return subDirs, err
			}
			dst.advance()

		default:
			// Both exist — compare from raw page data (ZERO allocation for match case)
			incrementSourceScanned(cca, src.itemEntityType(), src.itemIsVirtualPrefix())
			subDirs, err = mergeJoinHandleBothExistPageLevel(
				enumerator, cca, dir,
				src, dst,
				srcRelPath, srcPath,
				srcContainerName, dstContainerName,
				subDirs,
			)
			if err != nil {
				return subDirs, err
			}
			src.advance()
			dst.advance()
		}
	}

	// Drain remaining source-only objects
	for !src.done {
		srcRelPath := src.name()
		srcIsFolder := src.itemEntityType() == common.EEntityType.Folder()
		srcPath := buildChildPath(dir, srcRelPath, srcIsFolder)

		srcObj := src.toStoredObject(srcContainerName)
		srcObj.relativePath = srcPath
		srcObj.isVirtualPrefix = src.itemIsVirtualPrefix()
		incrementSourceScanned(cca, src.itemEntityType(), src.itemIsVirtualPrefix())
		subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcRelPath, subDirs)
		if err != nil {
			return subDirs, err
		}
		src.advance()
	}

	// Drain remaining dest-only objects
	for !dst.done {
		dstRelPath := dst.name()
		dstIsFolder := dst.itemEntityType() == common.EEntityType.Folder()
		dstPath := buildChildPath(dir, dstRelPath, dstIsFolder)

		dstObj := dst.toStoredObject(dstContainerName)
		dstObj.relativePath = dstPath
		err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
		if err != nil {
			return subDirs, err
		}
		dst.advance()
	}

	// Check for listing errors from prefetch goroutines
	if srcErr := src.checkError(); srcErr != nil {
		return subDirs, fmt.Errorf("source listing error during merge-join: %w", srcErr)
	}
	if dstErr := dst.checkError(); dstErr != nil {
		return subDirs, fmt.Errorf("destination listing error during merge-join: %w", dstErr)
	}

	return subDirs, nil
}

// isSelfReferentialDirSentinel returns true when a BlobFS (HNS) traverser emits
// the current directory itself as a Folder StoredObject with empty relativePath.
// This sentinel carries root/directory ACLs but must NOT be re-enqueued as a
// subdirectory — otherwise the same directory would be processed again, causing
// duplicate folder counts, failed transfers, and potential infinite loops.
func isSelfReferentialDirSentinel(obj StoredObject, originalRelativePath string, fromTo common.FromTo) bool {
	if obj.entityType != common.EEntityType.Folder() {
		return false
	}
	// GCP S3-compatible source emits directory placeholders with empty relativePath
	if isGCPSource && originalRelativePath == "" {
		return true
	}
	// BlobFS (HNS) traverser emits the current directory as a folder with empty relativePath
	if fromTo.From() == common.ELocation.BlobFS() && originalRelativePath == "" {
		return true
	}
	return false
}

// mergeJoinHandleSourceOnly processes an object that exists at the source but not
// the destination. Files are scheduled for transfer. Folders are added to the
// subdirectory list for recursive traversal.
func mergeJoinHandleSourceOnly(
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	srcObj StoredObject,
	originalRelativePath string,
	subDirs []minimalStoredObject,
) ([]minimalStoredObject, error) {

	if srcObj.entityType == common.EEntityType.Folder() {
		// Skip self-referential directory sentinels (GCP/HNS) from subDirs
		// but still schedule the transfer for ACL propagation
		if isSelfReferentialDirSentinel(srcObj, originalRelativePath, cca.fromTo) {
			err := enumerator.ctp.scheduleCopyTransfer(srcObj)
			return subDirs, err
		}

		subDirs = append(subDirs, minimalStoredObject{
			relativePath:           common.AZCOPY_PATH_SEPARATOR_STRING + srcObj.relativePath,
			changeTime:             srcObj.changeTime,
			isVirtualPrefix:        srcObj.isVirtualPrefix,
			isPresentAtDestination: false, // not at destination
		})

		// Schedule folder transfer (for ACLs, metadata, etc.)
		err := enumerator.ctp.scheduleCopyTransfer(srcObj)
		if err != nil {
			return subDirs, err
		}

		return subDirs, nil
	}

	// File: schedule transfer
	err := enumerator.ctp.scheduleCopyTransfer(srcObj)
	return subDirs, err
}

// mergeJoinHandleDestOnly processes an object that exists at the destination but
// not the source. Behavior depends on the --delete-destination flag:
//   - True: pass to destination cleaner for deletion
//   - False/Prompt: log and skip
func mergeJoinHandleDestOnly(
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	dstObj StoredObject,
) error {
	if cca.deleteDestination == common.EDeleteDestination.True() {
		// Pass to the destination cleaner (same function used by the indexMap path)
		return enumerator.objectComparator(dstObj)
	}

	// Not deleting destination — this object is extra but we leave it alone
	return nil
}

// mergeJoinHandleBothExist processes an object that exists at both source and destination.
// It compares properties (size, LWT, changeTime) to determine if a transfer is needed.
// Folders are always added to subdirectories for recursive traversal.
// This version creates full StoredObjects and delegates to processIfNecessaryWithOrchestrator
// for parity with the indexMap path.
func mergeJoinHandleBothExist(
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	comparator *syncDestinationComparator,
	srcObj StoredObject,
	dstObj StoredObject,
	originalRelativePath string,
	subDirs []minimalStoredObject,
) ([]minimalStoredObject, error) {

	if srcObj.entityType == common.EEntityType.Folder() {
		// Skip self-referential directory sentinels (GCP/HNS) from subDirs
		if isSelfReferentialDirSentinel(srcObj, originalRelativePath, cca.fromTo) {
			// Always schedule transfer for ACL propagation (matches indexMap path behavior
			// where the root sentinel with BlobName="" routes through SetContainerACL).
			// Do NOT re-enqueue as subdirectory — that would cause infinite loops.
			err := enumerator.ctp.scheduleCopyTransfer(srcObj)
			return subDirs, err
		}

		subDirs = append(subDirs, minimalStoredObject{
			relativePath:           common.AZCOPY_PATH_SEPARATOR_STRING + srcObj.relativePath,
			changeTime:             srcObj.changeTime,
			isVirtualPrefix:        srcObj.isVirtualPrefix,
			isPresentAtDestination: true,
		})
	}

	// For remote sources (Blob, S3, BlobFS, GCP), use isMoreRecentThan on LMT.
	// This mirrors the default comparison in processIfNecessary (indexMap path).
	// LWT/changeTime are not reliable for remote sources (e.g., blob Last-Modified
	// is always updated to copy time, and POSIX metadata keys may not exist).
	if srcObj.isMoreRecentThan(dstObj, comparator.preferSMBTime) {
		syncComparatorLog(srcObj.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
		err := enumerator.ctp.scheduleCopyTransfer(srcObj)
		return subDirs, err
	}

	// No change — skip transfer
	if enumerator.primaryTraverserTemplate.options.IncrementNotTransferred != nil && !srcObj.isVirtualPrefix {
		enumerator.primaryTraverserTemplate.options.IncrementNotTransferred(srcObj.entityType)
	}

	syncComparatorLog(srcObj.relativePath, syncStatusSkipped, syncSkipReasonNoChangeInLWTorCT, false)
	return subDirs, nil
}

// mergeJoinHandleBothExistPageLevel compares source and destination items directly
// from the raw SDK page data — ZERO StoredObject allocation for the match case.
//
// Quick check: if size matches AND source LMT <= destination LMT → skip (no alloc).
// If they differ, fall through to full comparison via StoredObject creation.
func mergeJoinHandleBothExistPageLevel(
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	dir string,
	src *pageCursor,
	dst *pageCursor,
	srcRelPath string,
	srcPath string,
	srcContainerName string,
	dstContainerName string,
	subDirs []minimalStoredObject,
) ([]minimalStoredObject, error) {

	srcEntityType := src.itemEntityType()
	dstEntityType := dst.itemEntityType()
	srcIsVirtualPrefix := src.itemIsVirtualPrefix()

	// Folders: always add to subDirs for recursive traversal
	if srcEntityType == common.EEntityType.Folder() {
		// Check for self-referential sentinel
		if !isSelfReferentialDirSentinelFromCursor(srcRelPath, srcIsVirtualPrefix, cca.fromTo) {
			subDirs = append(subDirs, minimalStoredObject{
				relativePath:           common.AZCOPY_PATH_SEPARATOR_STRING + srcPath,
				changeTime:             src.lmt(), // use LMT as changeTime for folders
				isVirtualPrefix:        srcIsVirtualPrefix,
				isPresentAtDestination: dstEntityType == common.EEntityType.Folder(),
			})
		}

		// For folders that are virtual prefixes, no transfer is needed
		if srcIsVirtualPrefix {
			return subDirs, nil
		}

		// Non-virtual folder (e.g., HNS folder blob) — create StoredObject and schedule
		srcObj := src.toStoredObject(srcContainerName)
		srcObj.relativePath = srcPath
		srcObj.isVirtualPrefix = srcIsVirtualPrefix
		err := enumerator.ctp.scheduleCopyTransfer(srcObj)
		return subDirs, err
	}

	// Files: fast path — compare size + LMT directly from page data (ZERO alloc)
	if srcEntityType == dstEntityType {
		srcSize := src.itemSize()
		dstSize := dst.itemSize()
		srcLMT := src.lmt()
		dstLMT := dst.lmt()

		// Quick match: same entity type, same size, source not newer → skip
		if srcSize == dstSize && !srcLMT.After(dstLMT) {
			// No transfer needed — zero allocation!
			if enumerator.primaryTraverserTemplate.options.IncrementNotTransferred != nil {
				enumerator.primaryTraverserTemplate.options.IncrementNotTransferred(srcEntityType)
			}
			return subDirs, nil
		}
	}

	// Slow path: something differs — create full StoredObjects for detailed comparison
	srcObj := src.toStoredObject(srcContainerName)
	srcObj.relativePath = srcPath
	srcObj.isVirtualPrefix = srcIsVirtualPrefix

	dstObj := dst.toStoredObject(dstContainerName)
	dstObj.relativePath = buildChildPath(dir, dst.name(), dstEntityType == common.EEntityType.Folder())

	comparator := &syncDestinationComparator{
		sourceIndex:             enumerator.objectIndexer,
		copyTransferScheduler:   enumerator.ctp.scheduleCopyTransfer,
		destinationCleaner:      enumerator.objectComparator,
		deleteDestination:       cca.deleteDestination,
		incrementNotTransferred: enumerator.primaryTraverserTemplate.options.IncrementNotTransferred,
		orchestratorOptions:     enumerator.orchestratorOptions,
	}

	return mergeJoinHandleBothExist(enumerator, cca, comparator, srcObj, dstObj, srcRelPath, subDirs)
}

// isSelfReferentialDirSentinelFromCursor is a lighter version of isSelfReferentialDirSentinel
// that works with cursor data (no StoredObject needed).
func isSelfReferentialDirSentinelFromCursor(relPath string, isVirtualPrefix bool, fromTo common.FromTo) bool {
	if relPath != "" {
		return false
	}
	// GCP S3-compatible source emits directory placeholders with empty relativePath
	if isGCPSource {
		return true
	}
	// BlobFS (HNS) traverser emits the current directory as a folder with empty relativePath
	if fromTo.From() == common.ELocation.BlobFS() {
		return true
	}
	return false
}

// mergeJoinSyncOneDirLog logs messages specific to the merge-join sync path.
// Set toConsole=true to also emit to stdout (visible in LAW container logs).
func mergeJoinSyncOneDirLog(level common.LogLevel, msg string, toConsole ...bool) {
	console := false
	if len(toConsole) > 0 {
		console = toConsole[0]
	}
	syncOrchestratorLog(level, fmt.Sprintf("[MergeJoin] %s", msg), console)
}

// mergeJoinElapsedSinceLog logs elapsed time for a merge-join directory operation.
// Only logs directories that took longer than 5 seconds to reduce noise.
func mergeJoinElapsedSinceLog(dir string, start time.Time) {
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		mergeJoinSyncOneDirLog(
			common.LogInfo,
			fmt.Sprintf("Dir '%s' completed in %v", dir, elapsed))
	}
}
