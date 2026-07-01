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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/minio/minio-go/v7"

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

// useBatchedChannelMergeJoin returns true when AZCOPY_USE_BATCHED_CHANNEL_MERGE_JOIN is
// "true" or "1". When set, the channel-based merge-join bridges traversers using batched
// channels (one []StoredObject slice per mergeJoinBatchSize items) instead of one object
// per channel op. Default false (one object per channel op).
func useBatchedChannelMergeJoin() bool {
	v := strings.ToLower(strings.TrimSpace(
		common.GetEnvironmentVariable(common.EEnvironmentVariable.UseBatchedChannelMergeJoin())))
	return v == "true" || v == "1"
}

// mergeJoinBatchSize is the number of StoredObjects accumulated into each slice sent over
// the batched channel. Larger batches amortize the channel send/recv cost (mutex + possible
// goroutine wakeup) across more items; sized near one ListBlobs page so a batch corresponds
// to roughly one network round-trip of listing.
const mergeJoinBatchSize = 1_000

// mergeJoinBatchChannelBufferSize is the number of in-flight batches buffered on the batched
// channel. With mergeJoinBatchSize=1000 this buffers up to 8000 objects, letting the consumer
// keep draining while the producer fetches the next listing page.
const mergeJoinBatchChannelBufferSize = 8

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

// traverserToBatchedChannel is the batched analogue of traverserToChannel. It accumulates
// callback objects into []StoredObject slices and sends one slice per mergeJoinBatchSize
// items (plus a final partial slice). This collapses the per-item channel send/recv overhead
// (mutex + scheduler wakeup) of the channel-based path into one op per batch, while preserving
// full StoredObject fidelity (metadata/properties are carried unchanged). The producer goroutine
// blocks when the batch channel buffer is full, providing natural back-pressure.
func traverserToBatchedChannel(
	ctx context.Context,
	t ResourceTraverser,
	filters []ObjectFilter,
	label string,
) (<-chan []StoredObject, <-chan error) {

	batchCh := make(chan []StoredObject, mergeJoinBatchChannelBufferSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(batchCh)
		defer close(errCh)

		batch := make([]StoredObject, 0, mergeJoinBatchSize)
		flush := func() bool {
			if len(batch) == 0 {
				return true
			}
			select {
			case batchCh <- batch:
			case <-ctx.Done():
				return false
			}
			batch = make([]StoredObject, 0, mergeJoinBatchSize)
			return true
		}

		count := 0
		err := t.Traverse(noPreProccessor, func(obj StoredObject) error {
			batch = append(batch, obj)
			count++
			if len(batch) >= mergeJoinBatchSize {
				if !flush() {
					return ctx.Err()
				}
			}
			return nil
		}, filters)

		flush() // flush the final partial batch

		if err != nil {
			mergeJoinSyncOneDirLog(common.LogError,
				fmt.Sprintf("%s traversal error after %d objects: %v", label, count, err), true)
			errCh <- err
		}
	}()

	return batchCh, errCh
}

// batchedReceiver pulls individual StoredObjects from a batched channel, yielding one at a
// time so the merge loop can stay item-oriented. recv() returns (obj, false) once the channel
// is closed and the current batch is exhausted.
type batchedReceiver struct {
	ch    <-chan []StoredObject
	batch []StoredObject
	idx   int
}

func (r *batchedReceiver) recv() (StoredObject, bool) {
	for r.idx >= len(r.batch) {
		b, ok := <-r.ch
		if !ok {
			return StoredObject{}, false
		}
		r.batch = b
		r.idx = 0
	}
	obj := r.batch[r.idx]
	r.idx++
	return obj, true
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

	// Bridge both traversers into channels. srcNext/dstNext yield one object at a time,
	// abstracting over the single-item (default) and batched (AZCOPY_USE_BATCHED_CHANNEL_MERGE_JOIN)
	// producers so the merge loop below is identical for both.
	srcLabel := fmt.Sprintf("SRC[%s]", dir)
	dstLabel := fmt.Sprintf("DST[%s]", dir)

	var srcNext, dstNext func() (StoredObject, bool)
	var srcErrCh, dstErrCh <-chan error

	closedErrCh := func() <-chan error {
		ec := make(chan error)
		close(ec)
		return ec
	}

	if useBatchedChannelMergeJoin() {
		var srcCh <-chan []StoredObject
		srcCh, srcErrCh = traverserToBatchedChannel(ctx, pt, enumerator.filters, srcLabel)
		srcRecv := &batchedReceiver{ch: srcCh}
		srcNext = srcRecv.recv
		if isDestinationPresent {
			var dstCh <-chan []StoredObject
			dstCh, dstErrCh = traverserToBatchedChannel(ctx, st, enumerator.filters, dstLabel)
			dstRecv := &batchedReceiver{ch: dstCh}
			dstNext = dstRecv.recv
		} else {
			dstNext = func() (StoredObject, bool) { return StoredObject{}, false }
			dstErrCh = closedErrCh()
		}
	} else {
		var srcCh <-chan StoredObject
		srcCh, srcErrCh = traverserToChannel(ctx, pt, enumerator.filters, srcLabel)
		srcNext = func() (StoredObject, bool) { return mergeJoinRecv(srcCh) }
		if isDestinationPresent {
			var dstCh <-chan StoredObject
			dstCh, dstErrCh = traverserToChannel(ctx, st, enumerator.filters, dstLabel)
			dstNext = func() (StoredObject, bool) { return mergeJoinRecv(dstCh) }
		} else {
			dstNext = func() (StoredObject, bool) { return StoredObject{}, false }
			dstErrCh = closedErrCh()
		}
	}

	// Build the comparator for property comparison (size, LWT, changeTime)
	comparator := &syncDestinationComparator{
		sourceIndex:             enumerator.objectIndexer,
		copyTransferScheduler:   enumerator.ctp.scheduleCopyTransfer,
		destinationCleaner:      enumerator.objectComparator,
		deleteDestination:       cca.deleteDestination,
		preferSMBTime:           cca.preserveInfo,
		incrementNotTransferred: enumerator.primaryTraverserTemplate.options.IncrementNotTransferred,
		orchestratorOptions:     enumerator.orchestratorOptions,
	}

	// Pull first object from each side
	srcObj, srcOk := srcNext()
	dstObj, dstOk := dstNext()

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
			srcObj, srcOk = srcNext()

		case cmp > 0:
			dstObj.relativePath = dstPath
			err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
			if err != nil {
				return subDirs, err
			}
			dstObj, dstOk = dstNext()

		default:
			srcObj.relativePath = srcPath
			dstObj.relativePath = dstPath
			subDirs, err = mergeJoinHandleBothExist(enumerator, cca, comparator, srcObj, dstObj, srcOrigPath, subDirs)
			if err != nil {
				return subDirs, err
			}
			srcObj, srcOk = srcNext()
			dstObj, dstOk = dstNext()
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
		srcObj, srcOk = srcNext()
	}

	// Drain remaining dest-only objects
	for dstOk {
		dstObj.relativePath = buildChildPath(dir, dstObj.relativePath, dstObj.entityType == common.EEntityType.Folder())
		err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
		if err != nil {
			return subDirs, err
		}
		dstObj, dstOk = dstNext()
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
	srcLocation        common.Location
	srcContainerClient *container.Client // blob / blobfs source
	srcS3Core          *minio.Core       // s3 source
	srcShareClient     *share.Client     // azure files source
	srcContainerName   string            // container / bucket / share name

	dstLocation        common.Location
	dstContainerClient *container.Client // blob / blobfs destination (always for c2c)
	dstS3Core          *minio.Core       // unused today (dest is never S3), kept symmetric
	dstShareClient     *share.Client     // azure files destination
	dstContainerName   string

	includeTags bool
}

// startSrcPrefetch launches the source listing prefetch goroutine appropriate for
// the source location (S3 vs Azure Files vs Blob/BlobFS) and returns the page/error channels.
func (mjCtx *mergeJoinContext) startSrcPrefetch(ctx context.Context, searchPrefix string) (<-chan rawPage, <-chan error) {
	switch mjCtx.srcLocation {
	case common.ELocation.S3():
		return startS3Prefetch(ctx, mjCtx.srcS3Core, mjCtx.srcContainerName, searchPrefix)
	case common.ELocation.File():
		return startFileSharePrefetch(ctx, mjCtx.srcShareClient, searchPrefix)
	}
	return startBlobHierarchyPrefetch(ctx, mjCtx.srcContainerClient, searchPrefix, mjCtx.includeTags, mjCtx.srcLocation == common.ELocation.BlobFS())
}

// startDstPrefetch launches the destination listing prefetch goroutine. The c2c
// destination is always Blob/BlobFS or Azure Files; the S3 branch is kept for symmetry.
func (mjCtx *mergeJoinContext) startDstPrefetch(ctx context.Context, searchPrefix string) (<-chan rawPage, <-chan error) {
	switch mjCtx.dstLocation {
	case common.ELocation.S3():
		return startS3Prefetch(ctx, mjCtx.dstS3Core, mjCtx.dstContainerName, searchPrefix)
	case common.ELocation.File():
		return startFileSharePrefetch(ctx, mjCtx.dstShareClient, searchPrefix)
	}
	return startBlobHierarchyPrefetch(ctx, mjCtx.dstContainerClient, searchPrefix, false, mjCtx.dstLocation == common.ELocation.BlobFS())
}

// newMergeJoinContext creates the shared source/destination listing clients.
// Called once before the crawl loop starts. The source may be S3, Azure Files, Blob,
// or BlobFS; the destination is Blob/BlobFS or Azure Files for cloud-to-cloud sync.
func newMergeJoinContext(ctx context.Context, enumerator *syncEnumerator, srcBaseURL, dstBaseURL string) (*mergeJoinContext, error) {
	ptt := enumerator.primaryTraverserTemplate
	stt := enumerator.secondaryTraverserTemplate

	mjCtx := &mergeJoinContext{
		srcLocation: ptt.location,
		dstLocation: stt.location,
		includeTags: ptt.options.PreserveBlobTags,
	}

	// ── Source listing client ──
	switch ptt.location {
	case common.ELocation.S3():
		core, bucket, err := createS3CoreAndBucket(ctx, srcBaseURL, &ptt.options)
		if err != nil {
			return nil, fmt.Errorf("failed to create source S3 client: %w", err)
		}
		mjCtx.srcS3Core = core
		mjCtx.srcContainerName = bucket
	case common.ELocation.File():
		srcSC, srcSN, _, err := getShareClientAndPrefix(srcBaseURL, &ptt.options)
		if err != nil {
			return nil, fmt.Errorf("failed to create source share client: %w", err)
		}
		mjCtx.srcShareClient = srcSC
		mjCtx.srcContainerName = srcSN
	default:
		srcCC, srcCN, _, err := getContainerClientAndPrefix(srcBaseURL, &ptt.options)
		if err != nil {
			return nil, fmt.Errorf("failed to create source container client: %w", err)
		}
		mjCtx.srcContainerClient = srcCC
		mjCtx.srcContainerName = srcCN
	}

	// ── Destination listing client (Blob/BlobFS or Azure Files) ──
	if stt.location == common.ELocation.File() {
		dstSC, dstSN, _, err := getShareClientAndPrefix(dstBaseURL, &stt.options)
		if err != nil {
			return nil, fmt.Errorf("failed to create destination share client: %w", err)
		}
		mjCtx.dstShareClient = dstSC
		mjCtx.dstContainerName = dstSN
	} else {
		dstCC, dstCN, _, err := getContainerClientAndPrefix(dstBaseURL, &stt.options)
		if err != nil {
			return nil, fmt.Errorf("failed to create destination container client: %w", err)
		}
		mjCtx.dstContainerClient = dstCC
		mjCtx.dstContainerName = dstCN
	}

	return mjCtx, nil
}

// searchPrefixForURLByLocation extracts the listing prefix (trailing slash) for the
// given resource URL, dispatching by location (S3 keys vs Azure Files paths vs blob names).
func searchPrefixForURLByLocation(rawURL string, loc common.Location) (string, error) {
	switch loc {
	case common.ELocation.S3():
		return s3SearchPrefix(rawURL)
	case common.ELocation.File():
		return fileSearchPrefix(rawURL)
	}
	return searchPrefixForURL(rawURL)
}

// fileSearchPrefix extracts the directory/file path from an Azure Files URL and
// ensures it has a trailing slash.
func fileSearchPrefix(rawURL string) (string, error) {
	_, dirOrFilePath, err := parseFileURL(rawURL)
	if err != nil {
		return "", err
	}
	if dirOrFilePath != "" && !strings.HasSuffix(dirOrFilePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
		dirOrFilePath += common.AZCOPY_PATH_SEPARATOR_STRING
	}
	return dirOrFilePath, nil
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
	case common.ELocation.S3(), common.ELocation.Blob(), common.ELocation.BlobFS(), common.ELocation.File():
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
	srcSearchPrefix, err := searchPrefixForURLByLocation(srcURL, mjCtx.srcLocation)
	if err != nil {
		return subDirs, fmt.Errorf("failed to parse source URL for dir '%s': %w", dir, err)
	}

	var dstPageCh <-chan rawPage
	var dstErrCh <-chan error
	var dstSearchPrefix string

	if isDestinationPresent {
		var dstErr error
		dstSearchPrefix, dstErr = searchPrefixForURLByLocation(dstURL, mjCtx.dstLocation)
		if dstErr != nil {
			return subDirs, fmt.Errorf("failed to parse destination URL for dir '%s': %w", dir, dstErr)
		}
		// Start dst prefetch BEFORE blocking on src — both listings run in parallel
		dstPageCh, dstErrCh = mjCtx.startDstPrefetch(ctx, dstSearchPrefix)
	}

	// Start src prefetch (goroutine launched immediately, non-blocking)
	srcPageCh, srcErrCh := mjCtx.startSrcPrefetch(ctx, srcSearchPrefix)

	// Now block on first pages — both HTTP requests are already in-flight
	src := newPageCursor(srcPageCh, srcErrCh)

	var dst *pageCursor
	if isDestinationPresent {
		dst = newPageCursor(dstPageCh, dstErrCh)
	} else {
		dst = newEmptyCursor()
	}

	// Top-level root folder emission for a BlobFS (HNS) source. A blob-hierarchy listing of
	// the crawl root's CONTENTS never includes the root folder itself, so we emit it here.
	// relativePath is kept empty so the transfer maps the crawl root -> the destination
	// crawl root (source subpath root -> target subpath root, or container -> container).
	//
	//   - Full container (srcSearchPrefix == ""): emit only for HNS->HNS, where the
	//     container-root ACL is propagated via SetContainerACL (destination BlobName == "").
	//     Flat-blob destinations have no container folder to create.
	//
	//   - Subpath (srcSearchPrefix != ""): the source's own subpath directory is a real
	//     folder (dir stub) with its own ACL. It must be recreated on the destination for
	//     every destination type — HNS->HNS to carry its ACL, HNS->FNS as a folder stub —
	//     so its Scanned/Transferred counts and ACL match the source.
	//
	// scheduleCopyTransfer still applies the folder property option, so cca.includeRoot must
	// be set (Fpo=AllFolders) for the empty-relativePath root to survive the
	// AllFoldersExceptRoot filter; the mover enables includeRoot for every HNS source.
	if (dir == "" || dir == common.AZCOPY_PATH_SEPARATOR_STRING) &&
		mjCtx.srcLocation == common.ELocation.BlobFS() {

		emitTopLevelRoot := false
		if srcSearchPrefix == "" {
			// Full container root: only HNS->HNS can land the container ACL.
			emitTopLevelRoot = cca.includeRoot && mjCtx.dstLocation == common.ELocation.BlobFS()
		} else {
			// Subpath root: recreate the source's own subpath folder on any destination.
			emitTopLevelRoot = cca.includeRoot
		}

		if emitTopLevelRoot {
			rootFolder := newStoredObject(
				nil,
				"",
				"",
				common.EEntityType.Folder(),
				time.Now(),
				0,
				noBlobProps,
				noBlobProps,
				nil,
				srcContainerName,
			)
			incrementSourceScanned(cca, common.EEntityType.Folder(), false)
			if schedErr := enumerator.ctp.scheduleCopyTransfer(rootFolder); schedErr != nil {
				return subDirs, schedErr
			}
		}
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
				ctx, mjCtx, srcSearchPrefix, dstSearchPrefix,
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

		// Virtual prefixes (FNS directories) have no folder entity to transfer —
		// recurse only. Real folders (HNS) are scheduled for transfer.
		if srcObj.isVirtualPrefix {
			return subDirs, nil
		}

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

// fetchFolderLMT retrieves the real last-modified time of a folder via GetProperties.
// On an HNS account a directory is a real entity that only appears as a zero-LMT
// BlobPrefix in a hierarchy listing; its true LMT must be fetched directly. This
// mirrors the blob traverser's DirStubs branch (zc_traverser_blob.go) which probes
// "<name>" and then "<name>/" for the folder. fullFolderPath is the container-relative
// folder path (search-prefix + child name). Returns (zero, false) when the folder
// has no directory entity (e.g. an FNS virtual prefix whose marker was deleted).
func fetchFolderLMT(ctx context.Context, containerClient *container.Client, fullFolderPath string) (time.Time, bool) {
	name := strings.TrimSuffix(fullFolderPath, common.AZCOPY_PATH_SEPARATOR_STRING)

	resp, err := containerClient.NewBlobClient(name).GetProperties(ctx, nil)
	if err != nil {
		// "foo" did not exist as a directory blob — try "foo/".
		resp, err = containerClient.NewBlobClient(name+common.AZCOPY_PATH_SEPARATOR_STRING).GetProperties(ctx, nil)
		if err != nil {
			return time.Time{}, false
		}
	}
	if resp.LastModified == nil {
		return time.Time{}, false
	}
	return resp.LastModified.UTC(), true
}

// mergeJoinScalarOutcome enumerates the page-level comparison decision for a
// matched, same-entity-type source/destination pair.
type mergeJoinScalarOutcome int

const (
	mergeJoinSkip         mergeJoinScalarOutcome = iota // unchanged → NoTransferNeeded
	mergeJoinTransferData                               // data changed → full transfer
	mergeJoinTransferMeta                               // metadata-only change → properties transfer
)

// mergeJoinCompareScalar mirrors syncDestinationComparator.compareSourceAndDestinationObject
// (the indexMap orchestrator comparator) operating purely on scalar fields pulled from the
// listing pages — NO StoredObject allocation. It is the page-level path's self-contained
// parity with the orchestrator comparison; the two are deliberately NOT unified (so the
// indexMap code stays untouched), so any change to compareSourceAndDestinationObject must be
// mirrored here.
//
// opts carries the orchestrator options (metaDataOnlySync, ctime optimization, last
// successful sync start). When opts is nil/metadata-only is off, an LWT match means skip.
func mergeJoinCompareScalar(
	entityType common.EntityType,
	srcSize, dstSize int64,
	srcLWT, dstLWT, srcCT, dstCT time.Time,
	opts *SyncOrchestratorOptions,
) mergeJoinScalarOutcome {
	isFolder := entityType == common.EEntityType.Folder()

	// Data change: size (skipped for folders, matching the orchestrator).
	if !isFolder && srcSize != dstSize {
		return mergeJoinTransferData
	}

	// Data change: last-write-time. A zero on either side means we can't compare, so
	// assume changed (orchestrator returns dataChanged=true here).
	if srcLWT.IsZero() || dstLWT.IsZero() {
		return mergeJoinTransferData
	}
	if srcLWT.Compare(dstLWT) != 0 {
		return mergeJoinTransferData
	}

	// LWT matches. Without metadata-only sync the orchestrator treats metadata as
	// unchanged → skip.
	if opts == nil || !opts.metaDataOnlySync {
		return mergeJoinSkip
	}

	if isNFSCopy {
		// ChangeTime is unreliable on an NFS target (set to migration time). Prefer the
		// last successful sync start when the ctime optimization is enabled.
		if opts.optimizeEnumerationByCTime && !opts.lastSuccessfulSyncJobStartTime.IsZero() {
			if srcCT.IsZero() {
				return mergeJoinTransferMeta
			}
			if srcCT.After(opts.lastSuccessfulSyncJobStartTime) {
				return mergeJoinTransferMeta
			}
			return mergeJoinSkip
		}
		return mergeJoinTransferMeta
	}

	// Non-NFS: reliable change time on the target.
	if srcCT.IsZero() && dstCT.IsZero() {
		return mergeJoinSkip
	}
	if srcCT.IsZero() || dstCT.IsZero() {
		return mergeJoinTransferMeta
	}
	if srcCT.Compare(dstCT) != 0 {
		return mergeJoinTransferMeta
	}
	return mergeJoinSkip
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
	ctx context.Context,
	mjCtx *mergeJoinContext,
	srcSearchPrefix string,
	dstSearchPrefix string,
	subDirs []minimalStoredObject,
) ([]minimalStoredObject, error) {

	srcEntityType := src.itemEntityType()
	dstEntityType := dst.itemEntityType()
	srcIsVirtualPrefix := src.itemIsVirtualPrefix()

	// ── Entity-type change at the same path ──
	// Source and destination share a name but differ in kind (e.g. a file replaced by
	// a folder, or vice-versa). Mirror processIfNecessaryWithOrchestrator: STE replaces
	// a non-folder destination on its own, but a destination FOLDER must be deleted
	// first — and recursively, since STE only removes empty folders — when
	// --delete-destination is enabled. Once the destination is clear the source has no
	// counterpart there, so we hand it to the source-only handler (which schedules the
	// transfer and, for a folder, enqueues it for recursive traversal).
	if srcEntityType != dstEntityType {
		if dstEntityType == common.EEntityType.Folder() && cca.deleteDestination == common.EDeleteDestination.True() {
			dstObj := dst.toStoredObject(dstContainerName)
			dstObj.relativePath = buildChildPath(dir, dst.name(), true)
			if delErr := mergeJoinHandleDestOnly(enumerator, cca, dstObj); delErr != nil {
				// Tolerable: keep the extra destination folder and skip the source this
				// run rather than transferring onto an incompatible destination.
				syncComparatorLog(srcPath, syncStatusSkipped, syncSkipReasonEntityTypeChangedFailedDelete, false)
				if inc := enumerator.primaryTraverserTemplate.options.IncrementNotTransferred; inc != nil {
					inc(srcEntityType)
				}
				return subDirs, nil
			}
		}

		srcObj := src.toStoredObject(srcContainerName)
		srcObj.relativePath = srcPath
		srcObj.isVirtualPrefix = srcIsVirtualPrefix
		return mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcRelPath, subDirs)
	}

	// Folders: always add to subDirs for recursive traversal
	if srcEntityType == common.EEntityType.Folder() {
		// Virtual prefix (FNS directory): recurse only — no folder entity to
		// transfer, no counting (handled by the caller's incrementSourceScanned).
		if srcIsVirtualPrefix {
			if !isSelfReferentialDirSentinelFromCursor(srcRelPath, srcIsVirtualPrefix, cca.fromTo) {
				subDirs = append(subDirs, minimalStoredObject{
					relativePath:           common.AZCOPY_PATH_SEPARATOR_STRING + srcPath,
					changeTime:             src.lmt(),
					isVirtualPrefix:        true,
					isPresentAtDestination: dstEntityType == common.EEntityType.Folder(),
				})
			}
			return subDirs, nil
		}

		// Non-virtual folder (HNS) present at both source and destination: delegate
		// to the shared both-exist handler, which enqueues the subdirectory for
		// recursion AND compares last-modified to decide transfer vs NoTransferNeeded.
		// This ensures a resync of an unchanged folder is counted as NoTransferNeeded
		// rather than re-transferred.
		srcObj := src.toStoredObject(srcContainerName)
		srcObj.relativePath = srcPath
		srcObj.isVirtualPrefix = false

		// SMB-aware folders (Azure Files) carry real last-write-time and change-time
		// from the listing, so apply the orchestrator scalar comparison directly — no
		// per-folder GetProperties round-trip needed. Enqueue for recursion, then
		// transfer the folder entity only when its properties changed (a folder
		// metadata-only change is a full folder transfer, matching the orchestrator).
		if cca.preserveInfo {
			if isSelfReferentialDirSentinel(srcObj, srcRelPath, cca.fromTo) {
				// Self folder: schedule for ACL propagation, do not recurse.
				return subDirs, enumerator.ctp.scheduleCopyTransfer(srcObj)
			}
			subDirs = append(subDirs, minimalStoredObject{
				relativePath:           common.AZCOPY_PATH_SEPARATOR_STRING + srcObj.relativePath,
				changeTime:             srcObj.changeTime,
				isVirtualPrefix:        false,
				isPresentAtDestination: true,
			})
			switch mergeJoinCompareScalar(
				srcEntityType,
				src.itemSize(), dst.itemSize(),
				src.lwt(), dst.lwt(),
				src.ct(), dst.ct(),
				enumerator.orchestratorOptions,
			) {
			case mergeJoinSkip:
				if inc := enumerator.primaryTraverserTemplate.options.IncrementNotTransferred; inc != nil {
					inc(srcEntityType)
				}
				return subDirs, nil
			default: // folder data or metadata changed → transfer the full folder
				return subDirs, enumerator.ctp.scheduleCopyTransfer(srcObj)
			}
		}

		dstObj := dst.toStoredObject(dstContainerName)
		dstObj.relativePath = buildChildPath(dir, dst.name(), dstEntityType == common.EEntityType.Folder())

		// On an HNS account a directory surfaces only as a zero-LMT BlobPrefix in a
		// hierarchy listing, so neither the source nor the destination folder carries a
		// usable last-modified time from the page data. The channel-based reference and
		// the production blob traverser both fetch the real folder LMT via GetProperties
		// (the DirStubs branch in zc_traverser_blob.go). Without it, isMoreRecentThan
		// would compare two zero times (folder wrongly skipped) or a real source LMT
		// against a zero destination LMT (folder wrongly re-transferred). We mirror the
		// traverser here, fetching the real LMT for whichever side is missing it. The
		// cost is paid per folder only (a tiny fraction of objects), matching the
		// traverser, and keeps the file fast-path allocation-free.
		if srcObj.lastModifiedTime.IsZero() && mjCtx.srcContainerClient != nil {
			if lmt, ok := fetchFolderLMT(ctx, mjCtx.srcContainerClient, srcSearchPrefix+src.name()); ok {
				srcObj.lastModifiedTime = lmt
			}
		}
		if dstObj.lastModifiedTime.IsZero() && mjCtx.dstContainerClient != nil {
			if lmt, ok := fetchFolderLMT(ctx, mjCtx.dstContainerClient, dstSearchPrefix+dst.name()); ok {
				dstObj.lastModifiedTime = lmt
			}
		}

		comparator := &syncDestinationComparator{
			sourceIndex:             enumerator.objectIndexer,
			copyTransferScheduler:   enumerator.ctp.scheduleCopyTransfer,
			destinationCleaner:      enumerator.objectComparator,
			deleteDestination:       cca.deleteDestination,
			preferSMBTime:           cca.preserveInfo,
			incrementNotTransferred: enumerator.primaryTraverserTemplate.options.IncrementNotTransferred,
			orchestratorOptions:     enumerator.orchestratorOptions,
		}

		return mergeJoinHandleBothExist(enumerator, cca, comparator, srcObj, dstObj, srcRelPath, subDirs)
	}

	// Files (same entity type): compare directly from page data — ZERO allocation
	// on the no-change path.
	if srcEntityType == dstEntityType {
		// SMB-aware (preserveInfo) source/target carry meaningful last-write-time and
		// change-time, so apply the full orchestrator comparison: mergeJoinCompareScalar
		// mirrors compareSourceAndDestinationObject (bidirectional LWT + metadata-only
		// change detection via change-time). A StoredObject is allocated only when a
		// transfer is actually scheduled (<1% of items).
		if cca.preserveInfo {
			switch mergeJoinCompareScalar(
				srcEntityType,
				src.itemSize(), dst.itemSize(),
				src.lwt(), dst.lwt(),
				src.ct(), dst.ct(),
				enumerator.orchestratorOptions,
			) {
			case mergeJoinSkip:
				// No transfer needed — zero allocation.
				if inc := enumerator.primaryTraverserTemplate.options.IncrementNotTransferred; inc != nil {
					inc(srcEntityType)
				}
				return subDirs, nil

			case mergeJoinTransferMeta:
				// Metadata-only change: transfer file properties without data, mirroring
				// processIfNecessaryWithOrchestrator (size=0, entityType=FileProperties).
				srcObj := src.toStoredObject(srcContainerName)
				srcObj.relativePath = srcPath
				srcObj.isVirtualPrefix = srcIsVirtualPrefix
				srcObj.size = 0
				srcObj.entityType = common.EEntityType.FileProperties()
				return subDirs, enumerator.ctp.scheduleCopyTransfer(srcObj)

			default: // mergeJoinTransferData
				srcObj := src.toStoredObject(srcContainerName)
				srcObj.relativePath = srcPath
				srcObj.isVirtualPrefix = srcIsVirtualPrefix
				return subDirs, enumerator.ctp.scheduleCopyTransfer(srcObj)
			}
		}

		// Non-SMB (blob/S3) entries carry a zero LWT/CT, so keep the last-modified
		// one-directional comparison (matches the indexMap isMoreRecentThan path for
		// non-Local sources). Quick match: same size, source not newer → skip.
		if src.itemSize() == dst.itemSize() && !src.lmt().After(dst.lmt()) {
			// No transfer needed — zero allocation!
			if inc := enumerator.primaryTraverserTemplate.options.IncrementNotTransferred; inc != nil {
				inc(srcEntityType)
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
		preferSMBTime:           cca.preserveInfo,
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
