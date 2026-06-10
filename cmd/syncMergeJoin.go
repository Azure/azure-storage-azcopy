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

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// activeMergeJoinDirs tracks how many mergeJoinSyncDir calls are in-flight.
var activeMergeJoinDirs atomic.Int64

// mergeJoinChannelBufferSize controls the buffer size of channels used to bridge
// push-based traversers into pull-based iteration for the merge-join algorithm.
// A larger buffer reduces goroutine context switches but uses more memory.
// Each slot holds one StoredObject (~430 bytes), so 100 slots ≈ 43KB per channel.
// With 500 workers × 2 channels, total buffer memory ≈ 43MB.
const mergeJoinChannelBufferSize = 100

// traverserResult wraps a StoredObject emitted by a traverser goroutine.
// When done is true, the channel has been drained and obj is invalid.
type traverserResult struct {
	obj StoredObject
	err error
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
			select {
			case objCh <- obj:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, filters)

		if firstObj {
			// Traverser returned 0 objects — log how long it took
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

// mergeJoinSyncDir performs a streaming merge-join of source and destination
// object listings for a single directory. Both sides must emit objects in
// lexicographic order (guaranteed by all remote storage APIs: S3, Blob, BlobFS).
//
// The algorithm walks both sorted streams in lockstep:
//   - srcPath < dstPath → source-only object: schedule transfer (new file)
//   - srcPath > dstPath → dest-only object: handle via delete-destination policy
//   - srcPath == dstPath → both exist: compare properties, transfer if stale
//
// This replaces the indexMap-based processor/comparator/finalize flow for remote
// sources, eliminating O(N) memory usage in favor of O(1) streaming.
func mergeJoinSyncDir(
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
		// Destination doesn't exist yet — all source objects are new
		emptyCh := make(chan StoredObject)
		close(emptyCh)
		emptyErrCh := make(chan error)
		close(emptyErrCh)
		dstCh = emptyCh
		dstErrCh = emptyErrCh
	}

	// Build the comparator for property comparison (size, LWT, changeTime)
	comparator := &syncDestinationComparator{
		sourceIndex:             enumerator.objectIndexer, // not used in merge-join path but required by struct
		copyTransferScheduler:   enumerator.ctp.scheduleCopyTransfer,
		destinationCleaner:      enumerator.objectComparator, // will be replaced below
		deleteDestination:       cca.deleteDestination,
		incrementNotTransferred: enumerator.primaryTraverserTemplate.options.IncrementNotTransferred,
		orchestratorOptions:     enumerator.orchestratorOptions,
	}

	// Pull first object from each side
	srcObj, srcOk := <-srcCh
	dstObj, dstOk := <-dstCh

	for srcOk && dstOk {
		srcOrigPath := srcObj.relativePath
		srcPath := buildChildPath(dir, srcObj.relativePath, srcObj.entityType == common.EEntityType.Folder())
		dstPath := buildChildPath(dir, dstObj.relativePath, dstObj.entityType == common.EEntityType.Folder())

		cmp := strings.Compare(srcPath, dstPath)

		switch {
		case cmp < 0:
			// Source-only: new object at source → schedule transfer
			srcObj.relativePath = srcPath
			subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcOrigPath, subDirs)
			if err != nil {
				return subDirs, err
			}
			srcObj, srcOk = <-srcCh

		case cmp > 0:
			// Dest-only: extra object at destination → handle based on delete-destination policy
			dstObj.relativePath = dstPath
			err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
			if err != nil {
				return subDirs, err
			}
			dstObj, dstOk = <-dstCh

		default:
			// Both exist: compare properties and decide
			srcObj.relativePath = srcPath
			dstObj.relativePath = dstPath
			subDirs, err = mergeJoinHandleBothExist(enumerator, cca, comparator, srcObj, dstObj, srcOrigPath, subDirs)
			if err != nil {
				return subDirs, err
			}
			srcObj, srcOk = <-srcCh
			dstObj, dstOk = <-dstCh
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
		srcObj, srcOk = <-srcCh
	}

	// Drain remaining dest-only objects
	for dstOk {
		dstObj.relativePath = buildChildPath(dir, dstObj.relativePath, dstObj.entityType == common.EEntityType.Folder())
		err = mergeJoinHandleDestOnly(enumerator, cca, dstObj)
		if err != nil {
			return subDirs, err
		}
		dstObj, dstOk = <-dstCh
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
