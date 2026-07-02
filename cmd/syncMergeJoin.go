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
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// mergeJoinTraceEnabled turns on verbose per-object, per-comparison tracing of the
// streaming merge-join sync path. Enable by setting AZCOPY_MERGEJOIN_TRACE to a
// truthy value (1/true/yes). Off by default so production logs stay quiet.
var mergeJoinTraceEnabled = func() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AZCOPY_MERGEJOIN_TRACE"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}()

// mergeJoinTraceLog emits a verbose merge-join trace line (log file + console, so it
// is visible in Log Analytics container logs) only when AZCOPY_MERGEJOIN_TRACE is on.
func mergeJoinTraceLog(format string, args ...interface{}) {
	if !mergeJoinTraceEnabled {
		return
	}
	mergeJoinSyncOneDirLog(common.LogInfo, "[TRACE] "+fmt.Sprintf(format, args...), true)
}

// mergeJoinEntityLabel returns a short human-readable label for a StoredObject's
// entity type, used in trace logs ("file", "folder", or "vdir" for an FNS virtual prefix).
func mergeJoinEntityLabel(obj StoredObject) string {
	if obj.entityType == common.EEntityType.Folder() {
		if obj.isVirtualPrefix {
			return "vdir"
		}
		return "folder"
	}
	return "file"
}

// mergeJoinCmpLabel maps a strings.Compare result to a human-readable merge-join
// branch label for trace logs.
func mergeJoinCmpLabel(cmp int) string {
	switch {
	case cmp < 0:
		return "SRC<DST => source-only"
	case cmp > 0:
		return "SRC>DST => dest-only"
	default:
		return "SRC==DST => both"
	}
}

// mergeJoinChildKey returns the per-object ordering key used by the streaming
// merge-join, matching buildChildPath's ordering (folder => name+"/", file => name).
// Because the parent directory is a common prefix for every object in a single
// traverser call, comparing these suffix keys is order-equivalent to comparing the
// full buildChildPath values — which lets us cheaply verify the sort invariant.
func mergeJoinChildKey(obj StoredObject) string {
	if obj.entityType == common.EEntityType.Folder() {
		return strings.TrimSuffix(obj.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) + common.AZCOPY_PATH_SEPARATOR_STRING
	}
	return obj.relativePath
}

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
		var prevKey string // last emitted ordering key, for the sort-invariant guard

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
			// Sort-invariant guard (always on, ~one string compare per object): the
			// streaming merge-join is only correct if each stream is emitted in strictly
			// non-decreasing key order. If a traverser ever emits an out-of-order key,
			// the merge-join can silently mis-classify objects (missed or extra transfers),
			// so we log it at Error even in production. This is rare (fires only on the
			// anomaly) and cheap, so it does not spam logs for millions of sorted files.
			curKey := mergeJoinChildKey(obj)
			if count > 1 && curKey < prevKey {
				mergeJoinSyncOneDirLog(common.LogError,
					fmt.Sprintf("ORDER VIOLATION in %s: object #%d key=%q < previous key=%q — listing is not lexicographically sorted; merge-join results may be INCORRECT for this directory",
						label, count, curKey, prevKey), true)
			}
			prevKey = curKey
			mergeJoinTraceLog("%s LIST #%d relPath=%q entity=%s size=%d lmt=%s",
				label, count, obj.relativePath, mergeJoinEntityLabel(obj), obj.size, obj.lastModifiedTime.Format(time.RFC3339))
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

	// Bridge both traversers into single-item pull channels; the merge loop consumes one
	// object at a time from each side.
	srcLabel := fmt.Sprintf("SRC[%s]", dir)
	dstLabel := fmt.Sprintf("DST[%s]", dir)

	var srcNext, dstNext func() (StoredObject, bool)
	var srcErrCh, dstErrCh <-chan error

	closedErrCh := func() <-chan error {
		ec := make(chan error)
		close(ec)
		return ec
	}

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
		mergeJoinTraceLog("CMP dir=%q SRC=%q(%s) vs DST=%q(%s) => %s",
			dir, srcPath, mergeJoinEntityLabel(srcObj), dstPath, mergeJoinEntityLabel(dstObj), mergeJoinCmpLabel(cmp))

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
		mergeJoinTraceLog("DRAIN-SRC dir=%q SRC=%q(%s) (dest exhausted) => source-only",
			dir, srcObj.relativePath, mergeJoinEntityLabel(srcObj))
		subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcOrigPath, subDirs)
		if err != nil {
			return subDirs, err
		}
		srcObj, srcOk = srcNext()
	}

	// Drain remaining dest-only objects
	for dstOk {
		dstObj.relativePath = buildChildPath(dir, dstObj.relativePath, dstObj.entityType == common.EEntityType.Folder())
		mergeJoinTraceLog("DRAIN-DST dir=%q DST=%q(%s) (source exhausted) => dest-only",
			dir, dstObj.relativePath, mergeJoinEntityLabel(dstObj))
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

// useStreamingMergeJoin reports whether the streaming merge-join should be used for
// this transfer. It is intentionally restricted to the source/destination pairs whose
// listing order is proven lexicographically sorted and validated end-to-end:
//   - S3 -> Blob (S3 source list-order fix + per-level merge)
//   - Azure -> Azure, i.e. Blob/BlobFS (FNS or HNS) in any combination
//
// Everything else (Azure Files, Local, GCP, etc.) stays on the existing indexMap-based
// flow. Keep this allow-list conservative: any source whose listing is not guaranteed
// globally sorted would silently break the two-pointer merge.
func useStreamingMergeJoin(fromTo common.FromTo) bool {
	isAzure := func(loc common.Location) bool {
		return loc == common.ELocation.Blob() || loc == common.ELocation.BlobFS()
	}
	from, to := fromTo.From(), fromTo.To()
	switch {
	case from == common.ELocation.S3() && to == common.ELocation.Blob():
		return true // S3 -> Blob
	case isAzure(from) && isAzure(to):
		return true // Azure -> Azure (Blob/BlobFS)
	default:
		return false
	}
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
			mergeJoinTraceLog("    ACTION SRC-ONLY folder %q: self-referential sentinel => TRANSFER (ACL only, no recurse)", srcObj.relativePath)
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
			mergeJoinTraceLog("    ACTION SRC-ONLY vdir %q: RECURSE only (FNS virtual prefix, nothing to transfer)", srcObj.relativePath)
			return subDirs, nil
		}

		// Schedule folder transfer (for ACLs, metadata, etc.)
		mergeJoinTraceLog("    ACTION SRC-ONLY folder %q: TRANSFER (ACL/metadata) + RECURSE", srcObj.relativePath)
		err := enumerator.ctp.scheduleCopyTransfer(srcObj)
		if err != nil {
			return subDirs, err
		}

		return subDirs, nil
	}

	// File: schedule transfer
	mergeJoinTraceLog("    ACTION SRC-ONLY file %q: TRANSFER (new at destination)", srcObj.relativePath)
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
		mergeJoinTraceLog("    ACTION DST-ONLY %q: DELETE (--delete-destination=true, not present at source)", dstObj.relativePath)
		return enumerator.objectComparator(dstObj)
	}

	// Not deleting destination — this object is extra but we leave it alone
	mergeJoinTraceLog("    ACTION DST-ONLY %q: SKIP (extra at destination, delete-destination not enabled)", dstObj.relativePath)
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
			mergeJoinTraceLog("    ACTION BOTH folder %q: self-referential sentinel => TRANSFER (ACL only, no recurse)", srcObj.relativePath)
			err := enumerator.ctp.scheduleCopyTransfer(srcObj)
			return subDirs, err
		}

		mergeJoinTraceLog("    ACTION BOTH folder %q: RECURSE (enqueue subdir); property comparison follows", srcObj.relativePath)
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
		mergeJoinTraceLog("    ACTION BOTH %q (%s): OVERWRITE (src LMT %s newer than dst LMT %s)",
			srcObj.relativePath, mergeJoinEntityLabel(srcObj),
			srcObj.lastModifiedTime.Format(time.RFC3339), dstObj.lastModifiedTime.Format(time.RFC3339))
		syncComparatorLog(srcObj.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
		err := enumerator.ctp.scheduleCopyTransfer(srcObj)
		return subDirs, err
	}

	// No change — skip transfer
	if enumerator.primaryTraverserTemplate.options.IncrementNotTransferred != nil && !srcObj.isVirtualPrefix {
		enumerator.primaryTraverserTemplate.options.IncrementNotTransferred(srcObj.entityType)
	}

	mergeJoinTraceLog("    ACTION BOTH %q (%s): SKIP (no change; src LMT %s <= dst LMT %s)",
		srcObj.relativePath, mergeJoinEntityLabel(srcObj),
		srcObj.lastModifiedTime.Format(time.RFC3339), dstObj.lastModifiedTime.Format(time.RFC3339))
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
