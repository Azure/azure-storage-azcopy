//go:build smslidingwindow
// +build smslidingwindow

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

package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ============================================================================
// TWO-WAY streaming merge-join: consumer side (two concurrent merges + lifecycle)
//
// The file-merge and folder-merge run CONCURRENTLY, each a plain two-pointer merge over its own
// source+destination typed channels. They share the enumerator (scheduleCopyTransfer is
// concurrency-safe — the indexMap path calls it from many crawl workers); only the folder-merge
// writes subDirs, so there is no shared-state contention. See two-way-merge-join-design.md.
// ============================================================================

// twoWayClosedObjCh returns an already-closed StoredObject channel (used when the destination is
// absent, so a merge sees an immediately-exhausted destination stream).
func twoWayClosedObjCh() <-chan StoredObject {
	c := make(chan StoredObject)
	close(c)
	return c
}

// mergeJoinTwoWayPollErrs NON-consumingly checks the source and destination side-error holders and,
// if either reported a failure, returns it as a tagged mergeJoinTraversalError. Called at the top of
// the merge loops and (critically) before any destination DELETE, so a truncated source listing
// never causes over-transfer or wrongful mirror deletes. Both merges can observe the same error
// because the holder is non-consuming.
func mergeJoinTwoWayPollErrs(cca *cookedSyncCmdArgs, srcSideErr, dstSideErr *twoWaySideErr) error {
	if e := srcSideErr.get(); e != nil {
		return &mergeJoinTraversalError{location: cca.fromTo.From(), err: fmt.Errorf("source traversal error during merge-join: %w", e)}
	}
	if e := dstSideErr.get(); e != nil {
		return &mergeJoinTraversalError{location: cca.fromTo.To(), err: fmt.Errorf("destination traversal error during merge-join: %w", e)}
	}
	return nil
}

// drainTypedTraverser reads a traverser's folder and file channels until BOTH are closed, discarding
// remaining objects. Used by the deferred cleanup to AWAIT a producer goroutine's full exit (so it
// can no longer write to shared channels) on every return path.
func drainTypedTraverser(folderCh, fileCh <-chan StoredObject) {
	for folderCh != nil || fileCh != nil {
		select {
		case _, ok := <-folderCh:
			if !ok {
				folderCh = nil
			}
		case _, ok := <-fileCh:
			if !ok {
				fileCh = nil
			}
		}
	}
}

// mergeJoinTwoWayMergeFiles performs the FILE two-pointer merge over the source and destination file
// streams. Files never produce sub-dirs. It polls the side-error holders before acting so a
// truncated source listing cannot cause over-transfer or (for mirror) wrongful destination deletes.
func mergeJoinTwoWayMergeFiles(
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	comparator *syncDestinationComparator,
	dir string,
	srcFileCh, dstFileCh <-chan StoredObject,
	srcSideErr, dstSideErr *twoWaySideErr,
) error {
	srcNext := func() (StoredObject, bool) { return mergeJoinRecv(srcFileCh) }
	dstNext := func() (StoredObject, bool) { return mergeJoinRecv(dstFileCh) }

	srcObj, srcOk := srcNext()
	dstObj, dstOk := dstNext()

	for srcOk && dstOk {
		if e := mergeJoinTwoWayPollErrs(cca, srcSideErr, dstSideErr); e != nil {
			return e
		}
		srcPath := buildChildPath(dir, srcObj.relativePath, false)
		dstPath := buildChildPath(dir, dstObj.relativePath, false)
		cmp := strings.Compare(srcPath, dstPath)
		mergeJoinTraceLog("FILE-CMP dir=%q SRC=%q vs DST=%q => %s", dir, srcPath, dstPath, mergeJoinCmpLabel(cmp))
		switch {
		case cmp < 0:
			srcObj.relativePath = srcPath
			if _, err := mergeJoinHandleSourceOnly(enumerator, cca, srcObj, "", nil); err != nil {
				return err
			}
			srcObj, srcOk = srcNext()
		case cmp > 0:
			dstObj.relativePath = dstPath
			if err := mergeJoinHandleDestOnly(enumerator, cca, dstObj); err != nil {
				return err
			}
			dstObj, dstOk = dstNext()
		default:
			srcObj.relativePath = srcPath
			dstObj.relativePath = dstPath
			if _, err := mergeJoinHandleBothExist(enumerator, cca, comparator, srcObj, dstObj, "", nil); err != nil {
				return err
			}
			srcObj, srcOk = srcNext()
			dstObj, dstOk = dstNext()
		}
	}

	for srcOk {
		if e := mergeJoinTwoWayPollErrs(cca, srcSideErr, dstSideErr); e != nil {
			return e
		}
		srcObj.relativePath = buildChildPath(dir, srcObj.relativePath, false)
		if _, err := mergeJoinHandleSourceOnly(enumerator, cca, srcObj, "", nil); err != nil {
			return err
		}
		srcObj, srcOk = srcNext()
	}

	for dstOk {
		// Mirror protection: if the SOURCE listing failed, remaining destination files are NOT
		// genuinely source-absent — deleting them would be data loss. Abort before any dest delete.
		if e := mergeJoinTwoWayPollErrs(cca, srcSideErr, dstSideErr); e != nil {
			return e
		}
		dstObj.relativePath = buildChildPath(dir, dstObj.relativePath, false)
		if err := mergeJoinHandleDestOnly(enumerator, cca, dstObj); err != nil {
			return err
		}
		dstObj, dstOk = dstNext()
	}
	return nil
}

// mergeJoinTwoWayMergeFolders performs the FOLDER two-pointer merge over the source and destination
// folder streams, collecting sub-dirs for recursion. Same mirror-protection discipline as the files.
func mergeJoinTwoWayMergeFolders(
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	comparator *syncDestinationComparator,
	dir string,
	srcFolderCh, dstFolderCh <-chan StoredObject,
	srcSideErr, dstSideErr *twoWaySideErr,
) ([]minimalStoredObject, error) {
	subDirs := make([]minimalStoredObject, 0, 64)
	srcNext := func() (StoredObject, bool) { return mergeJoinRecv(srcFolderCh) }
	dstNext := func() (StoredObject, bool) { return mergeJoinRecv(dstFolderCh) }

	srcObj, srcOk := srcNext()
	dstObj, dstOk := dstNext()

	var err error
	for srcOk && dstOk {
		if e := mergeJoinTwoWayPollErrs(cca, srcSideErr, dstSideErr); e != nil {
			return subDirs, e
		}
		srcOrigPath := srcObj.relativePath
		srcPath := buildChildPath(dir, srcObj.relativePath, true)
		dstPath := buildChildPath(dir, dstObj.relativePath, true)
		cmp := strings.Compare(srcPath, dstPath)
		mergeJoinTraceLog("FOLDER-CMP dir=%q SRC=%q vs DST=%q => %s", dir, srcPath, dstPath, mergeJoinCmpLabel(cmp))
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
			if err = mergeJoinHandleDestOnly(enumerator, cca, dstObj); err != nil {
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

	for srcOk {
		if e := mergeJoinTwoWayPollErrs(cca, srcSideErr, dstSideErr); e != nil {
			return subDirs, e
		}
		srcOrigPath := srcObj.relativePath
		srcObj.relativePath = buildChildPath(dir, srcObj.relativePath, true)
		subDirs, err = mergeJoinHandleSourceOnly(enumerator, cca, srcObj, srcOrigPath, subDirs)
		if err != nil {
			return subDirs, err
		}
		srcObj, srcOk = srcNext()
	}

	for dstOk {
		if e := mergeJoinTwoWayPollErrs(cca, srcSideErr, dstSideErr); e != nil {
			return subDirs, e
		}
		dstObj.relativePath = buildChildPath(dir, dstObj.relativePath, true)
		if err = mergeJoinHandleDestOnly(enumerator, cca, dstObj); err != nil {
			return subDirs, err
		}
		dstObj, dstOk = dstNext()
	}
	return subDirs, nil
}

// mergeJoinTwoWaySyncDir performs a streaming merge-join for one directory level using SEPARATE,
// concurrent file and folder merges over typed channels.
//
// cancel must cancel the context the traversers (pt/st) were created with: the deferred cleanup
// calls it to abort in-flight listing before draining/awaiting every producer goroutine, so none is
// still running (and able to write to the shared sync error channel) when this returns.
func mergeJoinTwoWaySyncDir(
	ctx context.Context,
	cancel context.CancelFunc,
	enumerator *syncEnumerator,
	cca *cookedSyncCmdArgs,
	dir string,
	pt ResourceTraverser,
	st ResourceTraverser,
	isDestinationPresent bool,
) (subDirs []minimalStoredObject, err error) {

	activeMergeJoinDirs.Add(1)
	defer activeMergeJoinDirs.Add(-1)

	srcLabel := fmt.Sprintf("SRC[%s]", dir)
	dstLabel := fmt.Sprintf("DST[%s]", dir)

	srcSideErr := &twoWaySideErr{}
	dstSideErr := &twoWaySideErr{}

	srcFolderCh, srcFileCh := traverserToTypedChannels(ctx, pt, enumerator.filters, srcLabel, srcSideErr)

	var dstFolderCh, dstFileCh <-chan StoredObject
	if isDestinationPresent {
		dstFolderCh, dstFileCh = traverserToTypedChannels(ctx, st, enumerator.filters, dstLabel, dstSideErr)
	} else {
		dstFolderCh = twoWayClosedObjCh()
		dstFileCh = twoWayClosedObjCh()
	}

	// Guarantee every producer goroutine is stopped and fully drained before returning, on EVERY
	// return path (success, error, cancellation). cancel() aborts in-flight listing; draining the
	// object channels lets a producer blocked on a send observe cancellation and exit and confirms
	// the producer goroutine has fully exited. If we do not already have an error to report, surface
	// any traversal error the side-holders recorded, attributed to the correct side (preserving
	// mirror delete-protection for a source truncated by an error).
	defer func() {
		cancel()
		drainTypedTraverser(srcFolderCh, srcFileCh)
		drainTypedTraverser(dstFolderCh, dstFileCh)
		if err == nil {
			if e := srcSideErr.get(); e != nil {
				err = &mergeJoinTraversalError{location: cca.fromTo.From(), err: fmt.Errorf("source traversal error during merge-join: %w", e)}
			} else if e := dstSideErr.get(); e != nil {
				err = &mergeJoinTraversalError{location: cca.fromTo.To(), err: fmt.Errorf("destination traversal error during merge-join: %w", e)}
			}
		}
	}()

	comparator := &syncDestinationComparator{
		sourceIndex:             enumerator.objectIndexer,
		copyTransferScheduler:   enumerator.ctp.scheduleCopyTransfer,
		destinationCleaner:      enumerator.objectComparator,
		deleteDestination:       cca.deleteDestination,
		preferSMBTime:           cca.preserveInfo,
		incrementNotTransferred: enumerator.primaryTraverserTemplate.options.IncrementNotTransferred,
		orchestratorOptions:     enumerator.orchestratorOptions,
	}

	// Run the file merge and folder merge concurrently. The FIRST error cancels the per-directory
	// context, which stops the other merge and all producers promptly.
	var wg sync.WaitGroup
	var fileErr, folderErr error
	var folderSubDirs []minimalStoredObject

	wg.Add(2)
	go func() {
		defer wg.Done()
		fileErr = mergeJoinTwoWayMergeFiles(enumerator, cca, comparator, dir, srcFileCh, dstFileCh, srcSideErr, dstSideErr)
		if fileErr != nil {
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		folderSubDirs, folderErr = mergeJoinTwoWayMergeFolders(enumerator, cca, comparator, dir, srcFolderCh, dstFolderCh, srcSideErr, dstSideErr)
		if folderErr != nil {
			cancel()
		}
	}()
	wg.Wait()

	if folderErr != nil {
		return folderSubDirs, folderErr
	}
	if fileErr != nil {
		return folderSubDirs, fileErr
	}
	return folderSubDirs, nil
}

// ---------------------------------------------------------------------------
// Consumer-owned merge-join helpers: channel receive, gating, entity/label
// helpers, the traversal-error type, the self-referential sentinel check, and
// the per-object decision handlers that the file-merge and folder-merge call.
// ---------------------------------------------------------------------------

// activeMergeJoinDirs tracks how many merge-join directory syncs are in-flight.
var activeMergeJoinDirs atomic.Int64

// mergeJoinChanDryEvents increments each time a consumer's receive finds the channel
// empty while still open (starvation: the listing/producer/network is slower than the merge).
var mergeJoinChanDryEvents atomic.Int64

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

// mergeJoinTraversalError wraps a traverser failure surfaced by the streaming merge-join,
// tagging which side (source or destination) failed. This lets the sync orchestrator report
// the error to the error channel with the correct TraverserLocation and path, matching the
// attribution the indexMap path produces for source vs destination enumeration failures.
type mergeJoinTraversalError struct {
	location common.Location // source (fromTo.From()) or destination (fromTo.To())
	err      error
}

func (e *mergeJoinTraversalError) Error() string { return e.err.Error() }
func (e *mergeJoinTraversalError) Unwrap() error  { return e.err }

// mergeJoinDefaultParallelTraversers is the default directory-crawl parallelism used ONLY for
// streaming merge-join jobs. It is intentionally separate from (and lower than) the indexMap
// path's parallelism because the merge-join additionally lists source and destination
// concurrently within each directory, so fewer directories in flight keeps the live working
// set (and GC pressure) bounded.
const mergeJoinDefaultParallelTraversers int32 = 32

// mergeJoinParallelTraversers is the directory-crawl parallelism for streaming merge-join jobs.
// Override with SYNC_MJ_PARALLEL_TRAVERSERS (positive integer). The indexMap sync path is
// unaffected and keeps its own parallelism (orchestratorOptions.parallelTraversers).
var mergeJoinParallelTraversers = func() int32 {
	if v := strings.TrimSpace(os.Getenv("SYNC_MJ_PARALLEL_TRAVERSERS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return int32(n)
		}
	}
	return mergeJoinDefaultParallelTraversers
}()

// useStreamingMergeJoin reports whether the streaming merge-join should be used for
// this transfer. Enablement is decided entirely in the mover and passed to azcopy as the single
// per-job flag cca.useStreamingMergeJoin, which the mover sets when EITHER:
//   - the job's subscription is allowlisted for the feature via featureConfig, OR
//   - the USE_STREAMING_MERGE_JOIN env var is set (blanket / testing).
//
// azcopy no longer reads any enablement env var itself. When the flag is set, the merge-join is
// still restricted to the source/destination pairs whose listing order is proven lexicographically
// sorted and validated end-to-end:
//   - S3 -> Blob (S3 source list-order fix + per-level merge)
//   - Azure -> Azure, i.e. Blob/BlobFS (FNS or HNS) in any combination
//
// Everything else (Azure Files, Local, GCP, etc.) always stays on the indexMap flow.
// Keep this allow-list conservative: any source whose listing is not guaranteed
// globally sorted would silently break the two-pointer merge.
func useStreamingMergeJoin(cca *cookedSyncCmdArgs) bool {
	if !cca.useStreamingMergeJoin {
		return false
	}
	isAzure := func(loc common.Location) bool {
		return loc == common.ELocation.Blob() || loc == common.ELocation.BlobFS()
	}
	from, to := cca.fromTo.From(), cca.fromTo.To()
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

// mergeJoinHandleBothExist processes an object present at both source and destination.
// Transfer-vs-skip is decided with isMoreRecentThan (source last-modified newer than the
// destination's) — the appropriate comparison for the merge-join's supported remote pairs
// (S3->Blob and Blob/BlobFS->Blob/BlobFS), whose listings expose a reliable last-modified time.
// It intentionally does NOT run the indexMap comparator (processIfNecessaryWithOrchestrator),
// whose SMB last-write-time / POSIX change-time / metadata-only-sync logic targets File (SMB)
// and NFS sources that the merge-join does not support (for FNS blob and S3, last-write/change
// time are unset, so that path degrades to a plain last-modified comparison anyway).
// Folders are enqueued for recursive traversal.
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
