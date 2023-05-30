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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common/parallel"
	"github.com/aymanjarrousms/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

// orderedTqueue is an ordered tqueue which implements strict child-after-parent ordering of directory
// entries added to tqueue (communication channel source traverser uses to communicate to-be-traversed
// directories to the target traverser). Note that it is crucial for target traverser to always process
// directory entries in strict child-after-parent order for correct handling of non-direct subdirectories
// rename.
// It contains the raw tqueue and other stuff needed to facilitate ordered addition of directory entries
// to the raw tqueue.
//
//   - What exactly do we want to achieve?
//     Scanner threads (default count 16) process directories and add then to any of the 4 channels which
//     are processed by Walk(). Though scanner threads will only scan a child after fully scanning its parent,
//     but since the scanner threads after scanning, add the directories to one of he 4 channels the directories
//     may get picked from these 4 channels in such a way that child is picked before the parent. This may cause
//     child directory to be added to tqueue before its parent directory. This is what we want to avoid.
//
//   - How does orderedTqueue help in that?
//     Since scanner threads always add directories correctly in strict child-after-parent order to the 4 channels,
//     but they get reordered since the threads processing those 4 channels may pick them in arbitrary order, we
//     have the scanner threads, apart from adding the scanned directories to the 4 channels, also add an entry to
//     orderedTqueue.dir in a serialized fashion so that entries in orderedTqueue.dir are added strictly in
//     child-after-parent order. Now when the threads process entries from the 4 channels they just mark the
//     corresponding entry in orderedTqueue.dir as "processed". A separate thread just goes over orderedTqueue.dir
//     and takes out entries from the head which are "processed" and adds them to orderedTqueue.tqueue.
//     It stops and waits when it encounters an head entry which is not marked "processed". This ensures that we add
//     entries to orderedTqueue.tqueue only in strict child-after-parent order.
//
//   - How does Source Traverser interact with orderedTqueue?
//     Source traverser MUST call orderedTqueue.Enqueue() in strict child-after-parent order.
//     Multiple parallel source traverser threads can safely call orderedTqueue.Enqueue() in parallel.
//
//   - How does Target Traverser interact with orderedTqueue?
//     Target traverser can simply dequeue directory entries from the raw orderedTqueue.tqueue.
//     The entries in tqueue are guaranteed to be in strict child-after-parent order.
type orderedTqueue struct {
	// Index in circular buffer where the reader should read the next entry from.
	readIdx int32

	// Index in circular buffer where the writer should write the next entry to.
	writeIdx int32

	// size of circular buffer.
	size int32

	// Number of entries available for the reader. count==0 signifies queue empty condition whereas count==size signifies queue full condition.
	count int32

	// circular buffer where source traverser adds entries in strict child-after-parent order.
	dir []parallel.DirectoryEntry

	lock sync.Mutex

	// Raw tqueue that contains entries in strict child-after-parent order, target traverser reads from here.
	tqueue chan interface{}

	// doNotEnforceChildAfterParent if set will not enforce child-after-parent through the Enqueue() and MarkProcessed() methods.
	// This will be set by callers who don't need the strict child-after-parent ordering (f.e. CFDMode=TargetCompare doesn't need it).
	doNotEnforceChildAfterParent bool
}

// Source traverser MUST call Enqueue() in strict child-after-parent order to add entries to tqueue.
// It returns the index in the circular buffer where the directory entry is added. This index must be
// conveyed along with the CrawlResult so that MarkProcessed() can mark the appropriate entry as processed.
func (t *orderedTqueue) Enqueue(dir parallel.DirectoryEntry) int32 {
	for {

		// If child-after-parent ordering is not needed then we don't need to do anything in Enqueue(),
		// this directory will be added to tqueue by MarkProcessed().
		if t.doNotEnforceChildAfterParent {
			return 0
		}

		t.lock.Lock()

		if t.size == 0 || cap(t.dir) == 0 {
			panic(fmt.Sprintf("Size(%v) or capacity of circular buffer(%v) is zero", t.size, cap(t.dir)))
		}

		// Is circular buffer full?
		if t.count == t.size {
			if t.writeIdx != t.readIdx {
				panic(fmt.Sprintf("WriteIdx(%v) and ReadIdx(%v) not same for circular buffer of size(%v) full", t.writeIdx, t.readIdx, t.count))
			}

			// Needs to wait as no room available.
			t.lock.Unlock()

			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Add to the next available slot.
		entryIdx := t.writeIdx
		t.dir[entryIdx] = parallel.ProcessDirEntry{
			Item:         dir,
			ProcessState: false,
		}

		// Increment writeIdx to point to next free slot.
		t.writeIdx += 1
		if t.writeIdx > t.size {
			panic(fmt.Sprintf("Invalid value of writeIdx(%v) of circular buffer size(%v)",
				t.writeIdx, t.size))
		}

		// One more entry in circular buffer.
		t.count += 1

		// sanity check count should never cross the size of buffer.
		if t.count > t.size {
			panic(fmt.Sprintf("Circular buffer count(%v) more than size(%v), for writeIdx(%v) and readIdx(%v)",
				t.count, t.size, t.writeIdx, t.readIdx))
		}

		// reached end of buffer lets rollover to start.
		if t.writeIdx == t.size {
			t.writeIdx = 0
		}

		t.lock.Unlock()
		return entryIdx
	}
}

func (t *orderedTqueue) GetTqueue() chan interface{} {
	return t.tqueue
}

// SourceTraverser processing threads will call MarkProcessed when they dequeue special "tqueue" entry.
func (t *orderedTqueue) MarkProcessed(idx int32, item interface{}) {
	// If child-after-parent ordering is not needed, we don't need to wait for the parent directory
	// to be added to tqueue, a child can be added as soon as it is ready (we got the special "tqueue" entry
	// for the directory), even if it means adding a child before parent to tqueue.
	if t.doNotEnforceChildAfterParent {
		t.tqueue <- item
		return
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	// Sanity Check
	if idx >= t.size || idx < 0 {
		panic(fmt.Sprintf("Invalid value of idx(%v) for circular buffer size(%v)",
			idx, t.size))
	}

	// Mark entry at idx as processed.
	if entry, ok := t.dir[idx].(parallel.ProcessDirEntry); ok {
		entry.ProcessState = true
		t.dir[idx] = entry
	} else {
		panic("Not valid")
	}

	// After marking this entry as processed, we might have a contiguous set of processed entries, add all of them to tqueue.
	for t.count != 0 {
		if entry, ok := t.dir[t.readIdx].(parallel.ProcessDirEntry); ok {
			if entry.ProcessState {
				t.count -= 1
				if t.count < 0 {
					panic("orderedTqueue.count less than zero, something wrong")
				}

				// Add to tqueue in strict child-after-parent order.
				t.tqueue <- entry.Item

				t.readIdx++
				// Reached end of the buffer, lets rollover to start.
				if t.readIdx == t.size {
					t.readIdx = 0
				}
			} else {
				break
			}
		} else {
			panic("Not Valid Entry")
		}
	}
}

// TargetCompare doesn't need any special rename handling, for the rest we
// maintain possiblyRenamedMap to track directories which could have been renamed and
// hence need to be enumerated on the target.q
func (cca *cookedSyncCmdArgs) ShouldConsultPossiblyRenamedMap() bool {
	return cca.cfdMode != common.CFDModeFlags.TargetCompare()
}

func (cca *cookedSyncCmdArgs) InitEnumerator(ctx context.Context, errorChannel chan ErrorFileInfo) (enumerator *syncEnumerator, err error) {

	srcCredInfo, srcIsPublic, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.Source.Value, cca.Source.SAS, true, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	if cca.fromTo.IsS2S() {
		if cca.fromTo.From() != common.ELocation.S3() && cca.fromTo.From() != common.ELocation.Blob() { // blob and S3 don't necessarily require SAS tokens (S3 w/ access key, blob w/ copysourceauthorization)
			// Adding files here seems like an odd case, but since files can't be public
			// the second half of this if statement does not hurt.

			if srcCredInfo.CredentialType != common.ECredentialType.Anonymous() && !srcIsPublic {
				return nil, fmt.Errorf("the source of a %s->%s sync must either be public, or authorized with a SAS token", cca.fromTo.From(), cca.fromTo.To())
			}
		}
	}

	// Note: As of now we only support source as local and target as blob.
	//
	// TODO: Need to add support for other source and targets.
	if cca.fromTo.From() != common.ELocation.Local() && cca.fromTo.To() != common.ELocation.Blob() {
		panic("New sync algorithm only support source as local and target as blob")
	}

	//
	// tqueue (communication channel between source and target) will communicate the directories
	// that the Target Traverser should enumerate. As Source Traverser scans the source it adds
	// "fully enumerated" directories to tqueue and Target Traverser dequeues from tqueue and processes
	// the dequeued directories. Both Source and Target traversers need tqueue we pass it to both.
	// Some important points to know:
	//
	// 1. Source Traverser will add a directory to tqueue only after it fully enumerates
	//    the directory (i.e., its direct children). It'll also add all the directory's direct
	//    children (with their attributes) in the indexer. This means that Target Traverser
	//    can be sure that whatever directory it processes from tqueue, all the directory's
	//    direct children are present in the indexer for change detection.
	// 2. Depending on the CFDMode (change file detection mode) in use if Target Traverser
	//    detects that a directory has not changed since last sync, it will not enumerate
	//    the directory.
	//
	// We want maxObjectIndexerSizeInGB to solely control how fast the Source Traverser
	// scans before it's made to block to let Target Traverser catch up. Since Source
	// Traverser will block when tqueue is full we want tqueue to be large enough to not
	// fill before the indexer size hits maxObjectIndexerSizeInGB. Assuming a practical
	// file/dir ratio of 20:1 and max sync window of say 20 million files, we use
	// 1 million as the channel size.
	//
	// TODO: See how it performs with experimentation on real workloads.
	// TODO: See if we can make this a function of maxObjectIndexerSizeInGB
	orderedTqueue := &orderedTqueue{}
	var possiblyRenamedMap *possiblyRenamedMap

	if cca.ShouldConsultPossiblyRenamedMap() {
		// set up the rename map, so that the rename can be detected.
		possiblyRenamedMap = newPossiblyRenamedMap()
	}

	orderedTqueue.tqueue = make(chan interface{}, 1000*1000)
	orderedTqueue.size = 100 * 1000
	orderedTqueue.dir = make([]parallel.DirectoryEntry, orderedTqueue.size)

	// set up the map, so that the source/destination can be compared
	objectIndexerMap := newfolderIndexer()

	sourceScannerLogger := common.NewJobLogger(cca.jobID, AzcopyLogVerbosity, AzcopyAppPathFolder, "-source-scanning")
	sourceScannerLogger.OpenLog()

	destinationScannerLogger := common.NewJobLogger(cca.jobID, AzcopyLogVerbosity, AzcopyAppPathFolder, "-destination-scanning")
	destinationScannerLogger.OpenLog()
	// TODO: enable symlink support in a future release after evaluating the implications
	// TODO: Consider passing an errorChannel so that enumeration errors during sync can be conveyed to the caller.
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	sourceTraverser, err := InitResourceTraverser(cca.Source, cca.fromTo.From(), &ctx, &srcCredInfo, &cca.followSymlinks,
		nil, cca.recursive, true, cca.isHNSToHNS, common.EPermanentDeleteOption.None(), func(entityType common.EntityType) {
			if entityType == common.EEntityType.File() {
				atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
			} else if entityType == common.EEntityType.Folder() {
				atomic.AddUint64(&cca.atomicSourceFoldersScanned, 1)
			}
		}, nil, cca.s2sPreserveBlobTags, AzcopyLogVerbosity.ToPipelineLogLevel(), cca.cpkOptions, errorChannel, objectIndexerMap, nil /* possiblyRenamedMap */, orderedTqueue, true /* isSource */, true, /* isSync */
		cca.maxObjectIndexerMapSizeInGB, time.Time{} /* lastSyncTime (not used by source traverser) */, cca.cfdMode, cca.metaDataOnlySync, sourceScannerLogger /* scannerLogger */)

	if err != nil {
		return nil, err
	}

	// Because we can't trust cca.credinfo, given that it's for the overall job, not the individual traversers, we get cred info again here.
	dstCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.Destination.Value,
		cca.Destination.SAS, false, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := InitResourceTraverser(cca.Destination, cca.fromTo.To(), &ctx, &dstCredInfo, nil, nil, cca.recursive, true, true /* includeDirectoryStubs */, common.EPermanentDeleteOption.None(), func(entityType common.EntityType) {
		if entityType == common.EEntityType.File() {
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		}
	}, nil, cca.s2sPreserveBlobTags, AzcopyLogVerbosity.ToPipelineLogLevel(), cca.cpkOptions, errorChannel, objectIndexerMap /*folderIndexerMap */, possiblyRenamedMap, orderedTqueue, false, /* isSource */
		true /* isSync */, cca.maxObjectIndexerMapSizeInGB /* maxObjectIndexerSizeInGB (not used by destination traverse) */, cca.lastSyncTime /* lastSyncTime */, cca.cfdMode, cca.metaDataOnlySync,
		destinationScannerLogger /*scannerLogger */)
	if err != nil {
		return nil, err
	}

	// set up the filters in the right order
	// Note: includeFilters and includeAttrFilters are ANDed
	// They must both pass to get the file included
	// Same rule applies to excludeFilters and excludeAttrFilters
	filters := buildIncludeFilters(cca.includePatterns)
	if cca.fromTo.From() == common.ELocation.Local() {
		includeAttrFilters := buildAttrFilters(cca.includeFileAttributes, cca.Source.ValueLocal(), true)
		filters = append(filters, includeAttrFilters...)
	}

	filters = append(filters, buildExcludeFilters(cca.excludePatterns, false)...)
	filters = append(filters, buildExcludeFilters(cca.excludePaths, true)...)
	if cca.fromTo.From() == common.ELocation.Local() {
		excludeAttrFilters := buildAttrFilters(cca.excludeFileAttributes, cca.Source.ValueLocal(), false)
		filters = append(filters, excludeAttrFilters...)
	}

	// includeRegex
	filters = append(filters, buildRegexFilters(cca.includeRegex, true)...)
	filters = append(filters, buildRegexFilters(cca.excludeRegex, false)...)

	// after making all filters, log any search prefix computed from them
	if jobsAdmin.JobsAdmin != nil {
		if prefixFilter := FilterSet(filters).GetEnumerationPreFilter(cca.recursive); prefixFilter != "" {
			jobsAdmin.JobsAdmin.LogToJobLog("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, pipeline.LogInfo) // "May be used" because we don't know here which enumerators will use it
		}
	}

	// decide our folder transfer strategy
	fpo, folderMessage := newFolderPropertyOption(cca.fromTo, cca.recursive, cca.StripTopDir /* stripTopDir */, filters, cca.preserveSMBInfo, cca.preservePermissions.IsTruthy(), cca.preservePOSIXProperties, cca.isHNSToHNS, strings.EqualFold(cca.Destination.Value, common.Dev_Null), false) // sync always acts like stripTopDir=true
	if !cca.dryrunMode {
		glcm.Info(folderMessage)
	}
	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(folderMessage, pipeline.LogInfo)
	}

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo)

	var comparator objectProcessor
	var finalize func() error

	switch cca.fromTo {
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile():
		// Upload implies transferring from a local disk to a remote resource.
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains

		//
		// This is the channel to signal delete workers that target enumeration has completed.
		// It's called from finalize(), which is called after target enumeration completes.
		// On receiving this signal, delete workers should exit.
		//
		stopDeleteWorkers := make(chan struct{})

		destinationCleaner, err := newSyncDeleteProcessor(cca, stopDeleteWorkers)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		destCleanerFunc := newFpoAwareProcessor(fpo, destinationCleaner.removeImmediately)

		// when uploading, we can delete remote objects immediately, because as we traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source
		comparator = newSyncDestinationComparator(objectIndexerMap, possiblyRenamedMap, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.mirrorMode, cca.cfdMode, cca.lastSyncTime, destinationScannerLogger, func(entityType common.EntityType) {
			if entityType == common.EEntityType.File() {
				atomic.AddUint64(&cca.atomicSourceFilesTransferNotRequired, 1)
			} else if entityType == common.EEntityType.Folder() {
				atomic.AddUint64(&cca.atomicSourceFoldersTransferNotRequired, 1)
			}
		}, cca.metaDataOnlySync).processIfNecessary

		finalize = func() error {
			//
			// Now that target traverser is done processing, there cannot be any more "delete jobs", tell
			// deleteWorkers to stop. They are listening on the stopDeleteWorkers channel and on reading from
			// that channel they will exit and close the stopDeleteWorkers channel. We wait for the stopDeleteWorkers
			// channel to close before proceeding further.
			//
			stopDeleteWorkers <- struct{}{}

			<-stopDeleteWorkers

			// Check if enumeration cancelled or not.
			if ctx.Err() != nil {
				return nil
			}

			// schedule every local file that doesn't exist at the destination
			err = objectIndexerMap.traverse(transferScheduler.scheduleCopyTransfer, filters)
			if err != nil {
				return err
			}

			jobInitiated, err := transferScheduler.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}

			quitIfInSync(jobInitiated, cca.GetDeletionCount() > 0, cca)
			cca.setScanningComplete()
			return nil
		}

		return newSyncEnumerator(sourceTraverser, destinationTraverser, objectIndexerMap, filters, comparator, finalize, orderedTqueue), nil
	default:
		objectIndexerMap.isDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.fromTo)
		possiblyRenamedMap.isDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.fromTo)
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(objectIndexerMap, transferScheduler.scheduleCopyTransfer, cca.mirrorMode).processIfNecessary

		finalize = func() error {
			// remove the extra files at the destination that were not present at the source
			// we can only know what needs to be deleted when we have FINISHED traversing the remote source
			// since only then can we know which local files definitely don't exist remotely
			var deleteScheduler objectProcessor
			switch cca.fromTo.To() {
			case common.ELocation.Blob(), common.ELocation.File():
				stopDeleteWorkers := make(chan struct{})
				deleter, err := newSyncDeleteProcessor(cca, stopDeleteWorkers)
				if err != nil {
					return err
				}
				deleteScheduler = newFpoAwareProcessor(fpo, deleter.removeImmediately)
			default:
				deleteScheduler = newFpoAwareProcessor(fpo, newSyncLocalDeleteProcessor(cca).removeImmediately)
			}

			err = objectIndexerMap.traverse(deleteScheduler, nil)
			if err != nil {
				return err
			}

			// let the deletions happen first
			// otherwise if the final part is executed too quickly, we might quit before deletions could finish
			jobInitiated, err := transferScheduler.dispatchFinalPart()
			// sync cleanly exits if nothing is scheduled.
			if err != nil && err != NothingScheduledError {
				return err
			}

			quitIfInSync(jobInitiated, cca.GetDeletionCount() > 0, cca)
			cca.setScanningComplete()
			return nil
		}

		return newSyncEnumerator(destinationTraverser, sourceTraverser, objectIndexerMap, filters, comparator, finalize, orderedTqueue), nil
	}
}

func IsDestinationCaseInsensitive(fromTo common.FromTo) bool {
	if fromTo.IsDownload() && runtime.GOOS == "windows" {
		return true
	} else {
		return false
	}
}

func quitIfInSync(transferJobInitiated, anyDestinationFileDeleted bool, cca *cookedSyncCmdArgs) {
	if !transferJobInitiated && !anyDestinationFileDeleted {
		cca.reportScanningProgress(glcm, 0)
		glcm.Exit(func(format common.OutputFormat) string {
			return "The source and destination are already in sync."
		}, common.EExitCode.Success())
	} else if !transferJobInitiated && anyDestinationFileDeleted {
		// some files were deleted but no transfer scheduled
		cca.reportScanningProgress(glcm, 0)
		glcm.Exit(func(format common.OutputFormat) string {
			return "The source and destination are now in sync."
		}, common.EExitCode.Success())
	}
}
