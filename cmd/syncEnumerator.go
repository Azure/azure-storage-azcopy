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
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (cca *cookedSyncCmdArgs) initEnumerator(ctx context.Context) (enumerator *syncEnumerator, err error) {

	srcCredInfo, srcIsPublic, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source.Value, cca.source.SAS, true, cca.cpkOptions)

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
	tqueue := make(chan interface{}, 1000*1000)

	// set up the map, so that the source/destination can be compared
	objectIndexerMap := newfolderIndexer()

	// TODO: enable symlink support in a future release after evaluating the implications
	// TODO: Consider passing an errorChannel so that enumeration errors during sync can be conveyed to the caller.
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	sourceTraverser, err := InitResourceTraverser(cca.source, cca.fromTo.From(), &ctx, &srcCredInfo, nil, nil, cca.recursive, true, cca.isHNSToHNS, common.EPermanentDeleteOption.None(), func(entityType common.EntityType) {
		if entityType == common.EEntityType.File() {
			atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
		}
	}, nil, cca.s2sPreserveBlobTags, cca.logVerbosity.ToPipelineLogLevel(), cca.cpkOptions, nil, /* errorChannel */
		objectIndexerMap, tqueue, true /* isSource */, true, /* isSync */
		cca.maxObjectIndexerMapSizeInGB, time.Time{} /* lastSyncTime (not used by source traverser) */, cca.cfdMode, cca.metaDataOnlySync)

	if err != nil {
		return nil, err
	}

	// Because we can't trust cca.credinfo, given that it's for the overall job, not the individual traversers, we get cred info again here.
	dstCredInfo, _, err := GetCredentialInfoForLocation(ctx, cca.fromTo.To(), cca.destination.Value,
		cca.destination.SAS, false, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := InitResourceTraverser(cca.destination, cca.fromTo.To(), &ctx, &dstCredInfo, nil, nil, cca.recursive, true, cca.isHNSToHNS, common.EPermanentDeleteOption.None(), func(entityType common.EntityType) {
		if entityType == common.EEntityType.File() {
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		}
	}, nil, cca.s2sPreserveBlobTags, cca.logVerbosity.ToPipelineLogLevel(), cca.cpkOptions,
		nil /* errorChannel */, objectIndexerMap /*folderIndexerMap */, tqueue, false /* isSource */, true, /* isSync */
		cca.maxObjectIndexerMapSizeInGB /* maxObjectIndexerSizeInGB (not used by destination traverse) */, cca.lastSyncTime /* lastSyncTime */, cca.cfdMode, cca.metaDataOnlySync)
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	if sourceTraverser.IsDirectory(true) != destinationTraverser.IsDirectory(true) {
		return nil, errors.New("trying to sync between different resource types (either file <-> directory or directory <-> file) which is not allowed." +
			"sync must happen between source and destination of the same type, e.g. either file <-> file or directory <-> directory")
	}

	// set up the filters in the right order
	// Note: includeFilters and includeAttrFilters are ANDed
	// They must both pass to get the file included
	// Same rule applies to excludeFilters and excludeAttrFilters
	filters := buildIncludeFilters(cca.includePatterns)
	if cca.fromTo.From() == common.ELocation.Local() {
		includeAttrFilters := buildAttrFilters(cca.includeFileAttributes, cca.source.ValueLocal(), true)
		filters = append(filters, includeAttrFilters...)
	}

	filters = append(filters, buildExcludeFilters(cca.excludePatterns, false)...)
	filters = append(filters, buildExcludeFilters(cca.excludePaths, true)...)
	if cca.fromTo.From() == common.ELocation.Local() {
		excludeAttrFilters := buildAttrFilters(cca.excludeFileAttributes, cca.source.ValueLocal(), false)
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
	fpo, folderMessage := newFolderPropertyOption(cca.fromTo, cca.recursive, true, filters, cca.preserveSMBInfo, cca.preservePermissions.IsTruthy(), false, cca.isHNSToHNS, strings.EqualFold(cca.destination.Value, common.Dev_Null), false) // sync always acts like stripTopDir=true
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
		destinationCleaner, err := newSyncDeleteProcessor(cca)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		destCleanerFunc := newFpoAwareProcessor(fpo, destinationCleaner.removeImmediately)

		// when uploading, we can delete remote objects immediately, because as we traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source
		comparator = newSyncDestinationComparator(objectIndexerMap, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.mirrorMode, cca.cfdMode, cca.lastSyncTime).processIfNecessary
		finalize = func() error {
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

			quitIfInSync(jobInitiated, cca.getDeletionCount() > 0, cca)
			cca.setScanningComplete()
			return nil
		}

		return newSyncEnumerator(sourceTraverser, destinationTraverser, objectIndexerMap, filters, comparator, finalize, tqueue), nil
	default:
		objectIndexerMap.isDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.fromTo)
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
				deleter, err := newSyncDeleteProcessor(cca)
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

			quitIfInSync(jobInitiated, cca.getDeletionCount() > 0, cca)
			cca.setScanningComplete()
			return nil
		}

		return newSyncEnumerator(destinationTraverser, sourceTraverser, objectIndexerMap, filters, comparator, finalize, tqueue), nil
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
