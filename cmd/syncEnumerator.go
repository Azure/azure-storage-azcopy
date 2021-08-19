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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (cca *cookedSyncCmdArgs) initEnumerator(ctx context.Context) (enumerator *syncEnumerator, err error) {

	srcCredInfo, srcIsPublic, err := GetCredentialInfoForLocation(ctx, cca.fromTo.From(), cca.source.Value, cca.source.SAS, true, cca.cpkOptions)

	if err != nil {
		return nil, err
	}

	if cca.fromTo.IsS2S() {
		if cca.fromTo.From() != common.ELocation.S3() {
			// Adding files here seems like an odd case, but since files can't be public
			// the second half of this if statement does not hurt.

			if srcCredInfo.CredentialType != common.ECredentialType.Anonymous() && !srcIsPublic {
				return nil, fmt.Errorf("the source of a %s->%s sync must either be public, or authorized with a SAS token", cca.fromTo.From(), cca.fromTo.To())
			}
		}
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	sourceTraverser, err := InitResourceTraverser(cca.source, cca.fromTo.From(), &ctx, &srcCredInfo, nil,
		nil, cca.recursive, true, false, func(entityType common.EntityType) {
			if entityType == common.EEntityType.File() {
				atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
			}
		}, nil, cca.s2sPreserveBlobTags, cca.logVerbosity.ToPipelineLogLevel(), cca.cpkOptions)

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
	destinationTraverser, err := InitResourceTraverser(cca.destination, cca.fromTo.To(), &ctx, &dstCredInfo, nil, nil, cca.recursive, true, false, func(entityType common.EntityType) {
		if entityType == common.EEntityType.File() {
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		}
	}, nil, cca.s2sPreserveBlobTags, cca.logVerbosity.ToPipelineLogLevel(), cca.cpkOptions)
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	if sourceTraverser.IsDirectory(true) != destinationTraverser.IsDirectory(true) {
		return nil, errors.New("sync must happen between source and destination of the same type," +
			" e.g. either file <-> file, or directory/container <-> directory/container")
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
	// after making all filters, log any search prefix computed from them
	if ste.JobsAdmin != nil {
		if prefixFilter := FilterSet(filters).GetEnumerationPreFilter(cca.recursive); prefixFilter != "" {
			ste.JobsAdmin.LogToJobLog("Search prefix, which may be used to optimize scanning, is: "+prefixFilter, pipeline.LogInfo) // "May be used" because we don't know here which enumerators will use it
		}
	}

	// decide our folder transfer strategy
	fpo, folderMessage := newFolderPropertyOption(cca.fromTo, cca.recursive, true, filters, cca.preserveSMBInfo, cca.preserveSMBPermissions.IsTruthy()) // sync always acts like stripTopDir=true
	glcm.Info(folderMessage)
	if ste.JobsAdmin != nil {
		ste.JobsAdmin.LogToJobLog(folderMessage, pipeline.LogInfo)
	}

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo)

	// set up the comparator so that the source/destination can be compared
	indexer := newObjectIndexer()
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
		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.mirrorMode).processIfNecessary
		finalize = func() error {
			// schedule every local file that doesn't exist at the destination
			err = indexer.traverse(transferScheduler.scheduleCopyTransfer, filters)
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

		return newSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator, finalize), nil
	default:
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(indexer, transferScheduler.scheduleCopyTransfer, cca.mirrorMode).processIfNecessary

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

			err = indexer.traverse(deleteScheduler, nil)
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

		return newSyncEnumerator(destinationTraverser, sourceTraverser, indexer, filters, comparator, finalize), nil
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
