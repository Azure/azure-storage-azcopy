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
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (cca *cookedSyncCmdArgs) initEnumerator(ctx context.Context) (enumerator *syncEnumerator, err error) {
	src, err := appendSASIfNecessary(cca.source, cca.sourceSAS)
	if err != nil {
		return nil, err
	}

	dst, err := appendSASIfNecessary(cca.destination, cca.destinationSAS)
	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	sourceTraverser, err := initResourceTraverser(src, cca.fromTo.From(), &ctx, &cca.credentialInfo,
		nil, nil, cca.recursive, true, func() {
			atomic.AddUint64(&cca.atomicSourceFilesScanned, 1)
		})

	if err != nil {
		return nil, err
	}

	// TODO: enable symlink support in a future release after evaluating the implications
	// GetProperties is enabled by default as sync supports both upload and download.
	// This property only supports Files and S3 at the moment, but provided that Files sync is coming soon, enable to avoid stepping on Files sync work
	destinationTraverser, err := initResourceTraverser(dst, cca.fromTo.To(), &ctx, &cca.credentialInfo,
		nil, nil, cca.recursive, true, func() {
			atomic.AddUint64(&cca.atomicDestinationFilesScanned, 1)
		})
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	if sourceTraverser.isDirectory(true) != destinationTraverser.isDirectory(true) {
		return nil, errors.New("sync must happen between source and destination of the same type, e.g. either file <-> file, or directory/container <-> directory/container")
	}

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart)

	// set up the filters in the right order
	// Note: includeFilters and includeAttrFilters are ANDed
	// They must both pass to get the file included
	// Same rule applies to excludeFilters and excludeAttrFilters
	filters := buildIncludeFilters(cca.include)
	if cca.fromTo.From() == common.ELocation.Local() {
		includeAttrFilters := buildAttrFilters(cca.includeFileAttributes, src, true)
		filters = append(filters, includeAttrFilters...)
	}

	filters = append(filters, buildExcludeFilters(cca.exclude, false)...)
	if cca.fromTo.From() == common.ELocation.Local() {
		excludeAttrFilters := buildAttrFilters(cca.excludeFileAttributes, src, false)
		filters = append(filters, excludeAttrFilters...)
	}

	// set up the comparator so that the source/destination can be compared
	indexer := newObjectIndexer()
	var comparator objectProcessor
	var finalize func() error

	switch cca.fromTo {
	case common.EFromTo.LocalBlob():
		// upload implies transferring from a local disk to a remote resource
		// in this scenario, the local disk (source) is scanned/indexed first
		// then the destination is scanned and filtered based on what the destination contains
		// we do the local one first because it is assumed that local file systems will be faster to enumerate than remote resources
		destinationCleaner, err := newSyncDeleteProcessor(cca)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}

		// when uploading, we can delete remote objects immediately, because as we traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source
		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, destinationCleaner.removeImmediately).processIfNecessary
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
		comparator = newSyncSourceComparator(indexer, transferScheduler.scheduleCopyTransfer).processIfNecessary

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
				deleteScheduler = deleter.removeImmediately
			default:
				deleteScheduler = newSyncLocalDeleteProcessor(cca).removeImmediately
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
