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

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

func (cca *cookedSyncCmdArgs) initEnumerator(ctx context.Context) (enumerator *traverser.SyncEnumerator, err error) {

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, copyJobTemplate)

	// set up the comparator so that the source/destination can be compared
	indexer := traverser.NewObjectIndexer()
	var comparator traverser.ObjectProcessor
	var finalize func() error

	if cca.fromTo.IsUpload() {
		// In this scenario, the local disk (source) is scanned/indexed first because it is assumed that local file systems will be faster to enumerate than remote resources
		// Then the destination is scanned and filtered based on what the destination contains
		destinationCleaner, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
		}
		destCleanerFunc := traverser.NewFpoAwareProcessor(fpo, destinationCleaner.removeImmediately)

		// when uploading, we can delete remote objects immediately, because as we Traverse the remote location
		// we ALREADY have available a complete map of everything that exists locally
		// so as soon as we see a remote destination object we can know whether it exists in the local source

		comparator = newSyncDestinationComparator(indexer, transferScheduler.scheduleCopyTransfer, destCleanerFunc, cca.compareHash, cca.preserveInfo, cca.mirrorMode).processIfNecessary
		finalize = func() error {
			// schedule every local file that doesn't exist at the destination
			err = indexer.Traverse(transferScheduler.scheduleCopyTransfer, filters)
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

		return traverser.NewSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator, finalize), nil
	} else {
		indexer.IsDestinationCaseInsensitive = IsDestinationCaseInsensitive(cca.fromTo)
		// in all other cases (download and S2S), the destination is scanned/indexed first
		// then the source is scanned and filtered based on what the destination contains
		comparator = newSyncSourceComparator(indexer, transferScheduler.scheduleCopyTransfer, cca.compareHash, cca.preserveInfo, cca.mirrorMode).processIfNecessary

		finalize = func() error {
			// remove the extra files at the destination that were not present at the source
			// we can only know what needs to be deleted when we have FINISHED traversing the remote source
			// since only then can we know which local files definitely don't exist remotely
			var deleteScheduler traverser.ObjectProcessor
			switch cca.fromTo.To() {
			case common.ELocation.Blob(), common.ELocation.File(), common.ELocation.FileNFS(), common.ELocation.BlobFS():
				deleter, err := newSyncDeleteProcessor(cca, fpo, copyJobTemplate.DstServiceClient)
				if err != nil {
					return err
				}
				deleteScheduler = traverser.NewFpoAwareProcessor(fpo, deleter.removeImmediately)
			default:
				deleteScheduler = traverser.NewFpoAwareProcessor(fpo, newSyncLocalDeleteProcessor(cca, fpo).removeImmediately)
			}

			err = indexer.Traverse(deleteScheduler, nil)
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

		return traverser.NewSyncEnumerator(destinationTraverser, sourceTraverser, indexer, filters, comparator, finalize), nil
	}
}

func IsDestinationCaseInsensitive(fromTo common.FromTo) bool {
	return fromTo.IsDownload() && runtime.GOOS == "windows"
}

func quitIfInSync(transferJobInitiated, anyDestinationFileDeleted bool, cca *cookedSyncCmdArgs) {
	if !transferJobInitiated {
		cca.reportScanningProgress(glcm, 0)
		if anyDestinationFileDeleted {
			glcm.Exit(func(format common.OutputFormat) string {
				return "The source and destination are now in sync."
			}, common.EExitCode.Success())
		} else {
			glcm.Exit(func(format common.OutputFormat) string {
				return "The source and destination are already in sync."
			}, common.EExitCode.Success())
		}
	}
}
