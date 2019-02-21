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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
)

// -------------------------------------- Implemented Enumerators -------------------------------------- \\

// download implies transferring from a remote resource to the local disk
// in this scenario, the destination is scanned/indexed first
// then the source is scanned and filtered based on what the destination contains
// we do the local one first because it is assumed that local file systems will be faster to enumerate than remote resources
func newSyncDownloadEnumerator(cca *cookedSyncCmdArgs) (enumerator *syncEnumerator, err error) {
	destinationTraverser, err := newLocalTraverserForSync(cca, false)
	if err != nil {
		return nil, err
	}

	sourceTraverser, err := newBlobTraverserForSync(cca, true)
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	_, isSingleBlob := sourceTraverser.getPropertiesIfSingleBlob()
	_, isSingleFile, _ := destinationTraverser.getInfoIfSingleFile()
	if isSingleBlob != isSingleFile {
		return nil, errors.New("sync must happen between source and destination of the same type: either blob <-> file, or container/virtual directory <-> local directory")
	}

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart)
	includeFilters := buildIncludeFilters(cca.include)
	excludeFilters := buildExcludeFilters(cca.exclude)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)

	// set up the comparator so that the source/destination can be compared
	indexer := newObjectIndexer()
	comparator := newSyncSourceFilter(indexer)

	finalize := func() error {
		// remove the extra files at the destination that were not present at the source
		// we can only know what needs to be deleted when we have FINISHED traversing the remote source
		// since only then can we know which local files definitely don't exist remotely
		deleteScheduler := newSyncLocalDeleteProcessor(cca)
		err = indexer.traverse(deleteScheduler.removeImmediately, nil)
		if err != nil {
			return err
		}

		// let the deletions happen first
		// otherwise if the final part is executed too quickly, we might quit before deletions could finish
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		quitIfInSync(jobInitiated, deleteScheduler.wasAnyFileDeleted(), cca)
		cca.setScanningComplete()
		return nil
	}

	return newSyncEnumerator(destinationTraverser, sourceTraverser, indexer, filters, comparator,
		transferScheduler.scheduleCopyTransfer, finalize), nil
}

// upload implies transferring from a local disk to a remote resource
// in this scenario, the local disk (source) is scanned/indexed first
// then the destination is scanned and filtered based on what the destination contains
// we do the local one first because it is assumed that local file systems will be faster to enumerate than remote resources
func newSyncUploadEnumerator(cca *cookedSyncCmdArgs) (enumerator *syncEnumerator, err error) {
	sourceTraverser, err := newLocalTraverserForSync(cca, true)
	if err != nil {
		return nil, err
	}

	destinationTraverser, err := newBlobTraverserForSync(cca, false)
	if err != nil {
		return nil, err
	}

	// verify that the traversers are targeting the same type of resources
	_, isSingleBlob := destinationTraverser.getPropertiesIfSingleBlob()
	_, isSingleFile, _ := sourceTraverser.getInfoIfSingleFile()
	if isSingleBlob != isSingleFile {
		return nil, errors.New("sync must happen between source and destination of the same type: either blob <-> file, or container/virtual directory <-> local directory")
	}

	transferScheduler := newSyncTransferProcessor(cca, NumOfFilesPerDispatchJobPart)
	includeFilters := buildIncludeFilters(cca.include)
	excludeFilters := buildExcludeFilters(cca.exclude)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)

	// set up the comparator so that the source/destination can be compared
	indexer := newObjectIndexer()
	destinationCleaner, err := newSyncBlobDeleteProcessor(cca)
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate destination cleaner due to: %s", err.Error())
	}
	// when uploading, we can delete remote objects immediately, because as we traverse the remote location
	// we ALREADY have available a complete map of everything that exists locally
	// so as soon as we see a remote destination object we can know whether it exists in the local source
	comparator := newSyncDestinationFilter(indexer, destinationCleaner.removeImmediately)

	finalize := func() error {
		// schedule every local file that doesn't exist at the destination
		err = indexer.traverse(transferScheduler.scheduleCopyTransfer, filters)
		if err != nil {
			return err
		}

		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		quitIfInSync(jobInitiated, destinationCleaner.wasAnyFileDeleted(), cca)
		cca.setScanningComplete()
		return nil
	}

	return newSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator,
		transferScheduler.scheduleCopyTransfer, finalize), nil
}

func quitIfInSync(transferJobInitiated, anyDestinationFileDeleted bool, cca *cookedSyncCmdArgs) {
	if !transferJobInitiated && !anyDestinationFileDeleted {
		cca.reportScanningProgress(glcm, "")
		glcm.Exit("The source and destination are already in sync.", common.EExitCode.Success())
	} else if !transferJobInitiated && anyDestinationFileDeleted {
		// some files were deleted but no transfer scheduled
		cca.reportScanningProgress(glcm, "")
		glcm.Exit("The source and destination are now in sync.", common.EExitCode.Success())
	}
}
