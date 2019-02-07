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
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		// remove the extra files at the destination that were not present at the source
		deleteScheduler := newSyncLocalDeleteProcessor(cca)
		err = indexer.traverse(deleteScheduler.removeImmediately, nil)
		if err != nil {
			return err
		}

		if !jobInitiated && !deleteScheduler.wasAnyFileDeleted() {
			return errors.New("the source and destination are already in sync")
		} else if !jobInitiated && deleteScheduler.wasAnyFileDeleted() {
			// some files were deleted but no transfer scheduled
			glcm.Exit("the source and destination are now in sync", common.EExitCode.Success())
		}

		cca.setScanningComplete()
		return nil
	}

	return newSyncEnumerator(destinationTraverser, sourceTraverser, indexer, filters, comparator,
		transferScheduler.scheduleCopyTransfer, finalize), nil
}

// upload implies transferring from a local disk to a remote resource
// in this scenario, the local disk (source) is scanned/indexed first
// then the destination is scanned and filtered based on what the destination contains
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
	comparator := newSyncDestinationFilter(indexer, destinationCleaner.removeImmediately)

	finalize := func() error {
		err = indexer.traverse(transferScheduler.scheduleCopyTransfer, filters)
		if err != nil {
			return err
		}

		anyBlobDeleted := destinationCleaner.wasAnyFileDeleted()
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			return err
		}

		if !jobInitiated && !anyBlobDeleted {
			return errors.New("the source and destination are already in sync")
		} else if !jobInitiated && anyBlobDeleted {
			// some files were deleted but no transfer scheduled
			glcm.Exit("the source and destination are now in sync", common.EExitCode.Success())
		}

		cca.setScanningComplete()
		return nil
	}

	return newSyncEnumerator(sourceTraverser, destinationTraverser, indexer, filters, comparator,
		transferScheduler.scheduleCopyTransfer, finalize), nil
}
