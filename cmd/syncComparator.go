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
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const (
	syncSkipReasonTime                        = "the source has an older LMT than the destination"
	syncSkipReasonTimeAndMissingHash          = "the source lacks an associated hash (please upload with --put-md5 for hash comparison) and has an older LMT than the destination"
	syncSkipReasonMissingHash                 = "the source lacks an associated hash; please upload with --put-md5"
	syncSkipReasonSameHash                    = "the source has the same hash"
	syncOverwriteReasonNewerHash              = "the source has a differing hash"
	syncOverwriteReasonNewerLMT               = "the source is more recent than the destination"
	syncOverwriteReasonNewerLMTAndMissingHash = "the source lacks an associated hash (please upload with --put-md5 for hash comparison) and is more recent than the destination"
	syncStatusSkipped                         = "skipped"
	syncStatusOverwritten                     = "overwritten"

	// Reasons for skipping an object during sync comparison based on source and destination LWT or ChangeTime
	// This comparison is used only for SyncOrchestrator
	syncSkipReasonNoChangeInLWTorCT             = "the source has no change in LastWriteTime or ChangeTime compared to the destination"
	syncSkipReasonEntityTypeChangedNoDelete     = "the source object type has changed compared to the destination and --delete-destination is false"
	syncSkipReasonEntityTypeChangedFailedDelete = "the source object type has changed compared to the destination and delete destination failed"
)

func syncComparatorLog(fileName, status, skipReason string, stdout bool) {
	out := fmt.Sprintf("File %s was %s because %s", fileName, status, skipReason)

	if azcopyScanningLogger != nil {
		azcopyScanningLogger.Log(common.LogInfo, out)
	}

	if stdout {
		glcm.Info(out)
	}
}

// timeEqual compares two timestamps with precision tolerance to handle filesystem precision differences.
// It truncates both times to the specified precision to handle precision differences
func timeEqual(t1, t2 time.Time, useMicroSecPrecision bool) bool {
	// Truncate both times to the specified precision to handle precision differences
	if useMicroSecPrecision {
		return t1.Truncate(time.Microsecond).Equal(t2.Truncate(time.Microsecond))
	}

	return t1.Equal(t2)
}

// timeAfter compares two timestamps with precision tolerance to handle filesystem precision differences.
// It truncates both times to the specified precision to handle precision differences
func timeAfter(t1, t2 time.Time, useMicroSecPrecision bool) bool {
	// Truncate both times to the specified precision to handle precision differences
	if useMicroSecPrecision {
		return t1.Truncate(time.Microsecond).After(t2.Truncate(time.Microsecond))
	}

	return t1.After(t2)
}

// with the help of an objectIndexer containing the source objects
// find out the destination objects that should be transferred
// in other words, this should be used when destination is being enumerated secondly
type syncDestinationComparator struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner objectProcessor

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the source objects
	sourceIndex *objectIndexer

	comparisonHashType common.SyncHashType

	preferSMBTime     bool
	disableComparison bool
	deleteDestination common.DeleteDestination

	// Function to increment files/folders not transferred as a result of no change since last sync.
	incrementNotTransferred func(common.EntityType)

	orchestratorOptions *SyncOrchestratorOptions

	// This flag helps to decide if orchestrator options can be used for comparison
	// pre-computing this flag helps to avoid redoing it for each object
	useOrchestratorOptions bool
}

func newSyncDestinationComparator(
	i *objectIndexer,
	copyScheduler,
	cleaner objectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime,
	disableComparison bool,
	deleteDestination common.DeleteDestination,
	incrementNotTransferred func(common.EntityType),
	orchestratorOptions *SyncOrchestratorOptions) *syncDestinationComparator {
	comp := &syncDestinationComparator{
		sourceIndex:             i,
		copyTransferScheduler:   copyScheduler,
		destinationCleaner:      cleaner,
		preferSMBTime:           preferSMBTime,
		disableComparison:       disableComparison,
		comparisonHashType:      comparisonHashType,
		deleteDestination:       deleteDestination,
		incrementNotTransferred: incrementNotTransferred,
		orchestratorOptions:     orchestratorOptions,
	}

	comp.useOrchestratorOptions = UseSyncOrchestrator && IsSyncOrchestratorOptionsValid(orchestratorOptions) &&
		orchestratorOptions.fromTo.From() == common.ELocation.Local()

	return comp
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) processIfNecessary(destinationObject StoredObject) error {
	var sourceObjectInMap StoredObject
	var present bool

	if f.sourceIndex.accessUnderLock {
		f.sourceIndex.rwMutex.RLock()
		sourceObjectInMap, present = f.sourceIndex.indexMap[destinationObject.relativePath]
		f.sourceIndex.rwMutex.RUnlock()
	} else {
		sourceObjectInMap, present = f.sourceIndex.indexMap[destinationObject.relativePath]
	}

	if !present && f.sourceIndex.isDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(destinationObject.relativePath)
		sourceObjectInMap, present = f.sourceIndex.indexMap[lcRelativePath]
	}

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		defer func() {
			if f.sourceIndex.accessUnderLock {
				f.sourceIndex.rwMutex.Lock()
				delete(f.sourceIndex.indexMap, destinationObject.relativePath)
				f.sourceIndex.rwMutex.Unlock()
			} else {
				delete(f.sourceIndex.indexMap, destinationObject.relativePath)
			}
		}()

		if f.useOrchestratorOptions {
			processed, _ := f.processIfNecessaryWithOrchestrator(sourceObjectInMap, destinationObject)
			if processed {
				return nil
			}
		}

		if f.disableComparison {
			syncComparatorLog(sourceObjectInMap.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if f.comparisonHashType != common.ESyncHashType.None() && sourceObjectInMap.entityType == common.EEntityType.File() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObjectInMap.md5 == nil {
					if sourceObjectInMap.isMoreRecentThan(destinationObject, f.preferSMBTime) {
						syncComparatorLog(sourceObjectInMap.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						return f.copyTransferScheduler(sourceObjectInMap)
					} else {
						// skip if dest is more recent
						syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
						return nil
					}
				}

				if !reflect.DeepEqual(sourceObjectInMap.md5, destinationObject.md5) {
					syncComparatorLog(sourceObjectInMap.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)

					// hash inequality = source "newer" in this model.
					return f.copyTransferScheduler(sourceObjectInMap)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}

			syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
			return nil
		} else if sourceObjectInMap.isMoreRecentThan(destinationObject, f.preferSMBTime) {
			syncComparatorLog(sourceObjectInMap.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		// if source is not more recent, we skip the transfer
		if f.incrementNotTransferred != nil {
			f.incrementNotTransferred(sourceObjectInMap.entityType)
		}

		// skip if dest is more recent
		syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonTime, false)
	} else {
		// purposefully ignore the error from destinationCleaner
		// it's a tolerable error, since it just means some extra destination object might hang around a bit longer
		_ = f.destinationCleaner(destinationObject)
	}

	return nil
}

// processIfNecessaryWithOrchestrator processes the source and destination objects using the SyncOrchestrator options.
// boolean return value indicates whether the object is processed (transferred or skipped).
// error return value indicates if there was an error during processing.
func (f *syncDestinationComparator) processIfNecessaryWithOrchestrator(
	sourceObjectInMap StoredObject,
	destinationObject StoredObject) (bool, error) {

	if sourceObjectInMap.entityType == common.EEntityType.Other() {
		// As of now, for special files at source, fallback to the default behavior
		return false, nil
	}

	// Don't compare the entity type for hardlinks as destination will consider them as files for followed hardlinks
	// This will cause unnecessary transfers
	if sourceObjectInMap.entityType != common.EEntityType.Hardlink() &&
		sourceObjectInMap.entityType != destinationObject.entityType {
		if destinationObject.entityType == common.EEntityType.Folder() {
			// This entity type compararison is necessary for SyncOrchestrator as we have the visibility
			// of a deleted object in the source only once during the directory non-recusrive enumeration.
			// The default flow keeps all the objects in the memory and has the complete view of the source
			// to do the proper deletion.
			// Sync orchestator needs to take care of deletion of folder recursively the first chance it gets.

			// If the destination object is a folder and the source object type has changed,
			// we need to handle it based on the deleteDestination option.
			// STE would do the proper deletion of non-folder destination objects but not folders.
			// If the destination object is a folder, STE will delete it only if its empty which
			// may not be the case always. here we handle the recursive deletion of the destination folder

			// if the entity type has changed, we will not be able to transfer the file
			// unless the destination folder is deleted first
			// XDM: Does destination support different entity type with same name?
			if f.deleteDestination == common.EDeleteDestination.True() {
				err := f.destinationCleaner(destinationObject)
				if err != nil {
					syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonEntityTypeChangedFailedDelete, false)
					if f.incrementNotTransferred != nil {
						// XDM:maybe we should have a different counter for skipped transfers
						f.incrementNotTransferred(sourceObjectInMap.entityType)
					}
					return true, nil
				}
			} else if f.deleteDestination == common.EDeleteDestination.False() {
				// If deleteDestination is set to false, we cannot transfer the file
				// because the destination object is not compatible with the source object.

				// Its better to let STE handle the behavior
			} else if f.deleteDestination == common.EDeleteDestination.Prompt() {
				// Ideally, we should let the default behavior handle this case as well.
				// But for now, we will panic as this should not happen for sync orchestrator compare.
				panic("unsupported delete destination option for sync orchestrator compare " + f.deleteDestination.String())
			}
		}

		// if its files, STE would do the right thing here and deletes the file at destination
		return true, f.copyTransferScheduler(sourceObjectInMap)
	}

	dataChanged, metadataChanged := f.compareSourceAndDestinationObject(sourceObjectInMap, destinationObject)

	if dataChanged {
		return true, f.copyTransferScheduler(sourceObjectInMap)
	}

	if metadataChanged {
		// If this is true, it means that metadataOnlySync is enabled and metadata has been changed

		// If metadata has changed for a folder, we can simply transfer
		// If metadata has changed for a symlink, both mtime and ctime will change
		// so data change will take care of it.
		if sourceObjectInMap.entityType == common.EEntityType.Folder() {
			return true, f.copyTransferScheduler(sourceObjectInMap)
		}

		if sourceObjectInMap.entityType == common.EEntityType.File() {
			// If metadata has changed but data hasn't, we want to just transfer the file properties.
			// This will execute for all entity types other than folders.
			// XDM: What about hardlinks/other entity type here when they are supported?

			// Set size to 0 to indicate that we are not transferring data, only metadata.
			sourceObjectInMap.size = 0

			// Set entity type to FileProperties to indicate metadata transfer.
			sourceObjectInMap.entityType = common.EEntityType.FileProperties()

			return true, f.copyTransferScheduler(sourceObjectInMap)
		}
	}

	// Data, metadata or entity type are unchanged, so we skip the transfer.
	if f.incrementNotTransferred != nil {
		f.incrementNotTransferred(sourceObjectInMap.entityType)
	}

	syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonNoChangeInLWTorCT, false)
	return true, nil
}

// compareSourceAndDestinationObject compares the source and destination objects to determine if data or metadata has changed.
func (f *syncDestinationComparator) compareSourceAndDestinationObject(
	sourceObject StoredObject,
	destinationObject StoredObject,
) (dataChanged, metadataChanged bool) {

	// Check if data has changed by comparing size and modification time

	if sourceObject.entityType != common.EEntityType.Folder() &&
		sourceObject.size != destinationObject.size {
		// Compare file sizes first
		// XDM NOTE: Do we really need to compare sizes here if we are already comparing LWT?
		return true, false
	}

	if sourceObject.lastWriteTime.IsZero() || destinationObject.lastWriteTime.IsZero() {
		// assume it changed as we can't compare
		return true, true
	}

	// Compare last write times with precision tolerance
	if !timeEqual(sourceObject.lastWriteTime, destinationObject.lastWriteTime, common.IsNFSCopy()) {
		return true, true
	}

	if !f.orchestratorOptions.metaDataOnlySync {
		// if metadata only sync is not enabled, return early
		// and assume metadata change status to be same as data
		return false, false
	}

	if common.IsNFSCopy() {
		// We can't rely on ChangeTime for NFS file share target
		// It is set to the time of migration for the objects
		// In this case, we try to use last successful job start time, if its available.
		if f.orchestratorOptions.optimizeEnumerationByCTime && !f.orchestratorOptions.lastSuccessfulSyncJobStartTime.IsZero() {
			// if last succesful job start time is available and valid, compare with change time to decide
			if sourceObject.changeTime.IsZero() {
				// invalid change time
				// assume metadata change
				return false, true
			} else {
				// else check if source changed after job start time
				return false, timeAfter(sourceObject.changeTime, f.orchestratorOptions.lastSuccessfulSyncJobStartTime, true)
			}
		} else {
			// If last successful job start time can't be used, we assume its changed
			// this will lead to more work but it is necessary to maintain fidelity
			return false, true
		}
	}

	// if its not NFS copy, we assume reliable change time is available in target

	if sourceObject.changeTime.IsZero() || destinationObject.changeTime.IsZero() {
		return false, true
	}

	// Compare change times with precision tolerance
	if !timeEqual(sourceObject.changeTime, destinationObject.changeTime, common.IsNFSCopy()) {
		return false, true
	}

	// if we reached here, its safe assume that we did valid comparisons and neither data or metadata has changed
	return false, false
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type syncSourceComparator struct {
	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the destination objects
	destinationIndex *objectIndexer

	comparisonHashType common.SyncHashType

	preferSMBTime     bool
	disableComparison bool

	// Function to increment files/folders not transferred as a result of no change since last sync.
	incrementNotTransferred func(common.EntityType)
}

func newSyncSourceComparator(
	i *objectIndexer,
	copyScheduler objectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime,
	disableComparison bool,
	incrementNotTransferred func(common.EntityType)) *syncSourceComparator {
	return &syncSourceComparator{
		destinationIndex:        i,
		copyTransferScheduler:   copyScheduler,
		preferSMBTime:           preferSMBTime,
		disableComparison:       disableComparison,
		comparisonHashType:      comparisonHashType,
		incrementNotTransferred: incrementNotTransferred,
	}
}

// it will only transfer source items that are:
//  1. not present in the map
//  2. present but is more recent than the entry in the map
//
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) processIfNecessary(sourceObject StoredObject) error {
	relPath := sourceObject.relativePath

	if f.destinationIndex.isDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}
	destinationObjectInMap, present := f.destinationIndex.indexMap[relPath]

	if present {
		defer delete(f.destinationIndex.indexMap, relPath)

		// if destination is stale, schedule source for transfer
		if f.disableComparison {
			syncComparatorLog(sourceObject.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObject)
		}

		if f.comparisonHashType != common.ESyncHashType.None() && sourceObject.entityType == common.EEntityType.File() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObject.md5 == nil {
					if sourceObject.isMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
						syncComparatorLog(sourceObject.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						return f.copyTransferScheduler(sourceObject)
					} else {
						// skip if dest is more recent
						syncComparatorLog(sourceObject.relativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
						return nil
					}
				}

				if !reflect.DeepEqual(sourceObject.md5, destinationObjectInMap.md5) {
					// hash inequality = source "newer" in this model.
					syncComparatorLog(sourceObject.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
					return f.copyTransferScheduler(sourceObject)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}

			syncComparatorLog(sourceObject.relativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
			return nil
		} else if sourceObject.isMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
			// if destination is stale, schedule source
			syncComparatorLog(sourceObject.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			return f.copyTransferScheduler(sourceObject)
		}

		// Neither data nor metadata for the file has changed, hence file is not transferred.
		if f.incrementNotTransferred != nil {
			f.incrementNotTransferred(sourceObject.entityType)
		}

		// skip if dest is more recent
		syncComparatorLog(sourceObject.relativePath, syncStatusSkipped, syncSkipReasonTime, false)
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
