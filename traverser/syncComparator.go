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

package traverser

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

	if common.AzcopyScanningLogger != nil {
		common.AzcopyScanningLogger.Log(common.LogInfo, out)
	}

	if stdout {
		common.GetLifecycleMgr().Info(out)
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
type SyncDestinationComparator struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner ObjectProcessor

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler ObjectProcessor

	// storing the source objects
	sourceIndex *ObjectIndexer

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

	inodeStore                     *common.InodeStore
	hardlinkRestructureDeleter     ObjectProcessor
	destPendingHardlinkObjects     *ObjectIndexer
	srcPathToInode                 map[string]string
	srcInodeHasIndependentDestFile map[string]bool
	skipSourceSymlinks             bool
	incrementSkippedSymlink        func()
}

type HardlinkSyncOptions struct {
	InodeStore              *common.InodeStore
	RestructureDeleter      ObjectProcessor
	DestinationCleaner      ObjectProcessor
	DestinationIsLocal      bool
	SkipSourceSymlinks      bool
	IncrementSkippedSymlink func()
}

func NewSyncDestinationComparator(
	i *ObjectIndexer,
	copyScheduler,
	cleaner ObjectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime,
	disableComparison bool,
	deleteDestination common.DeleteDestination,
	incrementNotTransferred func(common.EntityType),
	orchestratorOptions *SyncOrchestratorOptions, hardlinks ...HardlinkSyncOptions) *SyncDestinationComparator {
	comp := &SyncDestinationComparator{
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
		(orchestratorOptions.fromTo.From() == common.ELocation.Local() ||
			orchestratorOptions.fromTo.From() == common.ELocation.File())
	if len(hardlinks) > 0 {
		comp.inodeStore = hardlinks[0].InodeStore
		comp.hardlinkRestructureDeleter = hardlinks[0].RestructureDeleter
		comp.destPendingHardlinkObjects = NewObjectIndexer()
		comp.skipSourceSymlinks = hardlinks[0].SkipSourceSymlinks
		comp.incrementSkippedSymlink = hardlinks[0].IncrementSkippedSymlink
	}

	return comp
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *SyncDestinationComparator) ProcessIfNecessary(destinationObject StoredObject) error {
	if f.inodeStore != nil {
		if f.srcPathToInode == nil {
			f.srcPathToInode = buildSrcPathToInode(f.sourceIndex.IndexMap)
		}
		key := destinationObject.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			key = strings.ToLower(key)
		}
		if source, present := f.sourceIndex.IndexMap[key]; present && f.skipSourceSymlinks &&
			(source.EntityType == common.EEntityType.Symlink() || source.hardlinkedSymlink) {
			countSkippedSymlinkAlias(source, f.incrementSkippedSymlink)
			delete(f.sourceIndex.IndexMap, key)
			return nil
		}
		if destinationObject.EntityType == common.EEntityType.Hardlink() && destinationObject.Inode != "" {
			f.destPendingHardlinkObjects.IndexMap[destinationObject.RelativePath] = destinationObject
			return nil
		}
	}
	var sourceObjectInMap StoredObject
	var present bool

	if f.sourceIndex.accessUnderLock {
		f.sourceIndex.rwMutex.RLock()
		sourceObjectInMap, present = f.sourceIndex.IndexMap[destinationObject.RelativePath]
		f.sourceIndex.rwMutex.RUnlock()
	} else {
		sourceObjectInMap, present = f.sourceIndex.IndexMap[destinationObject.RelativePath]
	}

	if !present && f.sourceIndex.IsDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(destinationObject.RelativePath)
		sourceObjectInMap, present = f.sourceIndex.IndexMap[lcRelativePath]
	}

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		defer func() {
			if f.sourceIndex.accessUnderLock {
				f.sourceIndex.rwMutex.Lock()
				delete(f.sourceIndex.IndexMap, destinationObject.RelativePath)
				f.sourceIndex.rwMutex.Unlock()
			} else {
				delete(f.sourceIndex.IndexMap, destinationObject.RelativePath)
			}
		}()

		if f.inodeStore != nil && sourceObjectInMap.EntityType == common.EEntityType.Hardlink() {
			if err := f.normalizeHardlinkTarget(&sourceObjectInMap); err != nil {
				return err
			}
			if sourceObjectInMap.Inode != "" {
				if f.srcInodeHasIndependentDestFile == nil {
					f.srcInodeHasIndependentDestFile = make(map[string]bool)
				}
				f.srcInodeHasIndependentDestFile[sourceObjectInMap.Inode] = true
			}
			if err := f.hardlinkRestructureDeleter(destinationObject); err != nil {
				return err
			}
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if f.useOrchestratorOptions {
			processed, _ := f.processIfNecessaryWithOrchestrator(sourceObjectInMap, destinationObject)
			if processed {
				return nil
			}
		}

		if f.disableComparison {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if f.comparisonHashType != common.ESyncHashType.None() && sourceObjectInMap.EntityType == common.EEntityType.File() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObjectInMap.Md5 == nil {
					if sourceObjectInMap.IsMoreRecentThan(destinationObject, f.preferSMBTime) {
						syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						return f.copyTransferScheduler(sourceObjectInMap)
					} else {
						// skip if dest is more recent
						syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
						return nil
					}
				}

				if !reflect.DeepEqual(sourceObjectInMap.Md5, destinationObject.Md5) {
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)

					// hash inequality = source "newer" in this model.
					return f.copyTransferScheduler(sourceObjectInMap)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}

			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
			return nil
		} else if sourceObjectInMap.IsMoreRecentThan(destinationObject, f.preferSMBTime) {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		// if source is not more recent, we skip the transfer
		if f.incrementNotTransferred != nil && !sourceObjectInMap.IsVirtualPrefix {
			f.incrementNotTransferred(sourceObjectInMap.EntityType)
		}

		// skip if dest is more recent
		syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonTime, false)
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
func (f *SyncDestinationComparator) processIfNecessaryWithOrchestrator(
	sourceObjectInMap StoredObject,
	destinationObject StoredObject) (bool, error) {

	if sourceObjectInMap.EntityType == common.EEntityType.Other() {
		// As of now, for special files at source, fallback to the default behavior
		return false, nil
	}

	// Don't compare the entity type for hardlinks as destination will consider them as files for followed hardlinks
	// This will cause unnecessary transfers
	if sourceObjectInMap.EntityType != common.EEntityType.Hardlink() &&
		sourceObjectInMap.EntityType != destinationObject.EntityType {
		if destinationObject.EntityType == common.EEntityType.Folder() {
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
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonEntityTypeChangedFailedDelete, false)
					if f.incrementNotTransferred != nil {
						// XDM:maybe we should have a different counter for skipped transfers
						f.incrementNotTransferred(sourceObjectInMap.EntityType)
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

	dataChanged, metadataChanged := f.CompareSourceAndDestinationObject(sourceObjectInMap, destinationObject)

	if dataChanged {
		return true, f.copyTransferScheduler(sourceObjectInMap)
	}

	if metadataChanged {
		// If this is true, it means that metadataOnlySync is enabled and metadata has been changed

		// If metadata has changed for a folder, we can simply transfer
		// If metadata has changed for a symlink, both mtime and ctime will change
		// so data change will take care of it.
		if sourceObjectInMap.EntityType == common.EEntityType.Folder() {
			return true, f.copyTransferScheduler(sourceObjectInMap)
		}

		if sourceObjectInMap.EntityType == common.EEntityType.File() {
			// If metadata has changed but data hasn't, we want to just transfer the file properties.
			// This will execute for all entity types other than folders.
			// XDM: What about hardlinks/other entity type here when they are supported?

			// Set size to 0 to indicate that we are not transferring data, only metadata.
			sourceObjectInMap.Size = 0

			// Set entity type to FileProperties to indicate metadata transfer.
			sourceObjectInMap.EntityType = common.EEntityType.FileProperties()

			return true, f.copyTransferScheduler(sourceObjectInMap)
		}
	}

	// Data, metadata or entity type are unchanged, so we skip the transfer.
	if f.incrementNotTransferred != nil && !sourceObjectInMap.IsVirtualPrefix {
		f.incrementNotTransferred(sourceObjectInMap.EntityType)
	}

	syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonNoChangeInLWTorCT, false)
	return true, nil
}

// compareSourceAndDestinationObject compares the source and destination objects to determine if data or metadata has changed.
func (f *SyncDestinationComparator) CompareSourceAndDestinationObject(
	sourceObject StoredObject,
	destinationObject StoredObject,
) (dataChanged, metadataChanged bool) {

	// Check if data has changed by comparing size and modification time
	if sourceObject.EntityType != common.EEntityType.Folder() &&
		sourceObject.Size != destinationObject.Size {
		// Compare file sizes first
		// XDM NOTE: Do we really need to compare sizes here if we are already comparing LWT?
		return true, false
	}

	if sourceObject.LastWriteTime.IsZero() || destinationObject.LastWriteTime.IsZero() {
		// assume it changed as we can't compare
		return true, true
	}

	// Compare last write times with precision tolerance
	if !timeEqual(sourceObject.LastWriteTime, destinationObject.LastWriteTime, f.orchestratorOptions.fromTo.IsNFS()) {
		return true, true
	}

	if !f.orchestratorOptions.metaDataOnlySync {
		// if metadata only sync is not enabled, return early
		// and assume metadata change status to be same as data
		return false, false
	}

	// Cloud-to-cloud (e.g. Azure Files -> Azure Files): ChangeTime is not
	// reliably settable/preserved on the destination, so we cannot use it as the
	// metadata-change signal. Instead we use LastModifiedTime (LMT), which the
	// service bumps on any metadata/property change. At this point, size and
	// LastWriteTime are already known to be equal, so if only the LMT differs we
	// treat it as a metadata-only change.
	if f.orchestratorOptions.fromTo.From() == common.ELocation.File() {
		if !f.orchestratorOptions.lastSuccessfulSyncJobStartTime.IsZero() {

			if sourceObject.LastModifiedTime.IsZero() {
				// invalid LMT
				// assume metadata change
				return false, true
			} else {
				// else check if source or target changed after last successful job start time
				return false, sourceObject.LastModifiedTime.After(f.orchestratorOptions.lastSuccessfulSyncJobStartTime)
			}
		} else {
			// If last successful job start time can't be used, we assume it's changed
			// this will lead to more work but it is necessary to maintain fidelity
			return false, true
		}
	}

	if f.orchestratorOptions.fromTo.IsNFS() {
		// We can't rely on ChangeTime for NFS file share target
		// It is set to the time of migration for the objects
		// In this case, we try to use last successful job start time, if its available.
		if !f.orchestratorOptions.lastSuccessfulSyncJobStartTime.IsZero() {
			// if last succesful job start time is available and valid, compare with change time to decide
			if sourceObject.ChangeTime.IsZero() {
				// invalid change time
				// assume metadata change
				return false, true
			} else {
				// else check if source changed after job start time
				return false, timeAfter(sourceObject.ChangeTime, f.orchestratorOptions.lastSuccessfulSyncJobStartTime, true)
			}
		} else {
			// If last successful job start time can't be used, we assume its changed
			// this will lead to more work but it is necessary to maintain fidelity
			return false, true
		}
	}

	// if its not NFS copy, we assume reliable change time is available in target

	if sourceObject.ChangeTime.IsZero() || destinationObject.ChangeTime.IsZero() {
		return false, true
	}

	// Compare change times with precision tolerance
	if !timeEqual(sourceObject.ChangeTime, destinationObject.ChangeTime, f.orchestratorOptions.fromTo.IsNFS()) {
		return false, true
	}

	// if we reached here, its safe assume that we did valid comparisons and neither data or metadata has changed
	return false, false
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type SyncSourceComparator struct {
	// the processor responsible for scheduling copy transfers
	copyTransferScheduler ObjectProcessor

	// storing the destination objects
	destinationIndex *ObjectIndexer

	comparisonHashType common.SyncHashType

	preferSMBTime     bool
	disableComparison bool

	// Function to increment files/folders not transferred as a result of no change since last sync.
	incrementNotTransferred func(common.EntityType)

	inodeStore                     *common.InodeStore
	hardlinkRestructureDeleter     ObjectProcessor
	destinationCleaner             ObjectProcessor
	srcPendingHardlinkObjects      *ObjectIndexer
	dstPathToInode                 map[string]string
	srcInodeHasIndependentDestFile map[string]bool
	destinationIsLocal             bool
	skipSourceSymlinks             bool
	incrementSkippedSymlink        func()
}

func NewSyncSourceComparator(
	i *ObjectIndexer,
	copyScheduler ObjectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime,
	disableComparison bool,
	incrementNotTransferred func(common.EntityType), hardlinks ...HardlinkSyncOptions) *SyncSourceComparator {
	comp := &SyncSourceComparator{
		destinationIndex:        i,
		copyTransferScheduler:   copyScheduler,
		preferSMBTime:           preferSMBTime,
		disableComparison:       disableComparison,
		comparisonHashType:      comparisonHashType,
		incrementNotTransferred: incrementNotTransferred,
	}
	if len(hardlinks) > 0 {
		comp.inodeStore = hardlinks[0].InodeStore
		comp.hardlinkRestructureDeleter = hardlinks[0].RestructureDeleter
		comp.destinationCleaner = hardlinks[0].DestinationCleaner
		comp.destinationIsLocal = hardlinks[0].DestinationIsLocal
		comp.skipSourceSymlinks = hardlinks[0].SkipSourceSymlinks
		comp.incrementSkippedSymlink = hardlinks[0].IncrementSkippedSymlink
		comp.srcPendingHardlinkObjects = NewObjectIndexer()
	}
	return comp
}

// it will only transfer source items that are:
//  1. not present in the map
//  2. present but is more recent than the entry in the map
//
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *SyncSourceComparator) ProcessIfNecessary(sourceObject StoredObject) error {
	if f.inodeStore != nil && f.dstPathToInode == nil {
		f.dstPathToInode = buildSrcPathToInode(f.destinationIndex.IndexMap)
	}
	relPath := sourceObject.RelativePath

	if f.destinationIndex.IsDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}
	destinationObjectInMap, present := f.destinationIndex.IndexMap[relPath]

	if f.skipSourceSymlinks && (sourceObject.EntityType == common.EEntityType.Symlink() || sourceObject.hardlinkedSymlink) {
		countSkippedSymlinkAlias(sourceObject, f.incrementSkippedSymlink)
		delete(f.destinationIndex.IndexMap, relPath)
		return nil
	}

	if f.inodeStore != nil && sourceObject.EntityType == common.EEntityType.Hardlink() && sourceObject.Inode != "" {
		f.srcPendingHardlinkObjects.IndexMap[relPath] = sourceObject
		return nil
	}

	if present {
		defer delete(f.destinationIndex.IndexMap, relPath)

		if f.inodeStore != nil && destinationObjectInMap.EntityType == common.EEntityType.Hardlink() {
			if err := f.hardlinkRestructureDeleter(destinationObjectInMap); err != nil {
				return err
			}
			return f.copyTransferScheduler(sourceObject)
		}

		// if destination is stale, schedule source for transfer
		if f.disableComparison {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObject)
		}

		if f.comparisonHashType != common.ESyncHashType.None() && sourceObject.EntityType == common.EEntityType.File() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObject.Md5 == nil {
					if sourceObject.IsMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
						syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						return f.copyTransferScheduler(sourceObject)
					} else {
						// skip if dest is more recent
						syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
						return nil
					}
				}

				if !reflect.DeepEqual(sourceObject.Md5, destinationObjectInMap.Md5) {
					// hash inequality = source "newer" in this model.
					syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
					return f.copyTransferScheduler(sourceObject)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}

			syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
			return nil
		} else if sourceObject.IsMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
			// if destination is stale, schedule source
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			return f.copyTransferScheduler(sourceObject)
		}

		// Neither data nor metadata for the file has changed, hence file is not transferred.
		if f.incrementNotTransferred != nil {
			f.incrementNotTransferred(sourceObject.EntityType)
		}

		// skip if dest is more recent
		syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonTime, false)
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
