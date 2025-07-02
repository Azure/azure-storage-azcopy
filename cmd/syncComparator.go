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

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const (
	syncSkipReasonTime                          = "the source has an older LMT than the destination"
	syncSkipReasonTimeAndMissingHash            = "the source lacks an associated hash (please upload with --put-md5 for hash comparison) and has an older LMT than the destination"
	syncSkipReasonMissingHash                   = "the source lacks an associated hash; please upload with --put-md5"
	syncSkipReasonSameHash                      = "the source has the same hash"
	syncOverwriteReasonNewerHash                = "the source has a differing hash"
	syncOverwriteReasonNewerLMT                 = "the source is more recent than the destination"
	syncOverwriteReasonNewerLMTAndMissingHash   = "the source lacks an associated hash (please upload with --put-md5 for hash comparison) and is more recent than the destination"
	syncStatusSkipped                           = "skipped"
	syncStatusOverwritten                       = "overwritten"
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

	orchestratorOptions *syncOrchestratorOptions
}

func newSyncDestinationComparator(
	i *objectIndexer,
	copyScheduler,
	cleaner objectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime bool,
	disableComparison bool,
	deleteDestination common.DeleteDestination,
	incrementNotTransferred func(common.EntityType),
	orchestratorOptions *syncOrchestratorOptions,
) *syncDestinationComparator {

	return &syncDestinationComparator{
		sourceIndex:             i,
		copyTransferScheduler:   copyScheduler,
		destinationCleaner:      cleaner,
		preferSMBTime:           preferSMBTime,
		disableComparison:       disableComparison,
		deleteDestination:       deleteDestination,
		comparisonHashType:      comparisonHashType,
		incrementNotTransferred: incrementNotTransferred,
		orchestratorOptions:     orchestratorOptions,
	}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) processIfNecessary(destinationObject StoredObject) error {
	sourceObjectInMap, present := f.sourceIndex.indexMap[destinationObject.relativePath]
	if !present && f.sourceIndex.isDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(destinationObject.relativePath)
		sourceObjectInMap, present = f.sourceIndex.indexMap[lcRelativePath]
	}

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		defer delete(f.sourceIndex.indexMap, destinationObject.relativePath)

		if f.disableComparison {
			syncComparatorLog(sourceObjectInMap.relativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if UseSyncOrchestrator && f.orchestratorOptions != nil && f.orchestratorOptions.valid {
			// Use optimized comparison logic using source and target timestamps and sizes
			typeChanged, dataChanged, metadataChanged, valid := f.compareSourceAndDestinationObject(sourceObjectInMap, destinationObject)

			if valid {

				if typeChanged {
					// if the entity type has changed, we will not be able to transfer the file
					// unless the destination object is deleted first
					// XDM: Does destination support different entity type with same name?
					if f.deleteDestination == common.EDeleteDestination.True() {
						err := f.destinationCleaner(destinationObject)
						if err != nil {
							syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonEntityTypeChangedFailedDelete, false)
							return nil
						}
					} else if f.deleteDestination == common.EDeleteDestination.False() {
						// If deleteDestination is set to false, we cannot transfer the file
						// because the destination object is not compatible with the source object.
						syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonEntityTypeChangedNoDelete, false)
						return nil
					}
				}

				if metadataChanged && sourceObjectInMap.entityType.IsFolder() {
					dataChanged = true // If metadata has changed for a folder, we consider data changed as well.
				}

				if metadataChanged && !f.orchestratorOptions.metaDataOnlySync {
					dataChanged = true // If metadata has changed and metaDataOnlySync is not enabled, we consider data changed as well.
				}

				if dataChanged || typeChanged {
					return f.copyTransferScheduler(sourceObjectInMap)
				}

				if metadataChanged {
					// This will execute for all entity types other than folders.
					// If metadata has changed but data hasn't, we still want to transfer the file\folder properties.
					// XDM: Do we check symlinks entity type here?
					fmt.Printf("Metadata changed for %s, but data is the same. Transferring metadata only.\n", sourceObjectInMap.relativePath)
					sourceObjectInMap.size = 0                                         // Set size to 0 to indicate that we are not transferring data, only metadata.
					sourceObjectInMap.entityType = common.EEntityType.FileProperties() // Set entity type to FileProperties to indicate metadata transfer.
					return f.copyTransferScheduler(sourceObjectInMap)
				}

				if !dataChanged && !metadataChanged && !typeChanged {
					if f.incrementNotTransferred != nil {
						f.incrementNotTransferred(sourceObjectInMap.entityType)
					}
					syncComparatorLog(sourceObjectInMap.relativePath, syncStatusSkipped, syncSkipReasonNoChangeInLWTorCT, false)
					return nil
				}
			}
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

		// Neither data nor metadata for the file has changed, hence file is not transferred.
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

// compareSourceAndDestinationObject compares the source and destination objects to determine if data or metadata has changed.
func (f *syncDestinationComparator) compareSourceAndDestinationObject(
	sourceObject StoredObject,
	destinationObject StoredObject,
) (typeChanged, dataChanged, metadataChanged, valid bool) {
	// Check if data has changed by comparing size and modification time
	typeChanged = false
	dataChanged = false
	metadataChanged = false
	valid = true

	if sourceObject.entityType != destinationObject.entityType {
		typeChanged = true
		dataChanged = true     // If types differ, we consider data changed
		metadataChanged = true // Metadata is also considered changed if types differ
		fmt.Printf("Entity type changed for %s: source type %s, destination type %s\n", sourceObject.relativePath, sourceObject.entityType, destinationObject.entityType)
		return typeChanged, dataChanged, metadataChanged, valid
	}

	if !sourceObject.entityType.IsFolder() {
		// Compare file sizes first
		// XDM NOTE: Do we really need to compare sizes here if we are already comparing LWT?
		if sourceObject.size != destinationObject.size {
			dataChanged = true
			fmt.Printf("File size changed for %s: source size %d, destination size %d\n", sourceObject.relativePath, sourceObject.size, destinationObject.size)
			return typeChanged, dataChanged, metadataChanged, valid
		}
	}

	if sourceObject.lastWriteTime.IsZero() || destinationObject.lastWriteTime.IsZero() {
		valid = false
		fmt.Printf("Last write time is zero for %s: source LWT %v, destination LWT %v\n",
			sourceObject.relativePath, common.TimeAsRFC3339String(sourceObject.lastWriteTime),
			common.TimeAsRFC3339String(destinationObject.lastWriteTime))
		return typeChanged, dataChanged, metadataChanged, valid
	}

	// Compare last write times
	if sourceObject.lastWriteTime.Compare(destinationObject.lastWriteTime) != 0 {
		dataChanged = true
		fmt.Printf("Last write time changed for %s: source LWT %v, destination LWT %v\n",
			sourceObject.relativePath, common.TimeAsRFC3339String(sourceObject.lastWriteTime),
			common.TimeAsRFC3339String(destinationObject.lastWriteTime))
		return typeChanged, dataChanged, metadataChanged, valid
	}

	if sourceObject.changeTime.IsZero() || destinationObject.changeTime.IsZero() {
		// If change time is not available, we cannot determine metadata changes
		metadataChanged = true
		fmt.Printf("Change time is zero for %s: source CT %v, destination CT %v\n",
			sourceObject.relativePath, common.TimeAsRFC3339String(sourceObject.changeTime),
			common.TimeAsRFC3339String(destinationObject.changeTime))
		return typeChanged, dataChanged, metadataChanged, valid
	}

	// Compare change times
	if sourceObject.changeTime.Compare(destinationObject.changeTime) != 0 {
		metadataChanged = true
		fmt.Printf("Change time changed for %s: source CT %v, destination CT %v\n",
			sourceObject.relativePath, common.TimeAsRFC3339String(sourceObject.changeTime),
			common.TimeAsRFC3339String(destinationObject.changeTime))
		return typeChanged, dataChanged, metadataChanged, valid
	}

	fmt.Printf("No changes detected for %s: source LWT %v, CT %v, size %d; destination LWT %v, CT %v, size %d\n",
		sourceObject.relativePath,
		common.TimeAsRFC3339String(sourceObject.lastWriteTime), common.TimeAsRFC3339String(sourceObject.changeTime), sourceObject.size,
		common.TimeAsRFC3339String(destinationObject.lastWriteTime), common.TimeAsRFC3339String(destinationObject.changeTime), destinationObject.size)
	return typeChanged, dataChanged, metadataChanged, valid
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

	preferSMBTime           bool
	disableComparison       bool
	incrementNotTransferred func(common.EntityType)
}

func newSyncSourceComparator(i *objectIndexer, copyScheduler objectProcessor, comparisonHashType common.SyncHashType, preferSMBTime, disableComparison bool, incrementNotTransferred func(common.EntityType)) *syncSourceComparator {
	return &syncSourceComparator{destinationIndex: i, copyTransferScheduler: copyScheduler, preferSMBTime: preferSMBTime, disableComparison: disableComparison, comparisonHashType: comparisonHashType, incrementNotTransferred: incrementNotTransferred}
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
		} else {
			// Neither data nor metadata for the file has changed, hence file is not transferred.
			if f.incrementNotTransferred != nil {
				f.incrementNotTransferred(sourceObject.entityType)
			}
		}
		// skip if dest is more recent
		syncComparatorLog(sourceObject.relativePath, syncStatusSkipped, syncSkipReasonTime, false)
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
