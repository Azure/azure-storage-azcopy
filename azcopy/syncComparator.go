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

package azcopy

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"reflect"
	"strings"
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

// with the help of an objectIndexer containing the source objects
// find out the destination objects that should be transferred
// in other words, this should be used when destination is being enumerated secondly
type syncDestinationComparator struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner traverser.ObjectProcessor

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler traverser.ObjectProcessor

	// storing the source objects
	sourceIndex *traverser.ObjectIndexer

	comparisonHashType common.SyncHashType

	preferSMBTime     bool
	disableComparison bool

	destPendingHardlinkObjects traverser.ObjectIndexer
}

type NfsHardlinkManager struct {
	pendingHardlinkObjects *traverser.ObjectIndexer
}

func NewSyncDestinationComparator(i *traverser.ObjectIndexer, copyScheduler, cleaner traverser.ObjectProcessor, comparisonHashType common.SyncHashType, preferSMBTime, disableComparison bool, nfsOpts *NfsHardlinkManager) *syncDestinationComparator {
	return &syncDestinationComparator{sourceIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, preferSMBTime: preferSMBTime, disableComparison: disableComparison, comparisonHashType: comparisonHashType, destPendingHardlinkObjects: *nfsOpts.pendingHardlinkObjects}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) ProcessIfNecessary(destinationObject traverser.StoredObject) error {

	if destinationObject.EntityType == common.EEntityType.Hardlink() {

		// we will process hardlinks in a special way because we need to make sure the relationship is not broken.
		// So we will not directly schedule the copy transfer here, instead we will put it in a separate map and
		// process it after we have processed all the other objects. This is to make sure we have the complete picture of
		// what the destination hardlink relationships look like before we decide whether we need to break any of them.
		f.destPendingHardlinkObjects.IndexMap[destinationObject.RelativePath] = destinationObject
		return nil
	}

	sourceObjectInMap, present := f.sourceIndex.IndexMap[destinationObject.RelativePath]
	if !present && f.sourceIndex.IsDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(destinationObject.RelativePath)
		sourceObjectInMap, present = f.sourceIndex.IndexMap[lcRelativePath]
	}

	if present {
		// Clean up the index map once we start processing this path
		defer delete(f.sourceIndex.IndexMap, destinationObject.RelativePath)

		// 1. FORCE OVERWRITE: If comparison is disabled, schedule transfer immediately
		if f.disableComparison {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if sourceObjectInMap.EntityType == common.EEntityType.Hardlink() {
			// Case: Destination is a File/Folder/Symlink, but Source is now a Hardlink
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, "EntityTypeMismatch", false)

			// Delete the independent file/folder/symlink to allow link creation
			_ = f.destinationCleaner(destinationObject)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		// 3. CONTENT VALIDATION: Hash Comparison
		// If it's a file and hash comparison is enabled, we check data integrity.
		if f.comparisonHashType != common.ESyncHashType.None() && sourceObjectInMap.EntityType == common.EEntityType.File() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObjectInMap.Md5 == nil {
					// Fallback to LMT if hashes are missing
					if sourceObjectInMap.IsMoreRecentThan(destinationObject, f.preferSMBTime) {
						syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						return f.copyTransferScheduler(sourceObjectInMap)
					} else {
						syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
						return nil
					}
				}

				// If hashes exist but differ, the source is considered "newer/stale"
				if !reflect.DeepEqual(sourceObjectInMap.Md5, destinationObject.Md5) {
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
					return f.copyTransferScheduler(sourceObjectInMap)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}

			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
			return nil

			// 4. CONTENT VALIDATION: Last Modified Time (LMT)
			// Default sync behavior: check if source is newer than destination
		} else if sourceObjectInMap.IsMoreRecentThan(destinationObject, f.preferSMBTime) {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		// Default: Destination is up-to-date or newer
		syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonTime, false)

	} else {
		// 5. CLEANUP: If path is at destination but NOT at source, it is an extra file
		// This handles the --delete-destination functionality.
		_ = f.destinationCleaner(destinationObject)
	}

	return nil
}

func (f *syncDestinationComparator) ProcessPendingHardlinks() error {

	for _, destPendingHardlink := range f.destPendingHardlinkObjects.IndexMap {

		sourceObjectInMap, present := f.sourceIndex.IndexMap[destPendingHardlink.RelativePath]
		if !present {
			if f.sourceIndex.IsDestinationCaseInsensitive {
				lcRelativePath := strings.ToLower(destPendingHardlink.RelativePath)
				sourceObjectInMap, present = f.sourceIndex.IndexMap[lcRelativePath]
			}
		}

		if present {

			// Remove from source index so indexer.Traverse won't double-schedule this object
			delete(f.sourceIndex.IndexMap, destPendingHardlink.RelativePath)

			inodeStoreInstance, err := common.GetInodeStore()
			if err != nil {
				return err
			}

			dstAnchorFile, err := inodeStoreInstance.GetAnchor(destPendingHardlink.Inode)
			if err != nil {
				return err
			}

			// If source is a regular file (not in InodeStore), GetAnchor returns ""
			// which won't match the dest anchor, naturally triggering the mismatch path.
			srcAnchorFile, _ := inodeStoreInstance.GetAnchor(sourceObjectInMap.Inode)

			if srcAnchorFile != dstAnchorFile {
				syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, "HardlinkTargetMismatch", false)

				// We must delete the 'wrong' link at destination before creating the 'right' one
				_ = f.destinationCleaner(destPendingHardlink)
				if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
					return err
				}
				continue
			}

			// Anchor matches — check LMT to see if content changed
			if sourceObjectInMap.IsMoreRecentThan(destPendingHardlink, f.preferSMBTime) {
				syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
				if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
					return err
				}
			}
		} else {
			// If the source hardlink is missing, we should delete the destination hardlink as well to avoid leaving stale links behind
			syncComparatorLog(destPendingHardlink.RelativePath, syncStatusOverwritten, "SourceMissingForPendingHardlink", false)
			_ = f.destinationCleaner(destPendingHardlink)
		}
	}
	return nil
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type syncSourceComparator struct {
	// the processor responsible for scheduling copy transfers
	copyTransferScheduler traverser.ObjectProcessor

	// storing the destination objects
	destinationIndex *traverser.ObjectIndexer

	comparisonHashType common.SyncHashType

	preferSMBTime     bool
	disableComparison bool
}

func NewSyncSourceComparator(i *traverser.ObjectIndexer, copyScheduler traverser.ObjectProcessor, comparisonHashType common.SyncHashType, preferSMBTime, disableComparison bool) *syncSourceComparator {
	return &syncSourceComparator{destinationIndex: i, copyTransferScheduler: copyScheduler, preferSMBTime: preferSMBTime, disableComparison: disableComparison, comparisonHashType: comparisonHashType}
}

// it will only transfer source items that are:
//  1. not present in the map
//  2. present but is more recent than the entry in the map
//
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) ProcessIfNecessary(sourceObject traverser.StoredObject) error {
	relPath := sourceObject.RelativePath

	if f.destinationIndex.IsDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}
	destinationObjectInMap, present := f.destinationIndex.IndexMap[relPath]

	if present {
		defer delete(f.destinationIndex.IndexMap, relPath)

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

		// skip if dest is more recent
		syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonTime, false)
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
