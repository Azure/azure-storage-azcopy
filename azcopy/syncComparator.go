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
	syncEntityTypeMismatch                    = "the source and destination have different entity types (file/folder/symlink/hardlink)"
	syncHardlinkTargetMismatch                = "the source and destination hardlinks point to different targets"
	syncSourceMissingForPendingHardlink       = "the source hardlink is missing, so the destination hardlink is considered stale and will be deleted"
	syncSkipReasonHardlinkRelationshipIntact  = "the hardlink relationship is intact; no structural change at destination required"
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

	// srcPathToInode is a snapshot of the source index built on the first call to
	// ProcessIfNecessary (before any deletions). It maps each source path → its inode ID.
	// Used in ProcessPendingHardlinks to check whether a dest anchor path is still a
	// member of the source inode group, without needing the full nested group map.
	srcPathToInode map[string]string
}

func NewSyncDestinationComparator(i *traverser.ObjectIndexer,
	copyScheduler,
	cleaner traverser.ObjectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime,
	disableComparison bool,
	hardlinkIndexer *traverser.ObjectIndexer) *syncDestinationComparator {
	return &syncDestinationComparator{sourceIndex: i,
		copyTransferScheduler:      copyScheduler,
		destinationCleaner:         cleaner,
		preferSMBTime:              preferSMBTime,
		disableComparison:          disableComparison,
		comparisonHashType:         comparisonHashType,
		destPendingHardlinkObjects: *hardlinkIndexer}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) ProcessIfNecessary(destinationObject traverser.StoredObject) error {

	// Lazy-init: snapshot the source inode groups the first time we are called.
	// At this point source traversal is complete and the sourceIndex is fully
	// populated; no deletions from ProcessIfNecessary have happened yet.
	if f.srcPathToInode == nil {
		f.srcPathToInode = buildSrcPathToInode(f.sourceIndex.IndexMap)
	}

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

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		// Clean up the index map once we start processing this path
		defer delete(f.sourceIndex.IndexMap, destinationObject.RelativePath)

		// FORCE OVERWRITE: If comparison is disabled, schedule transfer immediately
		if f.disableComparison {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if sourceObjectInMap.EntityType == common.EEntityType.Hardlink() {
			// Case: Destination is a File/Folder/Symlink, but Source is now a Hardlink
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncEntityTypeMismatch, false)

			// Delete the independent file/folder/symlink to allow link creation
			_ = f.destinationCleaner(destinationObject)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		// CONTENT VALIDATION: Hash Comparison
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

			// CONTENT VALIDATION: Last Modified Time (LMT)
			// Default sync behavior: check if source is newer than destination
		} else if sourceObjectInMap.IsMoreRecentThan(destinationObject, f.preferSMBTime) {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			return f.copyTransferScheduler(sourceObjectInMap)
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

// buildSrcPathToInode builds a flat map of source path → inode ID.
// It is called once from ProcessIfNecessary (see lazy-init above) before any
// deletions from sourceIndex occur, so every source path is captured.
// A flat map is sufficient because ProcessPendingHardlinks only needs to answer
// "does this dest-anchor path exist in the source with inode X?" — no nested
// group structure is required, and no inner map allocations are needed.
func buildSrcPathToInode(indexMap map[string]traverser.StoredObject) map[string]string {
	m := make(map[string]string, len(indexMap))
	for path, obj := range indexMap {
		if obj.Inode != "" {
			m[path] = obj.Inode
		}
	}
	return m
}

func (f *syncDestinationComparator) ProcessPendingHardlinks() error {

	// Build two flat lookup tables to detect structural mismatches between
	// the source and destination inode groups.
	//
	// srcInodeIsMultiGroup:   src inode → true when its members span >1 dest inode
	//                         (group merge: two dest groups must be unified at dest)
	// destGroupIsMultiSource: dest inode → true when its members map to >1 src inode
	//                         (group split: one dest group must be broken apart)
	//
	// Both use a "first-seen + overflow" pattern to avoid nested map allocations,
	// keeping heap usage O(distinct inodes) rather than O(distinct inode pairs).
	srcInodeFirstDest := make(map[string]string)
	srcInodeIsMultiGroup := make(map[string]bool)
	destInodeFirstSrc := make(map[string]string)
	destGroupIsMultiSource := make(map[string]bool)

	for _, obj := range f.destPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
		srcInode := f.srcPathToInode[obj.RelativePath]
		if srcInode == "" {
			continue // not present in source; will be deleted below
		}
		if first, seen := srcInodeFirstDest[srcInode]; !seen {
			srcInodeFirstDest[srcInode] = obj.Inode
		} else if first != obj.Inode {
			srcInodeIsMultiGroup[srcInode] = true
		}
		if first, seen := destInodeFirstSrc[obj.Inode]; !seen {
			destInodeFirstSrc[obj.Inode] = srcInode
		} else if first != srcInode {
			destGroupIsMultiSource[obj.Inode] = true
		}
	}

	for _, destHardlinkObj := range f.destPendingHardlinkObjects.IndexMap {

		// Track the actual key used so we delete the correct index entry,
		// even on case-insensitive file systems where the stored key may be
		// lowercase while destHardlinkObj.RelativePath is mixed-case.
		srcKey := destHardlinkObj.RelativePath
		sourceObjectInMap, present := f.sourceIndex.IndexMap[srcKey]
		if !present && f.sourceIndex.IsDestinationCaseInsensitive {
			srcKey = strings.ToLower(destHardlinkObj.RelativePath)
			sourceObjectInMap, present = f.sourceIndex.IndexMap[srcKey]
		}

		if !present {
			// Path no longer exists at source — delete the stale link.
			syncComparatorLog(destHardlinkObj.RelativePath, syncStatusOverwritten, syncSourceMissingForPendingHardlink, false)
			_ = f.destinationCleaner(destHardlinkObj)
			continue
		}

		// Delete using srcKey (the key actually found) so the delete always hits.
		delete(f.sourceIndex.IndexMap, srcKey)

		inodeStoreInstance, err := common.GetInodeStore()
		if err != nil {
			return err
		}

		dstAnchorFile, err := inodeStoreInstance.GetAnchor(destHardlinkObj.Inode)
		if err != nil {
			return err
		}
		srcAnchorFile, _ := inodeStoreInstance.GetAnchor(sourceObjectInMap.Inode)

		// Determine whether the hardlink must be recreated.
		//
		// When srcAnchor == dstAnchor the relationship is identical → always skip.
		//
		// When they differ, recreate only if ANY of the following is true:
		//   (a) dstAnchor still exists in source but belongs to a DIFFERENT inode
		//       group → true re-target or group split with a live anchor.
		//   (b) Source group spans multiple dest inodes → group merge, must unify.
		//   (c) Dest group spans multiple source inodes → group split, must break up.
		//
		// Skip (no recreate) when srcAnchor ≠ dstAnchor but none of (a)-(c) apply:
		//   • dstAnchor was deleted from source (srcInode="") AND group is intact
		//     → only the anchor name changed; existing links are still valid.
		//   • dstAnchor is still in the source group (lex-smaller anchor was added)
		//     AND all group members already share a single dest inode.
		needsRecreate := false
		if srcAnchorFile != dstAnchorFile {
			if srcAnchorFile == "" {
				// Source is a regular file (not in InodeStore): entity type changed
				// from hardlink → file. Delete the dest link and re-upload as a file.
				needsRecreate = true
			} else {
				dstAnchorInSrc := f.srcPathToInode[dstAnchorFile]
				needsRecreate = (dstAnchorInSrc != "" && dstAnchorInSrc != sourceObjectInMap.Inode) || // (a)
					srcInodeIsMultiGroup[sourceObjectInMap.Inode] || // (b)
					destGroupIsMultiSource[destHardlinkObj.Inode] // (c)
			}
		}

		if needsRecreate {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncHardlinkTargetMismatch, false)
			_ = f.destinationCleaner(destHardlinkObj)
			if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
				return err
			}
		} else {
			// Relationship is intact — no transfer needed.
			// Content is owned by the anchor's own transfer; CreateHardlink carries no data.
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
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
