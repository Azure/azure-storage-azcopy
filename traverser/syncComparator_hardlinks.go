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

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func countSkippedSymlinkAlias(object StoredObject, increment func()) {
	if object.EntityType == common.EEntityType.Hardlink() && object.hardlinkedSymlink && increment != nil {
		increment()
	}
}

func hardlinkNeedsTransfer(source, destination StoredObject, hash common.SyncHashType, preferSMBTime, force bool) (bool, string) {
	if force {
		return true, syncOverwriteReasonGroupStructureChanged
	}
	if hash != common.ESyncHashType.None() {
		if hash != common.ESyncHashType.MD5() {
			panic("sanity check: unsupported hash type " + hash.String())
		}
		if source.Md5 == nil {
			if source.IsMoreRecentThan(destination, preferSMBTime) {
				return true, syncOverwriteReasonNewerLMTAndMissingHash
			}
			return false, syncSkipReasonTimeAndMissingHash
		}
		if reflect.DeepEqual(source.Md5, destination.Md5) {
			return false, syncSkipReasonSameHash
		}
		return true, syncOverwriteReasonNewerHash
	}
	if source.Size != destination.Size {
		return true, syncOverwriteReasonSizeMismatch
	}
	if source.IsMoreRecentThan(destination, preferSMBTime) {
		return true, syncOverwriteReasonNewerLMT
	}
	return false, syncSkipReasonHardlinkRelationshipIntact
}

const (
	syncEntityTypeMismatch                   = "the source and destination have different entity types (file/folder/symlink/hardlink)"
	syncHardlinkTargetMismatch               = "the source and destination hardlinks point to different targets"
	syncSourceMissingForPendingHardlink      = "the source hardlink is missing, so the destination hardlink is considered stale and will be deleted"
	syncSkipReasonHardlinkRelationshipIntact = "the hardlink relationship is intact; no structural change at destination required"
	syncOverwriteReasonGroupStructureChanged = "the hardlink group structure is changing (merge or split); anchor content must be verified"
	syncOverwriteReasonSizeMismatch          = "the source and destination anchor files differ in size"
)

func buildSrcPathToInode(indexMap map[string]StoredObject) map[string]string {
	m := make(map[string]string, len(indexMap))
	for path, obj := range indexMap {
		if obj.Inode != "" {
			m[path] = obj.Inode
		}
	}
	return m
}

func (f *SyncDestinationComparator) normalizeHardlinkTarget(sourceObj *StoredObject) error {
	if f.inodeStore == nil || sourceObj.Inode == "" ||
		sourceObj.EntityType != common.EEntityType.Hardlink() {
		return nil
	}
	anchor, err := f.inodeStore.GetAnchor(sourceObj.Inode)
	if err != nil {
		return fmt.Errorf("hardlink anchor lookup failed for %s (inode=%s): %w", sourceObj.RelativePath, sourceObj.Inode, err)
	}
	if anchor == "" {
		return nil
	}
	normAnchor := anchor
	normPath := sourceObj.RelativePath
	if f.sourceIndex.IsDestinationCaseInsensitive {
		normAnchor = strings.ToLower(normAnchor)
		normPath = strings.ToLower(normPath)
	}
	_, anchorIsSourceHardlink := f.srcPathToInode[normAnchor]
	if normAnchor == normPath {
		if sourceObj.TargetHardlinkFile == "" {
			return nil
		}
		normTarget := sourceObj.TargetHardlinkFile
		if f.sourceIndex.IsDestinationCaseInsensitive {
			normTarget = strings.ToLower(normTarget)
		}
		if _, targetIsHardlink := f.srcPathToInode[normTarget]; targetIsHardlink {
			sourceObj.TargetHardlinkFile = ""
		}
	} else if anchorIsSourceHardlink {
		sourceObj.TargetHardlinkFile = anchor
	}
	return nil
}

func (f *SyncDestinationComparator) NormalizeAndSchedule(
	scheduler ObjectProcessor,
) ObjectProcessor {
	if f.inodeStore == nil {
		return scheduler
	}
	return func(o StoredObject) error {
		if f.skipSourceSymlinks && (o.EntityType == common.EEntityType.Symlink() || o.hardlinkedSymlink) {
			countSkippedSymlinkAlias(o, f.incrementSkippedSymlink)
			return nil
		}
		if f.srcPathToInode == nil {
			f.srcPathToInode = buildSrcPathToInode(f.sourceIndex.IndexMap)
		}
		if o.EntityType == common.EEntityType.Hardlink() &&
			f.inodeStore != nil && o.Inode != "" {
			if err := f.normalizeHardlinkTarget(&o); err != nil {
				return err
			}
		}
		return scheduler(o)
	}
}

func (f *SyncDestinationComparator) ProcessPendingHardlinks() error {
	if f.inodeStore == nil {
		return nil
	}

	// Build two flat lookup tables to detect structural mismatches between
	// the source and destination inode groups.
	//
	// If one source Inode maps to multiple destination Inodes,
	// we need to merge them.
	// srcInodeIsMultiGroup:   src inode → true when its members span >1 dest inode
	//                         (group merge: two dest groups must be unified at dest)

	// If one destination Inode maps to multiple source Inodes,
	// we need to break them apart.
	// destGroupIsMultiSource: dest inode → true when its members map to >1 src inode
	//                         (group split: one dest group must be broken apart)
	//
	// Both use a "first-seen + overflow" pattern to keep heap usage
	// O(distinct inodes)
	srcInodeFirstDest := make(map[string]string)
	srcInodeIsMultiGroup := make(map[string]bool)
	destInodeFirstSrc := make(map[string]string)
	destGroupIsMultiSource := make(map[string]bool)

	for _, obj := range f.destPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
		srcKey := obj.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			srcKey = strings.ToLower(srcKey)
		}
		srcInode := f.srcPathToInode[srcKey]
		if srcInode == "" {
			// This dest member has no source hardlink counterpart. Either it is
			// absent at source (deleted → cleaned up below) or it became a plain
			// regular file (anchor/member detached). A detaching regular file
			// simply LEAVES the hardlink group; the surviving hardlink members do
			// not need to be recreated, so it is NOT a multi-source split. The
			// detached path is re-uploaded as a File via the srcAnchorFile=="" path.
			continue
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

	// FileJoinsGroup: source group has an independent dest File (A) AND hardlink
	// siblings at dest (B,C,D). The File never appears in destPendingHardlinkObjects,
	// so the loop above cannot set srcInodeIsMultiGroup — fold the flag in here.
	for srcInode := range f.srcInodeHasIndependentDestFile {
		if _, hasHardlinkSiblingAtDest := srcInodeFirstDest[srcInode]; hasHardlinkSiblingAtDest {
			srcInodeIsMultiGroup[srcInode] = true
		}
	}

	// splitSurvivor tracks one file per dest-inode group that has been fully
	// split into regular files at source.  When all present-at-source members of
	// a dest hardlink group become independent (nlink=1) files at source, we only
	// need to unlink (N-1) of them; the remaining "survivor" will naturally have
	// its nlink drop to 1 after the others are deleted (including members missing
	// from source).  This avoids an unnecessary delete+re-upload.
	splitSurvivor := make(map[string]string) // dest inode → survivor relative path

	// Count present-at-source members per dest inode and how many became regular files.
	destInodePresentCount := make(map[string]int)
	destInodeRegularCount := make(map[string]int)
	destInodeTotalCount := make(map[string]int)
	for _, obj := range f.destPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
		destInodeTotalCount[obj.Inode]++
		srcKey := obj.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			srcKey = strings.ToLower(srcKey)
		}
		// Check if the source file exists and is a regular file (no src inode).
		if _, present := f.sourceIndex.IndexMap[srcKey]; present {
			destInodePresentCount[obj.Inode]++
			srcInode := f.srcPathToInode[srcKey]
			if srcInode == "" {
				destInodeRegularCount[obj.Inode]++
				// Pick first encountered as survivor (arbitrary but deterministic per run).
				if _, has := splitSurvivor[obj.Inode]; !has {
					splitSurvivor[obj.Inode] = obj.RelativePath
				}
			}
		}
	}
	// Only keep survivors for groups where ALL present-at-source members became regular files.
	for inode, survivor := range splitSurvivor {
		if destInodeRegularCount[inode] < destInodePresentCount[inode] ||
			(f.deleteDestination != common.EDeleteDestination.True() && destInodePresentCount[inode] < destInodeTotalCount[inode]) {
			delete(splitSurvivor, inode)
			_ = survivor // suppress unused warning
		}
	}

	// destPathToInode maps each pending destination hardlink path to its destination inode.
	// This is used to determine where the srcAnchor is in the same group as a shared member at the destination
	// It lets us ask per member, "At the destination, is the source anchor already in the same hardlink group as
	// this member?"
	// (needsRecreate condition (d) )
	destPathToInode := make(map[string]string, len(f.destPendingHardlinkObjects.IndexMap))
	for _, obj := range f.destPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
		key := obj.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			key = strings.ToLower(key)
		}

		destPathToInode[key] = obj.Inode
	}

	for _, destHardlinkObj := range f.destPendingHardlinkObjects.IndexMap {

		// Normalize the key upfront for case-insensitive destinations so the
		// lookup always matches the lowercase-keyed sourceIndex.
		srcKey := destHardlinkObj.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			srcKey = strings.ToLower(srcKey)
		}
		sourceObjectInMap, present := f.sourceIndex.IndexMap[srcKey]

		if !present {
			// Path no longer exists at source — delete the stale link.
			syncComparatorLog(destHardlinkObj.RelativePath, syncStatusOverwritten, syncSourceMissingForPendingHardlink, false)
			if err := f.destinationCleaner(destHardlinkObj); err != nil {
				return err
			}
			continue
		}

		// Delete using srcKey (the key actually found) so the delete always hits.
		delete(f.sourceIndex.IndexMap, srcKey)

		if f.inodeStore == nil {
			return fmt.Errorf("inode store is not initialized; cannot process pending hardlinks")
		}

		dstAnchorFile, err := f.inodeStore.GetAnchor(destHardlinkObj.Inode)
		if err != nil {
			return err
		}
		var srcAnchorFile string
		if sourceObjectInMap.Inode != "" {
			srcAnchorFile, err = f.inodeStore.GetAnchor(sourceObjectInMap.Inode)
			if err != nil {
				return err
			}
		}

		// Recreate links for splits, merges, or a new data-bearing anchor. A
		// nominal anchor-name change still requires a separate content comparison.

		// Normalize anchor paths for case-insensitive key lookups and comparisons.
		normSrcAnchor := srcAnchorFile
		normDstAnchor := dstAnchorFile
		normRelPath := sourceObjectInMap.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			normSrcAnchor = strings.ToLower(normSrcAnchor)
			normDstAnchor = strings.ToLower(normDstAnchor)
			normRelPath = strings.ToLower(normRelPath)
		}

		// Normalize TargetHardlinkFile to align with the deterministic lex-smallest
		// anchor from InodeStore. During traversal, GetOrAdd assigns the data-carrier
		// role (TargetHardlinkFile = "") to whichever file os.Readdir returns first,
		// which is non-deterministic on Linux filesystems. The sync comparator uses
		// the lex-smallest anchor for content checks, so the anchor must be the data
		// carrier (TargetHardlinkFile = "") and non-anchor files must reference it.
		//
		// Guard: only override when the target is also a hardlink in the source.
		// For hardlinked symlinks, the traverser processes the first-seen member as
		// EntityType=Symlink (srcPathToInode has no entry for it) and assigns
		// subsequent members a TargetHardlinkFile pointing to it.  Overriding that
		// pointer would break the link relationship.
		if srcAnchorFile != "" {
			_, anchorIsSourceHardlink := f.srcPathToInode[normSrcAnchor]
			if normSrcAnchor == normRelPath {
				// This IS the lex-smallest anchor.  Only become the data
				// carrier if the current target (if any) is also a source
				// hardlink.  If it points to a symlink, preserve that link.
				if sourceObjectInMap.TargetHardlinkFile == "" {
					// Already the data carrier — nothing to change.
				} else {
					normTarget := sourceObjectInMap.TargetHardlinkFile
					if f.sourceIndex.IsDestinationCaseInsensitive {
						normTarget = strings.ToLower(normTarget)
					}
					if _, targetIsHardlink := f.srcPathToInode[normTarget]; targetIsHardlink {
						sourceObjectInMap.TargetHardlinkFile = ""
					}
				}
			} else if anchorIsSourceHardlink {
				// Non-anchor: point to the lex-smallest, but only when the
				// anchor is also a source hardlink.  Otherwise keep the
				// traverser's value (which points to a separately-processed
				// symlink).
				sourceObjectInMap.TargetHardlinkFile = srcAnchorFile
			}
		}

		needsRecreate := false
		if normSrcAnchor != normDstAnchor {
			if srcAnchorFile == "" {
				// Source is a regular file (not in InodeStore): entity type changed
				// from hardlink → file.
				// If this file is the survivor for a "pure split" group, we skip the
				// delete — its nlink will drop to 1 after other members are unlinked.
				survivor := splitSurvivor[destHardlinkObj.Inode]
				if survivor != "" && survivor == destHardlinkObj.RelativePath {
					needsRecreate = false // survivor: just check content below
				} else {
					needsRecreate = true
				}
			} else {
				dstAnchorInSrc := f.srcPathToInode[normDstAnchor]
				srcAnchorDstInode := destPathToInode[normSrcAnchor]

				needsRecreate = (dstAnchorInSrc != "" && dstAnchorInSrc != sourceObjectInMap.Inode) || // (a)
					srcInodeIsMultiGroup[sourceObjectInMap.Inode] || // (b)
					destGroupIsMultiSource[destHardlinkObj.Inode] || // (c)
					(srcAnchorDstInode != "" && srcAnchorDstInode != destHardlinkObj.Inode) || // (d1) source anchor lives in a different dest group
					srcAnchorDstInode == "" // A new data anchor requires relinking the existing members.
			}
		}

		if needsRecreate {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncHardlinkTargetMismatch, false)
			if err := f.hardlinkRestructureDeleter(destHardlinkObj); err != nil {
				return err
			}
			if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
				return err
			}
		} else {
			// Relationship is intact — no structural change needed.
			// However, the anchor file's content may still be stale even when the
			// link structure is unchanged.  Check and transfer content if necessary.
			// Non-anchor files carry no content (they link to the anchor), so no
			// content check is required for them.
			//
			// Use the deterministic lex-smallest anchor from InodeStore rather than
			// TargetHardlinkFile, which depends on non-deterministic enumeration order.

			// isSurvivor: this file is the sole kept member of a fully-split group.
			// It needs a content check like an anchor would.
			isSurvivor := srcAnchorFile == "" && splitSurvivor[destHardlinkObj.Inode] == destHardlinkObj.RelativePath

			if normSrcAnchor == normRelPath || isSurvivor {
				// This is the anchor (lex-smallest) file.  Perform generic content verification.
				//
				// groupStructureChanged is true when any file in this inode group is being
				// recreated (group merge: src inode spans multiple dest inodes, or group split:
				// dest inode spans multiple src inodes).  In those cases LMT comparison alone
				// is not a reliable proxy for content equivalence: newly relinked files at the
				// destination will inherit whatever data the anchor holds, so we must make sure
				// the anchor carries the source inode's content regardless of timestamps.
				groupStructureChanged := srcInodeIsMultiGroup[sourceObjectInMap.Inode] ||
					destGroupIsMultiSource[destHardlinkObj.Inode]

				transfer, reason := hardlinkNeedsTransfer(sourceObjectInMap, destHardlinkObj, f.comparisonHashType, f.preferSMBTime, f.disableComparison || groupStructureChanged)
				status := syncStatusSkipped
				if transfer {
					status = syncStatusOverwritten
					if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
						return err
					}
				}
				syncComparatorLog(sourceObjectInMap.RelativePath, status, reason, false)
			} else {
				// Non-anchor: content is owned by the anchor; only the link structure matters.
				syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
			}
		}
	}
	return nil
}

func (f *SyncSourceComparator) ProcessPendingHardlinks() error {
	if f.inodeStore == nil {
		return nil
	}

	// Build two flat lookup tables to detect structural mismatches between
	// source and destination inode groups (merge / split detection).
	//
	// srcInodeIsMultiGroup:   src inode → true when its members span >1 dest inode
	//                         (group merge: multiple dest groups must be unified)
	// destGroupIsMultiSource: dest inode → true when its members map to >1 src inode
	//                         (group split: one dest group must be broken apart)
	srcInodeFirstDest := make(map[string]string)
	srcInodeIsMultiGroup := make(map[string]bool)
	destInodeFirstSrc := make(map[string]string)
	destGroupIsMultiSource := make(map[string]bool)

	for _, obj := range f.srcPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
		lookupPath := obj.RelativePath
		if f.destinationIndex.IsDestinationCaseInsensitive {
			lookupPath = strings.ToLower(lookupPath)
		}
		destInode := f.dstPathToInode[lookupPath]
		if destInode == "" {
			// Path may exist at dest as an independent File (nlink=1 → Inode not
			// recorded in dstPathToInode). Treat that as FileJoinsGroup signal.
			if destObj, ok := f.destinationIndex.IndexMap[lookupPath]; ok &&
				destObj.EntityType != common.EEntityType.Hardlink() {
				if f.srcInodeHasIndependentDestFile == nil {
					f.srcInodeHasIndependentDestFile = make(map[string]bool)
				}
				f.srcInodeHasIndependentDestFile[obj.Inode] = true
			}
			continue // not a hardlink at destination; handled below
		}
		if first, seen := srcInodeFirstDest[obj.Inode]; !seen {
			srcInodeFirstDest[obj.Inode] = destInode
		} else if first != destInode {
			srcInodeIsMultiGroup[obj.Inode] = true
		}
		if first, seen := destInodeFirstSrc[destInode]; !seen {
			destInodeFirstSrc[destInode] = obj.Inode
		} else if first != obj.Inode {
			destGroupIsMultiSource[destInode] = true
		}
	}

	// FileJoinsGroup: independent dest File + hardlink siblings → merge.
	for srcInode := range f.srcInodeHasIndependentDestFile {
		if _, hasHardlinkSiblingAtDest := srcInodeFirstDest[srcInode]; hasHardlinkSiblingAtDest {
			srcInodeIsMultiGroup[srcInode] = true
		}
	}

	// Atomic local downloads replace the anchor inode. Recreate selected aliases
	// after the mixed phase whenever that anchor's content will be refreshed.
	refreshedLocalGroups := make(map[string]bool)
	if f.destinationIsLocal {
		for _, source := range f.srcPendingHardlinkObjects.IndexMap {
			anchor, err := f.inodeStore.GetAnchor(source.Inode)
			if err != nil {
				return err
			}
			path := source.RelativePath
			if f.destinationIndex.IsDestinationCaseInsensitive {
				path, anchor = strings.ToLower(path), strings.ToLower(anchor)
			}
			if path != anchor {
				continue
			}
			destination, present := f.destinationIndex.IndexMap[path]
			if !present {
				continue
			}
			force := f.disableComparison || srcInodeIsMultiGroup[source.Inode] || destGroupIsMultiSource[destination.Inode]
			refreshedLocalGroups[source.Inode], _ = hardlinkNeedsTransfer(source, destination, f.comparisonHashType, f.preferSMBTime, force)
		}
	}

	for _, sourceObject := range f.srcPendingHardlinkObjects.IndexMap {

		// Normalize TargetHardlinkFile early — before any decision path schedules
		// a transfer — so that ALL code paths (entity-type mismatch, needsRecreate,
		// !present, etc.) use the deterministic lex-smallest anchor rather than the
		// non-deterministic first-seen-by-parallel-traversal value.
		//
		// Guard: only override when the inode group is fully represented in the
		// pending set.  For hardlinked symlinks the traverser processes the
		// first-seen member as EntityType=Symlink (outside the pending set) and
		// assigns subsequent members a TargetHardlinkFile pointing to it.
		// Overriding that pointer would break the link relationship.
		if f.inodeStore != nil && sourceObject.Inode != "" {
			anchor, err := f.inodeStore.GetAnchor(sourceObject.Inode)
			if err != nil {
				return err
			}
			if anchor != "" {
				normAnchor := anchor
				normPath := sourceObject.RelativePath
				if f.destinationIndex.IsDestinationCaseInsensitive {
					normAnchor = strings.ToLower(normAnchor)
					normPath = strings.ToLower(normPath)
				}
				_, anchorInPending := f.srcPendingHardlinkObjects.IndexMap[normAnchor]
				if normAnchor == normPath {
					// This IS the lex-smallest anchor.  Only become the data
					// carrier if the current target (if any) is also a deferred
					// hardlink.  If it points to a file already processed
					// outside this set (e.g. a symlink), preserve that link.
					if sourceObject.TargetHardlinkFile == "" {
						// Already the data carrier — nothing to change.
					} else {
						normTarget := sourceObject.TargetHardlinkFile
						if f.destinationIndex.IsDestinationCaseInsensitive {
							normTarget = strings.ToLower(normTarget)
						}
						if _, targetInPending := f.srcPendingHardlinkObjects.IndexMap[normTarget]; targetInPending {
							sourceObject.TargetHardlinkFile = ""
						}
					}
				} else if anchorInPending {
					// Non-anchor: point to the lex-smallest, but only when the
					// anchor is also deferred.  Otherwise keep the traverser's
					// value (which already points to the correct non-deferred
					// file such as a separately-processed symlink).
					sourceObject.TargetHardlinkFile = anchor
				}
			}
		}

		// Normalize the key upfront for case-insensitive destinations so the
		// lookup always matches the lowercase-keyed destinationIndex.
		dstKey := sourceObject.RelativePath
		if f.destinationIndex.IsDestinationCaseInsensitive {
			dstKey = strings.ToLower(dstKey)
		}
		destinationObjectInMap, present := f.destinationIndex.IndexMap[dstKey]

		if !present {
			// Path does not exist at destination — transfer as new.
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// Remove from destination index so indexer.Traverse won't re-process it.
		delete(f.destinationIndex.IndexMap, dstKey)

		// Entity-type mismatch: dest is a plain file/folder/symlink but source is a
		// hardlink. Mirror the same logic syncDestinationComparator.ProcessIfNecessary
		// applies when src is a Hardlink and dest is a File: delete the stale object
		// at the destination and re-download as a hardlink.
		if destinationObjectInMap.EntityType != common.EEntityType.Hardlink() {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncEntityTypeMismatch, false)
			if err := f.hardlinkRestructureDeleter(destinationObjectInMap); err != nil {
				return err
			}
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		if f.inodeStore == nil {
			return fmt.Errorf("inodeStore is nil while processing pending hardlinks")
		}

		var srcAnchorFile string
		if sourceObject.Inode != "" {
			var err error
			srcAnchorFile, err = f.inodeStore.GetAnchor(sourceObject.Inode)
			if err != nil {
				return err
			}
		}
		// When Inode is empty the object is a regular file (not a hardlink in
		// the InodeStore), so we skip the GetAnchor call and dstAnchorFile
		// stays "".  This naturally triggers the entity-type mismatch /
		// srcAnchorFile=="" path below.
		var dstAnchorFile string
		if destinationObjectInMap.Inode != "" {
			var err error
			dstAnchorFile, err = f.inodeStore.GetAnchor(destinationObjectInMap.Inode)
			if err != nil {
				return err
			}
		}

		// groupIntact: the src inode group maps 1:1 onto a single dest inode group.
		groupIntact := !srcInodeIsMultiGroup[sourceObject.Inode] &&
			!destGroupIsMultiSource[destinationObjectInMap.Inode]

		// Normalize anchor paths for case-insensitive key lookups and comparisons.
		normSrcAnchor := srcAnchorFile
		normDstAnchor := dstAnchorFile
		normRelPath := sourceObject.RelativePath
		if f.destinationIndex.IsDestinationCaseInsensitive {
			normSrcAnchor = strings.ToLower(normSrcAnchor)
			normDstAnchor = strings.ToLower(normDstAnchor)
			normRelPath = strings.ToLower(normRelPath)
		}

		// srcAnchorInDst: the dest inode of the source anchor, or "" if the source
		// anchor does not exist at the destination.
		srcAnchorInDst := f.dstPathToInode[normSrcAnchor]
		anchorChanged := normSrcAnchor != normDstAnchor

		// Entity-type change: source became a regular file.  Delete the stale link
		// and re-upload.
		if srcAnchorFile == "" {
			if err := f.hardlinkRestructureDeleter(destinationObjectInMap); err != nil {
				return err
			}
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// A missing source anchor becomes a new inode; existing members must
		// link to it even if they previously formed an intact destination group.
		needsRecreate := anchorChanged &&
			((srcAnchorInDst != "" && srcAnchorInDst != destinationObjectInMap.Inode) ||
				srcAnchorInDst == "" ||
				!groupIntact)
		if normSrcAnchor != normRelPath && refreshedLocalGroups[sourceObject.Inode] {
			needsRecreate = true
		}

		if needsRecreate {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncHardlinkTargetMismatch, false)
			if err := f.hardlinkRestructureDeleter(destinationObjectInMap); err != nil {
				return err
			}
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// Structure is intact.  Non-anchor files carry no independent content;
		// only the anchor needs a content check.
		//
		// Use the InodeStore lex-smallest anchor (srcAnchorFile) rather than the
		// firstSeen-anchor flag (TargetHardlinkFile=="") to identify the anchor.
		// NFS directory listings are NOT guaranteed alphabetical, so the firstSeen
		// anchor may differ from the lex anchor.  When firstSeen≠lex, the firstSeen
		// anchor can hit the needsRecreate path above, while the true lex anchor
		// has TargetHardlinkFile!="" and would be incorrectly skipped.
		if normSrcAnchor != normRelPath {
			syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
			continue
		}

		transfer, reason := hardlinkNeedsTransfer(sourceObject, destinationObjectInMap, f.comparisonHashType, f.preferSMBTime, f.disableComparison || !groupIntact)
		status := syncStatusSkipped
		if transfer {
			status = syncStatusOverwritten
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
		}
		syncComparatorLog(sourceObject.RelativePath, status, reason, false)
	}
	return nil
}
