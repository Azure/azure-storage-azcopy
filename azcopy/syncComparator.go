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
	syncOverwriteReasonGroupStructureChanged  = "the hardlink group structure is changing (merge or split); anchor content must be verified"
	syncOverwriteReasonSizeMismatch           = "the source and destination anchor files differ in size"
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

	// hardlinkRestructureDeleter unconditionally deletes destination objects
	// when hardlink relationships must be restructured (split/merge).
	// Unlike destinationCleaner, this is NOT gated by --delete-destination.
	hardlinkRestructureDeleter traverser.ObjectProcessor

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

	// srcInodeHasIndependentDestFile records source inodes for which we saw a
	// destination File/Folder/Symlink at a path that is a hardlink at source.
	// Those dest objects never enter destPendingHardlinkObjects (nlink=1 → no
	// Inode), so without this flag merge detection cannot see FileJoinsGroup:
	// src {A,B,C,D} one group vs dest {A File + B-C-D hardlinks}.
	srcInodeHasIndependentDestFile map[string]bool

	inodeStore *common.InodeStore
}

func NewSyncDestinationComparator(i *traverser.ObjectIndexer,
	copyScheduler,
	cleaner traverser.ObjectProcessor,
	hardlinkRestructureDeleter traverser.ObjectProcessor,
	comparisonHashType common.SyncHashType,
	preferSMBTime,
	disableComparison bool,
	hardlinkIndexer *traverser.ObjectIndexer,
	inodeStore *common.InodeStore) *syncDestinationComparator {
	return &syncDestinationComparator{sourceIndex: i,
		copyTransferScheduler:      copyScheduler,
		destinationCleaner:         cleaner,
		hardlinkRestructureDeleter: hardlinkRestructureDeleter,
		preferSMBTime:              preferSMBTime,
		disableComparison:          disableComparison,
		comparisonHashType:         comparisonHashType,
		destPendingHardlinkObjects: *hardlinkIndexer,
		inodeStore:                 inodeStore}
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

	if destinationObject.EntityType == common.EEntityType.Hardlink() && f.inodeStore != nil && destinationObject.Inode != "" {

		// we will process hardlinks in a special way because we need to make sure the relationship is not broken.
		// So we will not directly schedule the copy transfer here, instead we will put it in a separate map and
		// process it after we have processed all the other objects. This is to make sure we have the complete picture of
		// what the destination hardlink relationships look like before we decide whether we need to break any of them.
		f.destPendingHardlinkObjects.IndexMap[destinationObject.RelativePath] = destinationObject
		return nil
	}

	// Normalize the key upfront for case-insensitive destinations so the
	// lookup always matches the lowercase-keyed sourceIndex.
	srcKey := destinationObject.RelativePath
	if f.sourceIndex.IsDestinationCaseInsensitive {
		srcKey = strings.ToLower(srcKey)
	}
	sourceObjectInMap, present := f.sourceIndex.IndexMap[srcKey]

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present {
		// Clean up the index map once we start processing this path
		defer delete(f.sourceIndex.IndexMap, srcKey)

		// FORCE OVERWRITE: If comparison is disabled, schedule transfer immediately
		if f.disableComparison {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObjectInMap)
		}

		if sourceObjectInMap.EntityType == common.EEntityType.Hardlink() &&
			destinationObject.EntityType != common.EEntityType.Hardlink() {

			// Normalize TargetHardlinkFile to the deterministic lex-smallest
			// anchor from InodeStore.  Without this, parallel directory walk
			// can leave TargetHardlinkFile pointing at a sibling that is still
			// deferred on the destination, causing CreateHardLink to 404.
			f.normalizeHardlinkTarget(&sourceObjectInMap)

			// Remember that this source inode has an independent dest File so
			// ProcessPendingHardlinks can treat sibling hardlinks as a merge
			// (FileJoinsGroup: recreate B/C/D → A after A is uploaded as carrier).
			if sourceObjectInMap.Inode != "" {
				if f.srcInodeHasIndependentDestFile == nil {
					f.srcInodeHasIndependentDestFile = make(map[string]bool)
				}
				f.srcInodeHasIndependentDestFile[sourceObjectInMap.Inode] = true
			}

			// Case: Destination is a File/Folder/Symlink, but Source is now a Hardlink
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncEntityTypeMismatch, false)

			// Structural hardlink change: delete even when --delete-destination is off.
			_ = f.hardlinkRestructureDeleter(destinationObject)
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

// normalizeHardlinkTarget rewrites sourceObj.TargetHardlinkFile to match the
// deterministic lex-smallest anchor recorded in InodeStore. The traverser's
// GetOrAdd assigns TargetHardlinkFile="" to whichever member os.Readdir
// returns first (non-deterministic on Linux). Without this step, the data
// carrier can end up being a non-anchor file and uploads of the true anchor
// can be routed through the hardlink sender, which then 404s on a target
// that doesn't exist at the destination yet.

func (f *syncDestinationComparator) normalizeHardlinkTarget(sourceObj *traverser.StoredObject) {
	if f.inodeStore == nil || sourceObj.Inode == "" ||
		sourceObj.EntityType != common.EEntityType.Hardlink() {
		return
	}
	anchor, err := f.inodeStore.GetAnchor(sourceObj.Inode)
	if err != nil {
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogWarning,
				fmt.Sprintf("hardlink anchor lookup failed for %s (inode=%s): %v; "+
					"continuing with traverser-assigned TargetHardlinkFile=%q",
					sourceObj.RelativePath, sourceObj.Inode, err, sourceObj.TargetHardlinkFile))
		}
		return
	}
	if anchor == "" {
		return
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
			return
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
}

// NormalizeAndSchedule wraps an object scheduler.
// This is to make sure leftover source objects
// (those with no destination counterpart, scheduled directly
// from indexer.Traverser) get the same TargetHardlinkFile
// normalization that ProcessIfNecessary applies to matched source
// objects.
//
// Without this, a source hardlink whose firstSeen-anchor
// (TargetHardlinkFile=="") and InodeStore-anchor disagree is routed
// into PendingTransfers as a regular data carrier rather than into
// PendingHardlinksTransfers, so CreateHardlink never runs.

/*
Example sync --hardlinks=preserve:
Before run:

	Source: A + B (hardlink group)
	Dest: A only

After run:

	Dest: A + B (CreateHardlink called on B)
*/
func (f *syncDestinationComparator) NormalizeAndSchedule(
	scheduler traverser.ObjectProcessor,
) traverser.ObjectProcessor {
	return func(o traverser.StoredObject) error {
		if f.srcPathToInode == nil {
			f.srcPathToInode = buildSrcPathToInode(f.sourceIndex.IndexMap)
		}
		if o.EntityType == common.EEntityType.Hardlink() &&
			f.inodeStore != nil && o.Inode != "" {
			f.normalizeHardlinkTarget(&o)
		}
		return scheduler(o)
	}
}

func (f *syncDestinationComparator) ProcessPendingHardlinks() error {

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
	for _, obj := range f.destPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
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
		if destInodeRegularCount[inode] < destInodePresentCount[inode] {
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
	// destInodeHasForeignMember: dest inode → true when the group contains a path
	// with no source counterpart (deleted at source). Used by (d2) for overlap merge.
	destInodeHasForeignMember := make(map[string]bool)
	for _, obj := range f.destPendingHardlinkObjects.IndexMap {
		if obj.Inode == "" {
			continue
		}
		key := obj.RelativePath
		if f.sourceIndex.IsDestinationCaseInsensitive {
			key = strings.ToLower(key)
		}

		destPathToInode[key] = obj.Inode
		if _, srcPresent := f.sourceIndex.IndexMap[key]; !srcPresent {
			destInodeHasForeignMember[obj.Inode] = true
		}
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
			_ = f.hardlinkRestructureDeleter(destHardlinkObj)
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

		// Determine whether the hardlink must be recreated.
		//
		// When srcAnchor == dstAnchor the relationship is identical → always skip.
		//
		// When they differ, recreate only if ANY of the following is true:
		//   (a) dstAnchor still exists in source but belongs to a DIFFERENT inode
		//       group → true re-target or group split with a live anchor.
		//   (b) Source group spans multiple dest inodes → group merge, must unify.
		//   (c) Dest group spans multiple source inodes → group split, must break up.
		// 	 (d) The member is linked to the WRONG group at the destination and must
		//		 be re-pointed. Split into two sub-cases:
		//		 (d1) srcAnchor exists at the destination but in a DIFFERENT dest inode
		//		      group → a real retarget.
		//		 (d2) srcAnchor is ABSENT from the destination AND this member's dest
		//		      group contains a foreign member (a dest hardlink with no source
		//		      hardlink counterpart) → overlap merge.
		//		 (d2) covers what (b)/(c) can't see: when source group {A,B} and
		//		 destination group {B,C} overlap on only the single shared file B, the
		//		 source-only member A and the destination-only member C are invisible to
		//		 the multi-inode counters, so without (d2) B would be wrongly left linked
		//		 to C. Requiring a foreign member keeps (d2) from over-firing when a new
		//		 lex-smaller anchor is merely added to an otherwise-intact group
		//		 (srcAnchor absent, no foreign member → skip).
		//
		// Skip (no recreate) when srcAnchor ≠ dstAnchor but none of (a)-(c) apply:
		//   • dstAnchor was deleted from source (srcInode="") AND group is intact
		//     → only the anchor name changed; existing links are still valid.
		//   • dstAnchor is still in the source group (lex-smaller anchor was added)
		//     AND all group members already share a single dest inode.

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
					(srcAnchorDstInode == "" && destInodeHasForeignMember[destHardlinkObj.Inode]) // (d2) overlap merge: srcAnchor absent + foreign dest member
			}
		}

		if needsRecreate {
			syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncHardlinkTargetMismatch, false)
			_ = f.hardlinkRestructureDeleter(destHardlinkObj)
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

				if f.disableComparison || groupStructureChanged {
					reason := syncOverwriteReasonNewerHash
					if groupStructureChanged {
						reason = syncOverwriteReasonGroupStructureChanged
					}
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, reason, false)
					if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
						return err
					}

				} else if f.comparisonHashType != common.ESyncHashType.None() {
					switch f.comparisonHashType {
					case common.ESyncHashType.MD5():
						if sourceObjectInMap.Md5 == nil {
							if sourceObjectInMap.IsMoreRecentThan(destHardlinkObj, f.preferSMBTime) {
								syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
								if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
									return err
								}
							} else {
								syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
							}
						} else if !reflect.DeepEqual(sourceObjectInMap.Md5, destHardlinkObj.Md5) {
							syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
							if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
								return err
							}
						} else {
							syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
						}
					default:
						panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
					}
				} else if sourceObjectInMap.Size != destHardlinkObj.Size {
					// Size mismatch is a reliable, hash-free content signal: if the anchor
					// files are different sizes, content has definitely changed.  Transfer
					// unconditionally rather than relying on LMT, which can be misleading
					// when files are copied, restored, or touched without changing data.
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonSizeMismatch, false)
					if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
						return err
					}
				} else if srcAnchorFile != "" && normSrcAnchor != normDstAnchor {
					// Nominal anchor rename: the dest anchor was deleted/renamed but
					// this group is structurally intact and the member is already
					// linked correctly (needsRecreate=false above).  Skip the LMT-only
					// re-transfer: re-uploading the anchor would create a NEW inode and
					// break the surviving links (the other members keep the old dest
					// inode).
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
				} else if sourceObjectInMap.IsMoreRecentThan(destHardlinkObj, f.preferSMBTime) {
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
					if err := f.copyTransferScheduler(sourceObjectInMap); err != nil {
						return err
					}
				} else {
					syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
				}
			} else {
				// Non-anchor: content is owned by the anchor; only the link structure matters.
				syncComparatorLog(sourceObjectInMap.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
			}
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

	// the processor responsible for deleting extra destination objects
	destinationCleaner traverser.ObjectProcessor

	// hardlinkRestructureDeleter unconditionally deletes destination objects
	// when hardlink relationships must be restructured (split/merge).
	hardlinkRestructureDeleter traverser.ObjectProcessor

	// storing the destination objects
	destinationIndex *traverser.ObjectIndexer

	comparisonHashType common.SyncHashType

	preferSMBTime             bool
	disableComparison         bool
	srcPendingHardlinkObjects traverser.ObjectIndexer

	// dstPathToInode is a snapshot of the destination index built on the first call
	// to ProcessIfNecessary (before any deletions). It maps each destination path →
	// its inode ID and is used in ProcessPendingHardlinks to reason about whether
	// the source anchor is still present in the destination group.
	dstPathToInode map[string]string

	// srcInodeHasIndependentDestFile: see syncDestinationComparator. Used for
	// FileJoinsGroup on the download/S2S path where the dest File is discovered
	// while processing deferred source hardlinks.
	srcInodeHasIndependentDestFile map[string]bool

	inodeStore *common.InodeStore
}

func NewSyncSourceComparator(i *traverser.ObjectIndexer, copyScheduler, cleaner, hardlinkRestructureDeleter traverser.ObjectProcessor, comparisonHashType common.SyncHashType, preferSMBTime, disableComparison bool, inodeStore *common.InodeStore) *syncSourceComparator {
	return &syncSourceComparator{
		destinationIndex:           i,
		copyTransferScheduler:      copyScheduler,
		destinationCleaner:         cleaner,
		hardlinkRestructureDeleter: hardlinkRestructureDeleter,
		preferSMBTime:              preferSMBTime,
		disableComparison:          disableComparison,
		comparisonHashType:         comparisonHashType,
		srcPendingHardlinkObjects:  traverser.ObjectIndexer{IndexMap: make(map[string]traverser.StoredObject)},
		inodeStore:                 inodeStore,
	}
}

// it will only transfer source items that are:
//  1. not present in the map
//  2. present but is more recent than the entry in the map
//
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) ProcessIfNecessary(sourceObject traverser.StoredObject) error {
	// Lazy-init: snapshot the destination inode groups the first time we are
	// called, before any deletions from ProcessIfNecessary remove entries.
	if f.dstPathToInode == nil {
		f.dstPathToInode = buildSrcPathToInode(f.destinationIndex.IndexMap)
	}

	relPath := sourceObject.RelativePath
	if f.destinationIndex.IsDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}
	destinationObjectInMap, present := f.destinationIndex.IndexMap[relPath]

	if sourceObject.EntityType == common.EEntityType.Hardlink() && f.inodeStore != nil && sourceObject.Inode != "" {
		// Defer hardlinks — we need the complete picture of source inode groups
		// before deciding whether any dest links need to be recreated.
		f.srcPendingHardlinkObjects.IndexMap[relPath] = sourceObject
		return nil
	}

	if present {
		defer delete(f.destinationIndex.IndexMap, relPath)

		// if destination is stale, schedule source for transfer
		if f.disableComparison {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
			return f.copyTransferScheduler(sourceObject)
		}

		// Entity-type mismatch: destination is a hardlink but source is a regular
		// file/folder/symlink.  Delete the stale link and re-upload as the new
		// entity type.
		if destinationObjectInMap.EntityType == common.EEntityType.Hardlink() && sourceObject.EntityType != common.EEntityType.Hardlink() {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncEntityTypeMismatch, false)
			_ = f.hardlinkRestructureDeleter(destinationObjectInMap)
			return f.copyTransferScheduler(sourceObject)
		}

		if f.comparisonHashType != common.ESyncHashType.None() && sourceObject.EntityType == common.EEntityType.File() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObject.Md5 == nil {
					if sourceObject.IsMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
						syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						return f.copyTransferScheduler(sourceObject)
					}
					syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
					return nil
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

	// Source path does not exist at destination — schedule transfer.
	return f.copyTransferScheduler(sourceObject)
}

func (f *syncSourceComparator) ProcessPendingHardlinks() error {

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

	// destInodeHasForeignMember: dest inode → true when the group contains a
	// path with no source hardlink counterpart. Used by (d2) for overlap merge.
	destInodeHasForeignMember := make(map[string]bool)
	for path, destInode := range f.dstPathToInode {
		if _, inSrc := f.srcPendingHardlinkObjects.IndexMap[path]; !inSrc {
			destInodeHasForeignMember[destInode] = true
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
			_ = f.hardlinkRestructureDeleter(destinationObjectInMap)
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
			_ = f.hardlinkRestructureDeleter(destinationObjectInMap)
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// needsRecreate: the hardlink target must change at the destination.
		// True only when the anchor change is substantive:
		//   (d1) source anchor lives in a different dest inode group, OR
		//   (d2) source anchor is absent from dest AND this member's dest group
		//        has a foreign member (overlap merge), OR
		//   (b)/(c) the group is merging or splitting (!groupIntact).
		needsRecreate := anchorChanged &&
			((srcAnchorInDst != "" && srcAnchorInDst != destinationObjectInMap.Inode) ||
				(srcAnchorInDst == "" && destInodeHasForeignMember[destinationObjectInMap.Inode]) ||
				!groupIntact)

		if needsRecreate {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncHardlinkTargetMismatch, false)
			_ = f.hardlinkRestructureDeleter(destinationObjectInMap)
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

		// Anchor content check.
		//
		// When the group is restructuring (!groupIntact), force-transfer the anchor
		// so all relinked files at the destination carry the correct data.
		if !groupIntact || f.disableComparison {
			reason := syncOverwriteReasonGroupStructureChanged
			if f.disableComparison {
				reason = syncOverwriteReasonNewerHash
			}
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, reason, false)
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// Hash comparison (when available).
		if f.comparisonHashType != common.ESyncHashType.None() {
			switch f.comparisonHashType {
			case common.ESyncHashType.MD5():
				if sourceObject.Md5 == nil {
					if sourceObject.IsMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
						syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMTAndMissingHash, false)
						if err := f.copyTransferScheduler(sourceObject); err != nil {
							return err
						}
					} else {
						syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonTimeAndMissingHash, false)
					}
				} else if !reflect.DeepEqual(sourceObject.Md5, destinationObjectInMap.Md5) {
					syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerHash, false)
					if err := f.copyTransferScheduler(sourceObject); err != nil {
						return err
					}
				} else {
					syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonSameHash, false)
				}
			default:
				panic("sanity check: unsupported hash type " + f.comparisonHashType.String())
			}
			continue
		}

		// Size mismatch: reliable hash-free content signal.
		if sourceObject.Size != destinationObjectInMap.Size {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonSizeMismatch, false)
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// LMT check — skipped for nominal anchor renames to avoid spurious transfers
		// caused by FILETIME precision loss (dest anchor NFS write-time was set from
		// the OLD anchor's mtime, not the new anchor's).
		anchorNominallyChanged := anchorChanged // needsRecreate=false is implied here
		if !anchorNominallyChanged && sourceObject.IsMoreRecentThan(destinationObjectInMap, f.preferSMBTime) {
			syncComparatorLog(sourceObject.RelativePath, syncStatusOverwritten, syncOverwriteReasonNewerLMT, false)
			if err := f.copyTransferScheduler(sourceObject); err != nil {
				return err
			}
			continue
		}

		// Content matches (or nominal anchor rename with matching size/hash): skip.
		syncComparatorLog(sourceObject.RelativePath, syncStatusSkipped, syncSkipReasonHardlinkRelationshipIntact, false)
	}
	return nil
}
