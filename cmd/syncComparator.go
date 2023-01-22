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
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/shubham808/azure-storage-azcopy/v10/common"
)

// with the help of an objectIndexer containing the source objects
// find out the destination objects that should be transferred
// in other words, this should be used when destination is being enumerated secondly
type syncDestinationComparator struct {
	// the rejected objects would be passed to the destinationCleaner
	destinationCleaner objectProcessor

	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	//
	// This stores the source files and their attributes as scanned by the source traverser. Source traverser populates it using folderIndexer.store()
	// while target traverser consumes it using syncDestinationComparator.processIfNecessary(). Note that source traverser refers to this using
	// syncEnumerator.objectIndexer so this and syncEnumerator.objectIndexer should refer to the same folderIndexer.
	//
	sourceFolderIndex *folderIndexer

	possiblyRenamedMap *possiblyRenamedMap

	disableComparison bool

	// Change file detection mode.
	// See cookedSyncCmdArgs.cfdMode for details.
	cfdMode common.CFDMode

	// Time of last Sync.
	// see cookedSyncCmdArgs.lastSyncTime for details.
	lastSyncTime time.Time

	// see cookedSyncCmdArgs.metaDataOnlySync for details.
	metaDataOnlySync bool

	//
	// This is the number of seconds that the Target’s clock is ahead of the Source’s clock. This will be subtracted from Target’s ctime value before comparing
	// with the Source’s ctime value when checking for “newness”. This will be set *only* for targets which do not allow setting ctime and instead set ctime to
	// the “now” value when a file operation is executed. For targets where we set the ctime value we can safely compare for equality with the source ctime value
	// since we would have set it to source ctime value when the target object was sync’ed, for such targets TargetCtimeSkew is set to 0.
	//
	// Note: If we choose a larger skew value, we might wrongly consider some object as needing sync, but that’s
	//       harmless as opposes to choosing a smaller skew and missing out syncing some object that has changed.
	//
	TargetCtimeSkew uint

	scannerLogger common.ILoggerResetable

	// Function to increment files/folders not transferred as a result of no change since last sync.
	incrementNotTransferred func(common.EntityType)
}

func newSyncDestinationComparator(i *folderIndexer, possiblyRenamedMap *possiblyRenamedMap, copyScheduler, cleaner objectProcessor, disableComparison bool, cfdMode common.CFDMode, lastSyncTime time.Time, scannerLogger common.ILoggerResetable, incrementNotTransferred func(common.EntityType), metaDataOnlySync bool) *syncDestinationComparator {
	return &syncDestinationComparator{sourceFolderIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, disableComparison: disableComparison,
		cfdMode:                 cfdMode,
		possiblyRenamedMap:      possiblyRenamedMap,
		lastSyncTime:            lastSyncTime,
		scannerLogger:           scannerLogger,
		incrementNotTransferred: incrementNotTransferred,
		metaDataOnlySync:   metaDataOnlySync,
	}
}

//
// Given a file and the corresponding scanned source object, find out if we need to copy data+metadata, only metadata, or nothing.
// This is called by TargetTraverser. It honours various sync qualifiers to make the decision, f.e., if sync
// qualifiers allow ctime/mtime to be used for CFD it may not need to query file attributes from target.
//
// Note: Caller will use the returned information to decide whether to copy the storedObject to target and whether to copy only metadata,
//       or both metadata+data.
//
// Note: This SHOULD NOT be called for children of "changed" directories, since for changed directories we cannot safely check for
//       changed files purely by doing local-time based comparison. Use HasFileChangedSinceLastSyncUsingTargetCompare() for children
//       of changed directories. This means it will NEVER BE CALLED for cfdMode==TargetCompare, since for that HasDirectoryChangedSinceLastSync()
//       always returns true, i.e., all directories are treated as “changed”.
//
// Return: (dataChanged, metadataChanged)
//
// Note: Since data change usually causes metadata change too (LMT is updated at the least), caller should check dataChanged first and if that is true, sync
//       both data+metadata, if dataChanged is not true then it should check metadataChanged and if that is true, sync only metadata, else sync nothing.
//
func (f *syncDestinationComparator) HasFileChangedSinceLastSyncUsingLocalChecks(so StoredObject) (dataChange bool, metadataChange bool) {
	// CFDMode==TargetCompare always treats target directories as “changed”, so we should never be called for that
	if f.cfdMode == common.CFDModeFlags.TargetCompare() {
		panic("HasFileChangedSinceLastSyncUsingLocalChecks() should not be called for CFDMode==TargetCompare")
	}

	// Changed file detection using Ctime and Mtime.
	if f.cfdMode == common.CFDModeFlags.CtimeMtime() {
		// File Mtime changed, which means data changed and it cause metadata change.
		if so.lastModifiedTime.After(f.lastSyncTime) {
			return true, true
		} else if so.lastChangeTime.After(f.lastSyncTime) {
			// File Ctime changed only, only meta data changed.
			//
			// Else if ctime > LastSyncTime, then only metadata needs to be sync'ed.
			// If MetadataOnlySync is True then we distinguish between data and metadata sync, else we always do data+metadata sync.
			//
			if f.metaDataOnlySync {
				return false, true
			}
			return true, true
		}
		// File not changed at all.
		return false, false
	} else if f.cfdMode == common.CFDModeFlags.Ctime() {
		// Changed file detection using Ctime only.

		// File changed since lastSync time. CFDMode is Ctime, so we can't rely on mtime as it can be modified by any other tool.
		if so.lastChangeTime.After(f.lastSyncTime) {
			// If MetaDataSync Flag is false we don't need to check for data or metadata change. We can return true in that case.
			if !f.metaDataOnlySync {
				return true, true
			} else {
				panic("We should not reach here, for CFDMODE==Ctime and metaDataOnlySync==true. It should be taken care with FinalizeAll flag.")
			}
		} else {
			// File Ctime not changed, means no data or metadata changed.
			return false, false
		}
	} else {
		// This is the case when neither CtimeMtime or Ctime cfdMode set.
		panic(fmt.Sprintf("HasFileChangedSinceLastSyncUsingLocalChecks() called for CFDMode==%s", f.cfdMode.String()))
	}
}

//
// This is called for two distinct scenarios:
// For target directories that are enumerated (because the source directory was seen to have changed), it's called after all the enumerated children
// are processed. In this case the files still present in ObjectIndexer map are the ones newly created in the source since last sync and *all* of them
// need to be copied to target. This condition is conveyed by passing the 2nd argument (FinalizeAll) as true.
// For target directories that are *not* enumerated (for which HasDirectoryChangedSinceLastSync() returned False), FinalizeTargetDirectory() is called
// without processing any children. In this case the files present in ObjectIndexer map are *all* files present in the source, not just newly created ones.
// So we have to copy only files which have changed since the last sync, also looking for whether data+metadata or only metadata has changed, but it's
// worth calling out that since the directory is not changed, we can safely test files for changes by locally comparing ctime/mtime with LastSyncTime,
// if the CFDMode allows that.
// This condition is conveyed by passing the 2nd argument (FinalizeAll) as false.
//
// This function will also update the folder metaData, if changed which is in case of finalizeAll true and delete the flderMap from the ObjectIndexerMap.
func (f *syncDestinationComparator) FinalizeTargetDirectory(relativeDir string, finalizeAll bool) {
	var size int64

	lcFolderName := relativeDir

	// Note: For the root directory,relativeDir will be "" but the SourceTraverser would have stored it's properties in folderMap["."], so use "." for root directory.
	if lcFolderName == "" {
		lcFolderName = "."
	}

	f.sourceFolderIndex.lock.Lock()

	folderMap, folderPresent := f.sourceFolderIndex.folderMap[lcFolderName]

	if !folderPresent {
		panic(fmt.Sprintf("Folder with relativePath[%s] not present in ObjectIndexerMap", relativeDir))
	}

	//
	// We remove the objectIndexer corresponding to this folder from f.sourceFolderIndex.folderMap before starting
	// to process it to avoid holding the lock across the expensive calls that we make later.
	// Anyway FinalizeTargetDirectory() is the last function to be called for a directory and it should remove the
	// objectIndexer map for that folder from the sourceFolderIndex.
	//
	delete(f.sourceFolderIndex.folderMap, lcFolderName)

	f.sourceFolderIndex.lock.Unlock()
	f.scannerLogger.Log(pipeline.LogInfo, fmt.Sprintf("Finalizing directory %s (FinalizeAll=%v)\n", relativeDir, finalizeAll))

	//
	// Go over all objects in the source directory, enumerated by SourceTraverser, and check each object for following:
	//
	// 1. Does the entire object need to be synced - data+metadata?
	// 2. Does only metadata need to be sync'ed?
	// 3. Object is not changed since last sync, no need to update.
	//
	//
	// Note: Finalizer is called for *every* directory present in the source and it's guaranteed to be called after all children of the directory are processed.
	//       To be precise, when Finalizer is called for a directory we will need to process all remaining children of that directory before we return from
	//       the Finalizer. Put another way, once Finalizer for a directory returns we shouldn't be looking at any children of the directory and hence, f.e.,
	//       for directory "dir1/dir2"" all folderMap entries with folderMap["dir1/dir2"] will be deleted before we return from the Finalizer.
	// For *every* folder we update its properties in its finalizer. f.e., for following directory structure present on source
	//
	// dir1/dir2
	// dir1/file1
	// dir2/file2
	//
	// the object indexer map entries will be following :-
	//
	// ["dir1"]["."]
	// ["dir1"]["dir2"]
	// ["dir1"]["file1"]
	// ["dir2"]["."]
	// ["dir2"]["file2"]
	// ["dir1/dir2"]["."]
	//
	// In dir1's Finalizer we do the following:
	//
	// 1. Create target file dir1/file1.
	// 2. Create target directory dir1/dir2.
	//    Note: As an optimization, or for some targets, we can skip this as component directories will be auto-created when a resident file is created,
	//          but note that this would mean that we won't create empty directories in the target.
	// 3. Update properties for target directory dir1.
	//
	// Note that we can safely update the dir1 properties as this is after the last chidren of dir1 has been created.
	//

	// For FinalizeAll flag true, we know target traverser done. Now the files left in ObjectIndexerMap are the new ones.
	// So, we don't do any ctime and mtime comparsion.
	for file := range folderMap.indexMap {
		//
		// ["dir1"]["."] is a special StoredObject for holding the properties for directory "dir1".
		// This need not be created, so skip it now. In the end, once we are done creating all the
		// files/dirs inside "dir1" we will update dir1's properties by querying from ["dir1"]["."].
		//
		if file == "." {
			continue
		}

		storedObject := folderMap.indexMap[file]
		size += storedObjectSize(storedObject)
		delete(folderMap.indexMap, file)

		// If finalizeAll==true we need to blindly copy *all* files/folders present in folderMap.indexMap.
		dataChange, metaDataChange := true, true

		// else, we need to find out if the file/folder has changed since last sync.
		if finalizeAll == false {
			dataChange, metaDataChange = f.HasFileChangedSinceLastSyncUsingLocalChecks(storedObject)
		}

		// TODO: Remove this code.
		//       Till we add support for updating directory attributes, we prevent this directory
		//       creation, else this clashes with the directory creation at the end of FinalizeTargetDirectory().
		//       Once we add support for directory attributes updation, change that call to cause
		//       directory attributes updation and not directory creation and remove this code.
		//
		if storedObject.entityType == common.EEntityType.Folder() {
			dataChange = false
		}

		//
		// finalizeAll==true implies that storedObject is newly created in the source.
		// This directory could have been possibly renamed in the source after the last sync.
		// It could very well be a new directory created on source after the last sync, but that doesn't
		// cause any additional overhead as new directories would need to be enumerated anyways.
		//
		if f.possiblyRenamedMap != nil && finalizeAll == true && storedObject.entityType == common.EEntityType.Folder() {
			f.scannerLogger.Log(pipeline.LogInfo, fmt.Sprintf("Directory(%s) not present on target, possibly it's renamed", storedObject.relativePath))

			// Add to "possibly renamed" map.
			f.possiblyRenamedMap.store(storedObject)
		}

		if dataChange {
			// For folders, this should create a new empty folder if not present.
			f.copyTransferScheduler(storedObject)
		} else if metaDataChange {
			// For folders, properties are sync'ed when the Finalizer for that folder is called.
			if storedObject.entityType != common.EEntityType.Folder() {
				f.scannerLogger.Log(pipeline.LogInfo, fmt.Sprintf("File(%s) scheduled for property transfer only", storedObject.relativePath))
				storedObject.entityType = common.EEntityType.FileProperties()

				// This is only file properties transfer, we don't want it to be accounted in bytes transferred.
				storedObject.size = 0
				f.copyTransferScheduler(storedObject)
			}
		} else {
			//
			// Neither data nor metadata for the file has changed, hence file is not transferred.
			//
			// Note :- For folders, properties are sync'ed when the Finalizer for that folder is called.
			//
			if f.incrementNotTransferred != nil && storedObject.entityType != common.EEntityType.Folder() {
				f.incrementNotTransferred(storedObject.entityType)
			}
		}
	}

	// last thing need to do the folder metaData updation incase of the finalizeAll true.
	// TODO: We need to take care cfdMode == TargetCompare, as for it finalizer will be true. It cause each folder properties updation.
	so, ok := folderMap.indexMap["."]
	if !ok {
		panic(fmt.Sprintf("Folder stored map not present"))
	}

	size += storedObjectSize(so)
	delete(folderMap.indexMap, ".")

	if finalizeAll {
		//
		// Note: We actually want to update the directory properties and *not* create a new directory.
		//
		// TODO: For CtimeMtime we will be called with finalizeAll=true, only when the directory on source has changed since last sync time,
		//       while for TargetCompare we will always be called with finalizeAll=true, even if the source directory has not changed.
		//       So, in case of TargetCompare we must copy directory properties only if it has changed. Also incrementNotTransferred() must be called
		//       accordingly.
		//

		// Add this transfer to job order.
		f.copyTransferScheduler(so)
	} else {
		//
		// finalizeAll will be false (and we will come here) for only CtimeMtime when the directory on the source has changed since the last sync.
		// It will be true (and we will not come here) for the following cases :-
		// 1. cfdMode = CtimeMtime , directory not changed on the source since last sync time.
		// 2. cfdMode = TargetCompare.
		// This means whenever we come here, we don't tranfers the folder property and hence we should call incrementNotTransferred().
		//
		if f.incrementNotTransferred != nil {
			f.incrementNotTransferred(so.entityType)
		}
	}

	// lets remove the folderMap, it should be empty.
	if len(folderMap.indexMap) != 0 {
		panic("Length of folderMap should be zero")
	}

	size += int64(unsafe.Sizeof(objectIndexer{}))

	atomic.AddInt64(&f.sourceFolderIndex.totalSize, -size)
	if atomic.LoadInt64(&f.sourceFolderIndex.totalSize) < 0 {
		panic("Total Size is negative.")
	}
}

// it will only schedule transfers for destination objects that are present in the indexer but stale compared to the entry in the map
// if the destinationObject is not at the source, it will be passed to the destinationCleaner
// ex: we already know what the source contains, now we are looking at objects at the destination
// if file x from the destination exists at the source, then we'd only transfer it if it is considered stale compared to its counterpart at the source
// if file x does not exist at the source, then it is considered extra, and will be deleted
func (f *syncDestinationComparator) processIfNecessary(destinationObject StoredObject) error {
	var lcFolderName, lcFileName, lcRelativePath string
	var present bool
	var sourceObjectInMap StoredObject

	//
	// We come here with 'destinationObject' when Target traverser (parallelList() for Target=Blob) enumerates a directory from tqueue and
	// creates and enqueues a destinationObject for each enumerated children. Target traverser also creates and enqueues a special
	// destinationObject (with isFolderEndMarker==true) after queueing destinationObjects for all direct children of the directory.
	// This special destinationObject is guaranteed to be queued *after* all the other children objects for the directory (if any).
	// See below for more details on the special isFolderEndMarker object.
	//
	if f.sourceFolderIndex.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(destinationObject.relativePath)
	} else {
		lcRelativePath = destinationObject.relativePath
	}

	//
	// parent dir name, will be "." for files/dirs at the copy root.
	//
	// So, for lcRelativePath == "dir1/dir2",
	// lcFolderName -> "dir1"
	// lcFileName   -> "dir2"
	//
	// and for "dir1" (dir1 present at copy root),
	// lcFolderName -> "."
	// lcFileName   -> "dir1"
	//
	// Q: When will folderPresent be false below?
	// A: Since we process every directory independently, if "dir1" is fully processed before "dir1/dir2",
	//    when processIfNecessary() is called for "dir1/dir2" with isFolderEndMarker==true, foldermap["dir1"]
	//    won't be found as it would have been deleted after "dir1" was fully processed, though we MUST have
	//    foldermap["dir1/dir2"].
	//    Also note that folderPresent MUST never be false when isFolderEndMarker==false, since target traverser
	//    only ever enumerates directories added to tqueue by the source traverser and all such directories
	//    MUST be added to folderMap by the source traverser.
	//
	lcFolderName = filepath.Dir(lcRelativePath)
	lcFileName = filepath.Base(lcRelativePath)

	if destinationObject.isFolderEndMarker && destinationObject.entityType == common.EEntityType.Folder() {
		//
		// We will see this special "end of folder" marker *after* we have seen all direct children of
		// that directory (if any). We have the following cases:
		// 1. destinationObject.relativePath directory does not exist in the target. In this case there won't be any children
		//    of destinationObject.relativePath queued for us and we need to copy the directory recursively to the target.
		// 2. destinationObject.relativePath directory exists in the target and the source directory has not "changed" since last
		//    sync. In this case the Target traverser would have skipped enumeration of destinationObject.relativePath and hence
		//    there won't be any children queued for us. We need to go over all its children added in sourceFolderIndex and test
		//    them for "newness" and copy them if they are found modified after lastsync.
		// 2. destinationObject.relativePath directory exists in the target and the source directory has "changed" since last sync.
		//    In this case Target traverser would have enumerated the directory and queued all enumerated children for us. After all
		//    the children it would have queued this special isFolderEndMarker object. So when we process the isFolderEndMarker object,
		//    we would have processed all children and hence whatever is left in sourceFolderIndex for that directory are only the files
		//    newly created in the source since last sync. We need to copy all of them to the target.
		//
		f.FinalizeTargetDirectory(lcRelativePath, destinationObject.isFinalizeAll)

		return nil
	}

	f.sourceFolderIndex.lock.Lock()

	foldermap, folderPresent := f.sourceFolderIndex.folderMap[lcFolderName]

	// sanity check folder always present till finalizer not called. If its not present then there might be some issue.
	if !folderPresent {
		panic(fmt.Sprintf("Folder with relativePath[%s] not present in ObjectIndexerMap", lcRelativePath))
	}

	// Folder Case.
	if destinationObject.entityType == common.EEntityType.Folder() {
		sourceObjectInMap, present = foldermap.indexMap[lcFileName]

		//
		// Target also has this object and is of the same type (directory) as the source? If yes, do nothing right now. When we process this directory from tqueue and finalize it,
		// that time we will sync the directory properties if needed. If either target does not have object with the same name or object type is different, we will need to delete it.
		// This will be done by the f.destinationCleaner() call below. The new directory will be created in FinalizeTargetDirectory() when we go over the folderIndexer map entries.
		// Each folder folderIndexer map has ["."] entry which represent that folder. We will delete this entry from map as it belong to its parent map.
		//
		// Note: This entry is required to detect the case where folder not present on source, but present on target.
		//
		if present && (destinationObject.entityType == sourceObjectInMap.entityType) {
			// Check if entries match or not.
			if sourceObjectInMap.relativePath != destinationObject.relativePath {
				panic(fmt.Sprintf("Relative Path at source[%s] not matched with destination[%s]", sourceObjectInMap.relativePath, destinationObject.relativePath))
			}

			//
			// For every enumerated target directory, we must compare the inode to check if the source directory was renamed since
			// the last sync. If a directory is renamed, all its children and grandchildren need to be enumerated at the target, for
			// safely copying (grand)children directories.
			//

			// Inode should be set for both source and destination.
			if destinationObject.inode == 0 || sourceObjectInMap.inode == 0 {
				panic(fmt.Sprintf("Either destinationObject inode(%v) or sourceObjectInMap inode(%v) is not set for relativePath(%s)\n",
					destinationObject.inode, sourceObjectInMap.inode, destinationObject.relativePath))
			}

			if destinationObject.inode != sourceObjectInMap.inode && f.possiblyRenamedMap != nil {
				f.scannerLogger.Log(pipeline.LogInfo, fmt.Sprintf("DestinationDir(%s, %v) inode not match with sourceDir(%s, %v), possibly it's rename",
					destinationObject.relativePath, destinationObject.inode, sourceObjectInMap.relativePath, sourceObjectInMap.inode))

				f.possiblyRenamedMap.store(sourceObjectInMap)
			}

			delete(foldermap.indexMap, lcFileName)
			atomic.AddInt64(&f.sourceFolderIndex.totalSize, -storedObjectSize(sourceObjectInMap))

			f.sourceFolderIndex.lock.Unlock()
			return nil
		}
		// We detect folder not present on source, now we need to delete the folder and files underneath.
		// Other case will be folder at source changed to file, so we need to delete the folder in target.
		f.sourceFolderIndex.lock.Unlock()

		_ = f.destinationCleaner(destinationObject)

		return nil
	}

	// File case.
	sourceObjectInMap, present = foldermap.indexMap[lcFileName]

	// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
	if present && (sourceObjectInMap.entityType == destinationObject.entityType) {
		// Sanity check.
		if sourceObjectInMap.relativePath != destinationObject.relativePath {
			panic(fmt.Sprintf("Relative Path at source[%s] not matched with destination[%s]", sourceObjectInMap.relativePath, destinationObject.relativePath))
		}
		dataChanged, metadataChanged := f.HasFileChangedSinceLastSyncUsingTargetCompare(destinationObject, sourceObjectInMap)

		var err error
		if f.disableComparison || dataChanged {
			err = f.copyTransferScheduler(sourceObjectInMap)
		} else if metadataChanged {
			f.scannerLogger.Log(pipeline.LogInfo, fmt.Sprintf("File(%s) scheduled for file property transfer only", sourceObjectInMap.relativePath))

			sourceObjectInMap.entityType = common.EEntityType.FileProperties()

			// This is only file properties transfer, we don't want it to be accounted in bytes transferred.
			sourceObjectInMap.size = 0
			err = f.copyTransferScheduler(sourceObjectInMap)
		} else {
			// Neither data nor metadata for the file has changed, hence file is not transferred.
			if f.incrementNotTransferred != nil {
				f.incrementNotTransferred(sourceObjectInMap.entityType)
			}
		}

		atomic.AddInt64(&f.sourceFolderIndex.totalSize, -storedObjectSize(sourceObjectInMap))
		delete(foldermap.indexMap, lcFileName)

		f.sourceFolderIndex.lock.Unlock()
		return err
	}

	f.sourceFolderIndex.lock.Unlock()
	// Parent folder not present this may happen when all the entries for that folder in folder map processed.
	// In that case the foldemap will be deleted, so we know this file not present in source.
	// Other case is in source file changed to folder, so we need to delete the file at destination.
	_ = f.destinationCleaner(destinationObject)

	return nil
}

//
// Given both the local and target attributes of the file, find out if we need to copy data+metadata, only metadata, or none.
// This is called by TargetTraverser for children of directories that may have "changed" as detected by
// HasDirectoryChangedSinceLastSync(). For children of "changed" directories we cannot safely do local ctime
// checks as children with the same local and remote path may actually be completely different files and we need to
// compare the source and target files to know for sure.
//
// Unlike HasFileChangedSinceLastSyncUsingLocalChecks(), it needs the target attributes also, which means it can only
// be called for cases where we enumerate the target.
//
// Return: (dataChanged, metadataChanged)
func (f *syncDestinationComparator) HasFileChangedSinceLastSyncUsingTargetCompare(to StoredObject, so StoredObject) (bool, bool) {
	//
	// If mtime or size of target file is different from source file it means the file data (and metadata) has changed,
	// else only metadata has changed.
	//
	if to.size != so.size || so.lastModifiedTime.UnixNano() != to.lastModifiedTime.UnixNano() {
		return true, true
	} else {
		//
		// If we come here, we are sure that the source and target objects we are looking at refer to the same object (else it’s highly unlikely
		// that size and mtime both are equal). Now we need to find out if the source object could have its metadata updated after last sync.
		// For this we need to compare the target file’s ctime with the source file’s ctime. The way we compare depends on whether the target’s
		// ctime value is set by us or is it set by the target when we last sync’ed the object. If it’s set by us, then we can do a simple equality
		// comparison since if source ctime is updated after last sync it’ll not be same as the target ctime which we set at last sync time.
		// If target object’s ctime is set by the target (f.e., NFS target) then we check if the source ctime is greater than target ctime minus the
		// TargetCtimeSkew. If source ctime is greater then we know that the source object metadata was updated.
		//
		if f.TargetCtimeSkew == 0 {
			//
			// We would have set target object’s ctime equal to source object’s ctime when we last sync’ed the object, so if it’s not equal now,
			// it means the source object’s metadata was updated since last sync.
			//
			if so.lastChangeTime.UnixNano() != to.lastChangeTime.UnixNano() {
				if f.metaDataOnlySync {
					return false, true
				} else {
					return true, true
				}
			}
		} else if so.lastChangeTime.UnixNano() > to.lastChangeTime.UnixNano()-time.Unix(int64(f.TargetCtimeSkew), 0).UnixNano() {
			//
			// Target object’s ctime is set locally on the target so we cannot compare for equality with the source object’s ctime.
			// We can check if source object’s ctime was updated after target object’s ctime while accounting for the skew.
			// Note that if we choose a larger skew value we might wrongly consider some object as needing sync, but that’s harmless as compared to
			// choosing a smaller skew and missing out syncing some object that was changed.
			//
			if f.metaDataOnlySync {
				return false, true
			} else {
				return true, true
			}
		}
	}
	// File has not changed since last sync.
	return false, false
}

// with the help of an objectIndexer containing the destination objects
// filter out the source objects that should be transferred
// in other words, this should be used when source is being enumerated secondly
type syncSourceComparator struct {
	// the processor responsible for scheduling copy transfers
	copyTransferScheduler objectProcessor

	// storing the destination objects
	destinationIndex *folderIndexer

	disableComparison bool
}

func newSyncSourceComparator(i *folderIndexer, copyScheduler objectProcessor, disableComparison bool) *syncSourceComparator {
	return &syncSourceComparator{destinationIndex: i, copyTransferScheduler: copyScheduler, disableComparison: disableComparison}
}

// it will only transfer source items that are:
//	1. not present in the map
//  2. present but is more recent than the entry in the map
// note: we remove the StoredObject if it is present so that when we have finished
// the index will contain all objects which exist at the destination but were NOT seen at the source
func (f *syncSourceComparator) processIfNecessary(sourceObject StoredObject) error {
	relPath := sourceObject.relativePath

	if f.destinationIndex.isDestinationCaseInsensitive {
		relPath = strings.ToLower(relPath)
	}

	destinationObjectInMap, present := f.destinationIndex.folderMap[filepath.Dir(relPath)].indexMap[filepath.Base(relPath)]

	if present {
		defer delete(f.destinationIndex.folderMap[filepath.Dir(relPath)].indexMap, relPath)

		// if destination is stale, schedule source for transfer
		if f.disableComparison || sourceObject.isMoreRecentThan(destinationObjectInMap) {
			return f.copyTransferScheduler(sourceObject)
		}
		// skip if source is more recent
		return nil
	}

	// if source does not exist at the destination, then schedule it for transfer
	return f.copyTransferScheduler(sourceObject)
}
