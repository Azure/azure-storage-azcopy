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
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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

	sourceFolderIndex *folderIndexer

	disableComparison bool

	// Change file detection mode. Valid Enums will be TargetCompare, Ctime, CtimeMtime.
	// TargetCompare - Default sync comparsion where target enumerated for each file. It's least optimized, but gurantee no data loss.
	// Ctime - CTime used to detect change files/folder. Should be used where mtime not reliable.
	// CtimeMtime - Both CTime      and MTime used to detect changed files/folder. It's most efficient in all of the cfdModes.
	CFDMode common.CFDMode

	//
	// Time of last sync, used by the sync process.
	// This is used as a lower bound to find out files/dirs changed since last sync.
	// Depending on CFDMode this will be compared with source files' ctime/mtime or the target files' ctime/mtime.
	//
	lastSync time.Time

	// In case of metadata change only, shall we transfer whole file or only metadata. This flag governs that.
	MetaDataOnlySync bool

	TargetCtimeSkew time.Time
}

func newSyncDestinationComparator(i *folderIndexer, copyScheduler, cleaner objectProcessor, disableComparison bool, cfdMode common.CFDMode, lastSyncTime time.Time) *syncDestinationComparator {
	return &syncDestinationComparator{sourceFolderIndex: i, copyTransferScheduler: copyScheduler, destinationCleaner: cleaner, disableComparison: disableComparison,
		CFDMode:  cfdMode,
		lastSync: lastSyncTime}
}

// HasFileChangedSinceLastSyncUsingLocalChecks depending on mode returns dataChanged and metadataChanged.
// Given a file and the corresponding scanned source object, find out if we need to copy data, metadata, both, none.
// This is called by TargetTraverser. It honours various sync qualifiers to make the decision, f.e., if sync
// qualifiers allow ctime/mtime to be used for CFD it may not need to query file attributes from target.
//
// Note: Caller will use the returned information to decide whether to copy the storedObject to target and whether to
// 		 copy only data, only metadata or both.
//
// Note: This SHOULD NOT be called for children of "changed" directories, since for changed directories we cannot safely
// 	     check for changed files purely by doing time based comparison. Use HasFileChangedSinceLastSyncUsingTargetCompare()
// 		 for children of changed directories.
func (f *syncDestinationComparator) HasFileChangedSinceLastSyncUsingLocalChecks(so StoredObject, filePath string) (dataChange bool, metadataChange bool) {
	// Changed file detection using Ctime and Mtime.
	if f.CFDMode == common.CFDModeFlags.CtimeMtime() {
		// File Mtime changed, which means data changed and it cause metadata change.
		if so.lastModifiedTime.After(f.lastSync) {
			return true, true
		} else if so.lastChangeTime.After(f.lastSync) {
			// File Ctime changed only, only meta data changed.
			return false, true
		}
		// File not changed at all.
		return false, false
	} else if f.CFDMode == common.CFDModeFlags.Ctime() {
		// Changed file detection using Ctime only.

		// File changed since lastSync time. CFDMode is Ctime, so we can't rely on mtime as it can be modified by any other tool.
		if so.lastChangeTime.After(f.lastSync) {
			// If MetaDataSync Flag is false we don't need to check for data or metadata change. We can return true in that case.
			if !f.MetaDataOnlySync {
				return true, true
			} else {
				// Need to know whether data changed or metadata changed or both. For that we need to get properties of blob.
				// TODO: Need to do target traverse and get the properties of blob.
				//       Only problem with that we don't have any info about container at this point. Other point will be how much efficient this would be. As we
				// 		 try to get properties for each blob, so if in a directory 100 files are there and 2 changed then we need to make 2 getProperties call.
				// 		 Whereas if we do list of blob in a directory, we get all the result in one call for 5K blobs (exception to this directory stubs), where
				//       we need to call getproperties to know the metadata.
				return true, true
			}
		} else {
			// File Ctime not changed, means no data or metadata changed.
			return false, false
		}
	} else {
		// This is the case when neither CtimeMtime or Ctime CFDMode set. So its target traverse and if we reach here
		// means these entries for new files.
		return true, true
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

	if f.sourceFolderIndex.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(destinationObject.relativePath)
	} else {
		lcRelativePath = destinationObject.relativePath
	}

	lcFolderName = filepath.Dir(lcRelativePath)
	lcFileName = filepath.Base(lcRelativePath)

	f.sourceFolderIndex.lock.Lock()

	foldermap, folderPresent := f.sourceFolderIndex.folderMap[lcFolderName]

	// Do the auditing of map.
	defer func() {
		if present {
			delete(f.sourceFolderIndex.folderMap[lcFolderName].indexMap, lcFileName)
		}
		if folderPresent {
			if len(f.sourceFolderIndex.folderMap[lcFolderName].indexMap) == 0 {
				size := int64(unsafe.Sizeof(objectIndexer{}))
				atomic.AddInt64(&f.sourceFolderIndex.totalSize, -size)
				delete(f.sourceFolderIndex.folderMap, lcFolderName)
			}
		}
		f.sourceFolderIndex.lock.Unlock()
	}()

	// Folder Case.
	if destinationObject.isVirtualFolder || destinationObject.entityType == common.EEntityType.Folder() {

		if destinationObject.isFolderEndMarker {
			var size int64

			// Each folder storedObject stored in its parent directory, one exception to this is root folder.
			// Which is stored in root folder only as "." filename. End marker come for folder present on source.
			// NOTE: Following are the scenarios for end marker.
			//       1. End Marker came for folder which is not present on target(only happen in case of root folder and first copy).
			//			In that case folderPresent will be true. Otherwise this folder will be created by its parent.
			//       2. End Marker came for folder present on target. In that case we have only newly files.

			// This will happen if folder not present on target. Lets create folder first and then files.
			if folderPresent {
				if lcFolderName != "" {
					panic(fmt.Sprintf("Code should not reach here, except for root folder. LcFolderName: %s", lcFolderName))
				}
				storedObject := foldermap.indexMap[lcFileName]
				size += storedObjectSize(storedObject)
				delete(foldermap.indexMap, lcFileName)
				// This is more valid in case of azure files. Where we need to create empty folder too.
				// So it may happen root folder is empty and target side we need to create folder under some container.
				// As of now azcopy don't support empty folders so this is not required.
				f.copyTransferScheduler(storedObject)

				// Delete the parent map of folder if it's empty.
				if len(f.sourceFolderIndex.folderMap[lcFolderName].indexMap) == 0 {
					size := int64(unsafe.Sizeof(objectIndexer{}))
					atomic.AddInt64(&f.sourceFolderIndex.totalSize, -size)
					delete(f.sourceFolderIndex.folderMap, lcFolderName)
				}
			}

			lcFolderName = path.Join(lcFolderName, lcFileName)
			foldermap, folderPresent = f.sourceFolderIndex.folderMap[lcFolderName]
			// lets copy all the files underneath in this folder which are left out.
			// It may happen all the files present on target then there is nothing left in map, otherwise left files will be taken care.
			if folderPresent {
				for file := range foldermap.indexMap {
					storedObject := foldermap.indexMap[file]
					size += storedObjectSize(storedObject)
					delete(foldermap.indexMap, file)

					metaChange, dataChange := f.HasFileChangedSinceLastSyncUsingLocalChecks(storedObject, storedObject.relativePath)
					if dataChange {
						f.copyTransferScheduler(storedObject)
					}
					if metaChange {
						// TODO: Add calls to just update meta data of file.
					}
				}
			}
			size = -size
			atomic.AddInt64(&f.sourceFolderIndex.totalSize, size)

			if atomic.LoadInt64(&f.sourceFolderIndex.totalSize) < 0 {
				panic("Total Size is negative.")
			}
			return nil
		} else {
			// Folder present on source and its present on target too.
			if folderPresent {
				sourceObjectInMap = foldermap.indexMap[lcFileName]
				delete(foldermap.indexMap, lcFileName)
				size := storedObjectSize(sourceObjectInMap)
				size = -size
				atomic.AddInt64(&f.sourceFolderIndex.totalSize, size)
				if sourceObjectInMap.relativePath != destinationObject.relativePath {
					panic("Relative Path not matched")
				}
				if f.disableComparison || sourceObjectInMap.isMoreRecentThan(destinationObject) {
					err := f.copyTransferScheduler(sourceObjectInMap)
					if err != nil {
						return err
					}
				}
			} else {
				// We detect folder not present on source, now we need to delete the folder and files underneath.
				// TODO: Need to add call to delete the folder.
				_ = f.destinationCleaner(destinationObject)
			}
			return nil
		}
	}

	// File case.
	if folderPresent {
		sourceObjectInMap, present = foldermap.indexMap[lcFileName]

		// if the destinationObject is present at source and stale, we transfer the up-to-date version from source
		if present {
			size := storedObjectSize(sourceObjectInMap)
			size = -size
			atomic.AddInt64(&f.sourceFolderIndex.totalSize, size)
			if sourceObjectInMap.relativePath != destinationObject.relativePath {
				panic("Relative Path not matched")
			}
			if f.disableComparison {
				err := f.copyTransferScheduler(sourceObjectInMap)
				if err != nil {
					return err
				}
			} else {
				dataChanged, metadataChanged := f.HasFileChangedSinceLastSyncUsingTargetCompare(destinationObject, sourceObjectInMap)
				if dataChanged {
					err := f.copyTransferScheduler(sourceObjectInMap)
					if err != nil {
						return err
					}
				} else if metadataChanged {

				}
			}
		} else {
			_ = f.destinationCleaner(destinationObject)
		}
	} else {
		// TODO: Need to add the delete code which distinquish between blob and file.
		_ = f.destinationCleaner(destinationObject)
	}

	return nil
}

//
// Given both the local and target attributes of the file, find out if we need to copy data, metadata, both, none.
// This is called by TargetTraverser for children of directories that may have "changed" as detected by
// HasDirectoryChangedSinceLastSync(). For children of "changed" directories we cannot safely do local ctime
// checks as children with the same local and remote path may actually be completely different files and we need to
// compare the source and target files to know for sure.
//
// Unlike HasFileChangedSinceLastSyncUsingLocalChecks(), it needs the target attributes also, which means it can only
// be called for cases where we enumerate the target.
//
func (f *syncDestinationComparator) HasFileChangedSinceLastSyncUsingTargetCompare(to StoredObject, so StoredObject) (bool, bool) {
	//
	// If mtime or size of target file is different from source file it means the file data (and metadata) has changed,
	// else only metadata has changed.
	//
	if to.size != so.size || so.lastModifiedTime.After(to.lastModifiedTime) {
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
		if f.TargetCtimeSkew.IsZero() {
			//
			// We would have set target object’s ctime equal to source object’s ctime when we last sync’ed the object, so if it’s not equal now,
			// it means the source object’s metadata was updated since last sync.
			//
			if so.lastChangeTime.After(to.lastChangeTime) {
				if f.MetaDataOnlySync {
					return false, true
				} else {
					return true, true
				}
			}
		} else {
			//
			// Target object’s ctime is set locally on the target so we cannot compare for equality with the source object’s ctime.
			// We can check if source object’s ctime was updated after target object’s ctime while accounting for the skew.
			// Note that if we choose a larger skew value we might wrongly consider some object as needing sync, but that’s harmless as compared to
			// choosing a smaller skew and missing out syncing some object that was changed.
			//
			if f.MetaDataOnlySync {
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
