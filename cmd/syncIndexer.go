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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shubham808/azure-storage-azcopy/v10/common"
	"github.com/shubham808/azure-storage-azcopy/v10/common/parallel"
)

//
// Hierarchical object indexer for quickly searching children of a given directory.
// We keep parent and child relationship only not ancestor. That's said it's flat and hierarchical namespace where all folder at flat level,
// each folder contains its children. So it helps us take decision on folder. Used by the streaming sync processor.
//
type folderIndexer struct {
	// FolderMap is a map for a folder, which stores map of files in respective folder.
	// Key here is parent folder f.e dir1/dir2/file.txt the key is dir1/dir2. It has objectIndexer map for this path.
	// ObjectIndexer is again map with key file.txt and it stores storedObject of this file.
	folderMap map[string]*objectIndexer

	// lock should be held when reading/modifying folderMap.
	lock sync.RWMutex

	// Memory consumed in bytes by this folderIndexer, including all the objectIndexer data it stores.
	// It's signed as it help to do sanity check.
	totalSize int64

	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

// Constructor for folderIndexer.
func newfolderIndexer() *folderIndexer {
	return &folderIndexer{folderMap: make(map[string]*objectIndexer)}
}

// storedObjectSize function calculates the size of given stored object.
func storedObjectSize(so StoredObject) int64 {

	return int64(unsafe.Sizeof(StoredObject{})) +
		int64(len(so.name)+
			len(so.relativePath)+
			len(so.contentDisposition)+
			len(so.cacheControl)+
			len(so.contentLanguage)+
			len(so.contentEncoding)+
			len(so.contentType)+
			len(so.ContainerName)+
			len(so.DstContainerName)+
			len(so.md5))
}

// folderIndexer.store() is called by Source Traverser for all scanned objects.
// It stores the storedObject in folderIndexer.folderMap["parent directory path"] where Target Traverser can
// look it up for finding if the object needs to be sync'ed.
//
// Note: Source Traverser blindly saves all the scanned objects for Target Traverser to look up.
//       All the intelligence regarding which object needs to be sync'ed, whether it's a full sync or
//       just metadata sync, is in the Target Traverser.
func (i *folderIndexer) store(storedObject StoredObject) (err error) {
	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	var lcFileName, lcFolderName, lcRelativePath string
	var size int64

	/*
	 * processOneDirectoryWithAutoPacer() will queue up all children of a directory to be added to
	 * indexerMap, followed by the directory itself to be added to tqueue. Once a directory is added
	 * to tqueue, target traverser can pick the directory and start processing it, which means it will
	 * query indexerMap for attributes of the directory and its children.
	 * This means that BEFORE ADDING A DIRECTORY TO TQUEUE ALL THE DIRECTORY'S CHILDREN AND THE DIRECTORY
	 * ITSELF MUST HAVE THEIR ATTRIBUTES ADDED TO INDEXERMAP.
	 *
	 * f.e., For the following source structure,
	 *
	 * dir1/dir2/file1.txt
	 * dir1/dir2/file2.txt
	 * dir1/file3.txt
	 *
	 * When directory dir1/dir2 is queued to tqueue, target traverser will query the following entries from
	 * indexerMap.
	 *
	 * ["dir1/dir2"]["."]           <- for HasDirectoryChangedSinceLastSync() check.
	 * ["dir1/dir2"]["file1.txt"]
	 * ["dir1/dir2"]["file2.txt"]
	 *
	 * To make sure that all these entries are added correctly, processOneDirectoryWithAutoPacer() will send
	 * the following entries:
	 *
	 * dir1/dir2/<DummyGUID>    <- this is for the attributes of dir1/dir2 itself.
	 * dir1/dir2/file1.txt
	 * dir1/dir2/file2.txt
	 *
	 * We just need to add all these entries correctly to indexerMap and that should be enough to make sure
	 * all the required entries are added to indexerMap before the directory is added to tqueue.
	 * As we can see we use <DummyGUID> to represent the directory itself, since "dir1/dir2/." would be normalized
	 * to "dir1/dir2".
	 *
	 * Above ensures that each directory can be handled independent of other directories (even its children
	 * directories) and we can have multiple goroutines processing the scanned objects as long as all children
	 * of a directory (and the entry for directory itself) are added to the channel (processed by same goroutine).
	 */

	if i.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(storedObject.relativePath)
	} else {
		lcRelativePath = storedObject.relativePath
	}

	lcFolderName = filepath.Dir(lcRelativePath)
	lcFileName = filepath.Base(lcRelativePath)

	if lcFileName == parallel.DotSpecialGUID {
		lcFileName = "."
		if lcFolderName == "." {
			// "/<DummyGUID>" actually refers to "/"
			storedObject.relativePath = ""
		} else {
			// "/dir1/<DummyGUID>" actually refers to "/dir1"
			storedObject.relativePath = lcFolderName
		}
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	// Very first object scanned in the folder, create the objectIndexer for this folder.
	if _, ok := i.folderMap[lcFolderName]; !ok {
		i.folderMap[lcFolderName] = newObjectIndexer()
		size += int64(unsafe.Sizeof(objectIndexer{}))
	}

	// Create child entry for file/folder. For root folder this is not applicable as it's not child of anyone.
	if lcRelativePath != "" {
		if _, ok := i.folderMap[lcFolderName].indexMap[lcFileName]; !ok {
			i.folderMap[lcFolderName].indexMap[lcFileName] = storedObject
		} else {
			err := fmt.Errorf("FileName [%s] under Folder [%s] already present in map", lcFileName, lcFolderName)
			return err
		}
		size += storedObjectSize(storedObject)
	}

	atomic.AddInt64(&i.totalSize, size)

	return
}

//
// This MUST be called only with CFDMode==Ctime and MetadataOnlySync==true. This forces caller (target traverser) to use the more efficient ListDir for
// getting file properties in bulk instead of querying individual file properties.
//
func (i *folderIndexer) filesChangedInDirectory(relativePath string, lastSyncTime time.Time) bool {
	var lcRelativePath string

	if i.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(relativePath)
	} else {
		lcRelativePath = relativePath
	}

	i.lock.RLock()
	defer i.lock.RUnlock()

	if foldermap, ok := i.folderMap[lcRelativePath]; ok {
		for file := range foldermap.indexMap {
			so := foldermap.indexMap[file]
			if so.lastChangeTime.After(lastSyncTime) {
				return true
			}
		}
	} else {
		panic(fmt.Sprintf("Folder map not present for this relativePath: %s", lcRelativePath))
	}
	return false
}

//
// Given the relative path of a directory this returns the StoredObject corresponding to that directory.
// To be precise it returns the StoredObject stored in folderMap["dir1/dir2"]["."] for directory named "dir1/dir2."
//
// This MUST be called *only* by HasDirectoryChangedSinceLastSync() when it wants to find out if a directory dequeued from 'tqueue' must
// be enumerated at the target. Such a StoredObject would have been added by the source traverser for each directory it adds to tqueue.
//
func (i *folderIndexer) getStoredObject(relativePath string) StoredObject {
	var lcRelativePath, lcFolderName string

	if i.isDestinationCaseInsensitive {
		lcRelativePath = strings.ToLower(relativePath)
	} else {
		lcRelativePath = relativePath
	}

	//
	// Note: For the root directory, lcRelativePath will be "" but the SourceTraverser would have stored it's properties in folderMap["."], so that's why this conversion.
	//       For other path we can directly use the lcRelativePath to index the folderMap[].
	if lcRelativePath == "" {
		lcFolderName = "."
	} else {
		lcFolderName = lcRelativePath
	}

	i.lock.RLock()
	defer i.lock.RUnlock()

	if folderMap, ok := i.folderMap[lcFolderName]; ok {
		if so, ok := folderMap.indexMap["."]; ok {
			if so.entityType != common.EEntityType.Folder() {
				panic(fmt.Sprintf("StoredObject for relative path[%s] and folderName[%s] is not of type folder", relativePath, lcFolderName))
			}

			return so
		}
	}
	panic(fmt.Sprintf("Stored Object for relative path[%s] and folderName[%s] not found", relativePath, lcFolderName))
}

// getObjectIndexerMapSize return the size of map.
func (i *folderIndexer) getObjectIndexerMapSize() int64 {
	return atomic.LoadInt64(&i.totalSize)
}

// traverse called in last as sanity.
func (i *folderIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	found := false

	if atomic.LoadInt64(&i.totalSize) != 0 {
		panic(fmt.Sprintf("\n Total Size should be zero. Size: %v", atomic.LoadInt64(&i.totalSize)))
	}

	for _, folder := range i.folderMap {
		found = true
		fmt.Printf("\n Folder with relative path[%s] still in map", folder.indexMap["."].relativePath)
		for _, value := range folder.indexMap {
			fmt.Printf("\n File with relative path[%s] still in map", value.relativePath)
		}
	}

	// Let's panic in case there are entries in objectIndexerMap.
	if found {
		panic("Map should be empty but still it has some entries.")
	}
	return
}

// the objectIndexer is essential for the generic sync enumerator to work
// it can serve as a:
// 		1. objectProcessor: accumulate a lookup map with given StoredObjects
//		2. resourceTraverser: go through the entities in the map like a traverser
type objectIndexer struct {
	indexMap map[string]StoredObject

	counter int

	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

func newObjectIndexer() *objectIndexer {
	return &objectIndexer{indexMap: make(map[string]StoredObject)}
}

// process the given stored object by indexing it using its relative path
func (i *objectIndexer) store(storedObject StoredObject) (err error) {
	// TODO we might buffer too much data in memory, figure out whether we should limit the max number of files
	// TODO previously we used 10M as the max, but it was proven to be too small for some users

	// It is safe to index all StoredObjects just by relative path, regardless of their entity type, because
	// no filesystem allows a file and a folder to have the exact same full path.  This is true of
	// Linux file systems, Windows, Azure Files and ADLS Gen 2 (and logically should be true of all file systems).
	if i.isDestinationCaseInsensitive {
		lcRelativePath := strings.ToLower(storedObject.relativePath)
		i.indexMap[lcRelativePath] = storedObject
	} else {
		i.indexMap[storedObject.relativePath] = storedObject
	}
	i.counter += 1
	return
}

// go through the remaining stored objects in the map to process them
func (i *objectIndexer) traverse(processor objectProcessor, filters []ObjectFilter) (err error) {
	for _, value := range i.indexMap {
		err = processIfPassedFilters(filters, value, processor)
		_, err = getProcessingError(err)
		if err != nil {
			return
		}
	}
	return
}
