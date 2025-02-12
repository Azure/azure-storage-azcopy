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
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
)

// TraverserErrorItemInfo provides an interface for error information related to files and folders that failed enumeration.
// It includes methods to retrieve th	e file path, file size, last modified time,
// whether the file is a directory, the error message, and whether the file is a source.
//
// Methods:
// - FullFilePath() string: Returns the path of the file.
// - FileName() string: Returns the name of the file.
// - FileSize() int64: Returns the size of the file in bytes.
// - FileLastModifiedTime() time.Time: Returns the last modified time of the file.
// - IsDir() bool: Returns true if the file is a directory, false otherwise.
// - ErrorMsg() error: Returns the error message associated with the file.
// - IsSource() bool: Returns true if the file is a source, false otherwise.
type TraverserErrorItemInfo interface {
	FullPath() string
	Name() string
	Size() int64
	LastModifiedTime() time.Time
	IsDir() bool
	ErrorMessage() error
	IsSource() bool
}

// Hierarchical object indexer for quickly searching children of a given directory.
// We keep parent and child relationship only not ancestor. That's said it's flat and hierarchical namespace where all folder at flat level,
// each folder contains its children. So it helps us take decision on folder. Used by the streaming sync processor.
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

// Structure to store possibly renamed directories, used by hasAnAncestorThatIsPossiblyRenamed() which
// tells if any of the ancestors of a source directory could have been possibly
// renamed. For any “possibly renamed” directory we must enumerate all
// the children/grandchildren target directories to ensure that we copy all
// their children correctly, even though the ctime/mtime of those directories
// won’t be more than LastSyncTime.
type possiblyRenamedMap struct {
	folderMap map[string]struct{}

	lock sync.RWMutex

	// isDestinationCaseInsensitive is true when the destination is case-insensitive
	// In Windows, both paths D:\path\to\dir and D:\Path\TO\DiR point to the same resource.
	// Apple File System (APFS) can be configured to be case-sensitive or case-insensitive.
	// So for such locations, the key in the indexMap will be lowercase to avoid infinite syncing.
	isDestinationCaseInsensitive bool
}

// SyncTraverserOptions defines the options for the enumerator that are required for the sync operation.
// It contains various settings and configurations used during the sync process.
type SyncTraverserOptions struct {
	// Hierarchical object indexer for quickly searching children of a given directory.
	// We keep parent and child relationship only not ancestor. That's said it's flat and hierarchical namespace where all folder at flat level,
	// each folder contains its children. So it helps us take decision on folder. Used by the streaming sync processor.
	indexerMap *folderIndexer

	// Structure to store possibly renamed directories, used by hasAnAncestorThatIsPossiblyRenamed() which
	// tells if any of the ancestors of a source directory could have been possibly
	// renamed. For any “possibly renamed” directory we must enumerate all
	// the children/grandchildren target directories to ensure that we copy all
	// their children correctly, even though the ctime/mtime of those directories
	// won’t be more than LastSyncTime.
	possiblyRenamedMap *possiblyRenamedMap

	// It is communication channel b/w source and destination and required in case of sync.
	orderedTqueue parallel.OrderedTqueueInterface

	// isSource indicates if the enumerator is for the source.
	isSource bool

	// isSync indicates if the operation is a sync operation.
	isSync bool

	//
	// Limit on size of ObjectIndexerMap in memory.
	// This is used only by the sync process and it controls how much the source traverser can fill the ObjectIndexerMap before it has to wait
	// for target traverser to process and empty it. This should be kept to a value less than the system RAM to avoid thrashing.
	// If you are not interested in source traverser scanning all the files for estimation purpose you can keep it low, just enough to never have the
	// target traverser wait for directories to scan. The only reason we would want to keep it high is to let the source complete scanning irrespective
	// of the target traverser speed, so that the scanned information can then be used for ETA estimation.
	//
	maxObjectIndexerSizeInGB uint32

	//
	// This is the time of last sync in ISO8601 format. This is compared with the source files' ctime/mtime value to find out if they have changed
	// since the last sync and hence need to be copied. It's not used if the CFDMode is anything other than CTimeMTime and CTime, since in other
	// CFDModes it's not considered safe to trust file times and we need to compare every file with target to find out which files have changed.
	// There are a few subtelties that caller should be aware of:
	//
	// Since sync takes finite time, this should be set to the start of sync and not any later, else files that changed in the source while the
	// last sync was running may be (incorrectly) skipped in this sync. Infact, to correctly account for any time skew between the machine running AzCopy
	// and the machine hosting the source filesystem (if they are different, f.e., when the source is an NFS/SMB mount) this should be set to few seconds
	// in the past. 60 seconds is a good value for skew adjustment. A larger skew value could cause more data to be sync'ed while a smaller skew value may
	// cause us to miss some changed files, latter is obviously not desirable. If we are not sure what skew value to use, it's best to create a temp file
	// on the source and compare its ctime with the nodes time and use that as a baseline.
	//
	// Time of last sync, used by the sync process.
	// A file/directory having ctime/mtime greater than lastSyncTime is considered "changed", though the exact interpretation depends on the CFDMode
	// used and other variables. Depending on CFDMode this will be compared with source files' ctime/mtime and/or target files' ctime/mtime.
	//
	lastSyncTime time.Time

	//
	// Change file detection mode.
	// This controls how target traverser decides whether a file has changed (and hence needs to be sync'ed to the target) by looking at the file
	// properties stored in the sourceFolderIndex. Valid Enums will be TargetCompare, Ctime, CtimeMtime.
	//
	// TargetCompare - This is the most generic and the slowest of all. It enumerates all target directories
	//                 and compares the children at source and target to find out if an object has changed.
	//
	// CtimeMtime    - Compare source file’s mtime/ctime (Unix/NFS) or LastWriteTime/ChangeTime (Windows/SMB) with LastSyncTime for detecting changed files.
	//                 If mtime/LastWriteTime > LastSyncTime then it means both data and metadata have changed else if ctime/ChangeTime > LastSyncTime then
	//                 only metadata has changed. For detecting changed directories this uses ctime/ChangeTime (not mtime/LastWriteTime) of the directory,
	//                 changed directories will be enumerated in the target and every object will be compared with the source to find changes. This is needed
	//                 to cover the case of directory renames where a completely different directory in the source can have the same name as a target directory
	//                 and checking only mtime/LastWriteTime of children is not safe since rename of a directory won’t affect mtime of its children. If a directory
	//                 has not changed then it’ll compare mtime/LastWriteTime and ctime/ChangeTime of its children with LastSyncTime for finding data/metadata changes,
	//                 thus it enumerates only target directories that have changed.
	//                 This is the most efficient of all and should be used when we safely can use it, i.e., we know that source updates ctime/mtime correctly.
	//
	// Ctime         - If we don’t want to depend on mtime/LastWriteTime (since it can be changed by applications) but we still don’t want to lose the advantage of
	//                 ctime/ChangeTime which is much more dependable, we can use this mode. In this mode we use only ctime/ChangeTime to detect if there’s a change
	//                 to a file/directory and if there’s a change, to find the exact change (data/metadata/both) we compare the file with the target. This has the
	//                 advantage that we use the more robust ctime/ChangeTime and only for files/directories which could have potentially changed we have
	//                 to compare with the target. This results in a much smaller set of files to enumerate at the target.
	//
	// So, in the order of preference we have,
	//
	// CtimeMtime -> Ctime -> TargetCompare.
	//
	cfdMode common.CFDMode

	//
	// Sync only file metadata if only metadata has changed and not the file content, else for changed files both file data and metadata are sync’ed.
	// The latter could be expensive especially for cases where user updates some file metadata (owner/mode/atime/mtime/etc) recursively. How we find out
	// whether only metadata has changed or only data has changed, or both have changed depends on the CFDMode that controls how changed files are detected.
	//
	metaDataOnlySync bool

	// scannerLogger is a logger that can be reset.
	scannerLogger common.ILoggerResetable
}

// Function to initialize a default SyncEnumeratorOptions struct object
func NewDefaultSyncTraverserOptions() SyncTraverserOptions {
	return SyncTraverserOptions{
		indexerMap:               nil,
		possiblyRenamedMap:       nil,
		orderedTqueue:            nil,
		isSource:                 false,
		isSync:                   false,
		maxObjectIndexerSizeInGB: 0,
		lastSyncTime:             time.Time{},                      // -1 indicates no limit
		cfdMode:                  common.CFDModeFlags.NotDefined(), // Default timeout of 30 seconds
		metaDataOnlySync:         false,
		scannerLogger:            nil,
	}
}
