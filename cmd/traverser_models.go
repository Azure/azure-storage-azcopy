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
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
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

// SyncTraverserOptions defines the options for the enumerator that are required for the sync operation.
// It contains various settings and configurations used during the sync process.
type SyncTraverserOptions struct {
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
	// Sync only file metadata if only metadata has changed and not the file content, else for changed files both file data and metadata are sync’ed.
	// The latter could be expensive especially for cases where user updates some file metadata (owner/mode/atime/mtime/etc) recursively. How we find out
	// whether only metadata has changed or only data has changed, or both have changed.
	//
	metaDataOnlySync bool

	// scannerLogger is a logger that can be reset.
	scannerLogger common.ILoggerResetable
}

// Function to initialize a default SyncEnumeratorOptions struct object
func NewDefaultSyncTraverserOptions() SyncTraverserOptions {
	return SyncTraverserOptions{
		isSource:                 false,
		isSync:                   false,
		maxObjectIndexerSizeInGB: 0,
		metaDataOnlySync:         false,
		scannerLogger:            nil,
	}
}
