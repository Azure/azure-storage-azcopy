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
	"runtime"
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

// syncOrchestratorOptions defines the options for the enumerator that are required for the sync operation.
// It contains various settings and configurations used during the sync process.
type syncOrchestratorOptions struct {
	valid bool // Indicates whether the options are valid or not. If false, the enumerator will not be used.

	// MaxDirectoryDirectChildCount is the maximum number of direct children in a directory.
	// This is used to limit the number of direct children that can be enumerated in a directory.
	// This is useful for performance optimization and to avoid excessive memory usage.
	maxDirectoryDirectChildCount int64

	//
	// Sync only file metadata if only metadata has changed and not the file content, else for changed files both file data and metadata are sync’ed.
	// The latter could be expensive especially for cases where user updates some file metadata (owner/mode/atime/mtime/etc) recursively.
	//
	metaDataOnlySync bool

	// last successful sync job start time is the time when the last successful sync job started.
	// this is used by XDM Mover to optimize the enumeration of source objects.
	lastSuccessfulSyncJobStartTime time.Time

	// Optimize the enumeration by comparing ctime values of source objects with lastSuccessfulSyncJobStartTime
	optimizeEnumerationByCTime bool
}

func (s *syncOrchestratorOptions) validate(from common.Location) {
	if s.valid && from != common.ELocation.Local() {
		panic("sync optimizations using timestamps should only be used for local to remote syncs")
	}

	// The main limitation in windows OS that prevents us from using the optimizations is its dependendy on posix timestamps.
	// We can use the optimizations on Windows by disabling the ctime optimization.
	if runtime.GOOS != "linux" && s.valid {
		panic("sync optimizations using posix timestamps are not supported on non-linux platforms")
	}
}

// Function to initialize a default SyncEnumeratorOptions struct object
func NewDefaultSyncOrchestratorOptions() syncOrchestratorOptions {
	return syncOrchestratorOptions{
		maxDirectoryDirectChildCount:   0,
		metaDataOnlySync:               false,
		lastSuccessfulSyncJobStartTime: time.Time{},
		optimizeEnumerationByCTime:     false,
		valid:                          false,
	}
}

// Function to initialize a test SyncEnumeratorOptions struct object
func NewTestSyncOrchestratorOptions() syncOrchestratorOptions {
	utcString := "Sat Jun 28 22:30:15 UTC 2025"
	layout := "Mon Jan 2 15:04:05 MST 2006"

	customTime, err := time.Parse(layout, utcString)
	if err != nil {
		customTime = time.Now().Add(-5 * time.Minute)
	}

	customTime = time.Now().Add(-10 * time.Minute)

	return syncOrchestratorOptions{
		maxDirectoryDirectChildCount:   1_000_000,
		metaDataOnlySync:               true,
		lastSuccessfulSyncJobStartTime: customTime,
		optimizeEnumerationByCTime:     true,
		valid:                          true,
	}
}
