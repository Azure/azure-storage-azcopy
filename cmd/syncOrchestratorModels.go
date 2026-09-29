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
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// DefaultSyncOrchestratorOptions provides a default-initialized SyncOrchestratorOptions struct.
var DefaultSyncOrchestratorOptions = SyncOrchestratorOptions{
	valid:                          true,
	maxDirectoryDirectChildCount:   100_000, // This will not get honored by e2e test framework
	metaDataOnlySync:               false,
	lastSuccessfulSyncJobStartTime: time.Now().Add(-10 * time.Minute), // Default to 10 minutes ago
	optimizeEnumerationByCTime:     false,
	parallelTraversers:             64,
}

// SyncOrchestratorOptions defines the options for the enumerator that are required for the sync operation.
// It contains various settings and configurations used during the sync process.
type SyncOrchestratorOptions struct {
	valid bool // Indicates whether the options are valid or not. If false, the enumerator will not be used.

	// MaxDirectoryDirectChildCount is the maximum number of direct children in a directory.
	// This is used to limit the number of direct children that can be enumerated in a directory.
	// This is useful for performance optimization and to avoid excessive memory usage.
	maxDirectoryDirectChildCount uint64

	// parallelTraversers is the number of parallel traversers to use for the sync operation.
	parallelTraversers int32

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

	fromTo common.FromTo
}

func (s *SyncOrchestratorOptions) validate(from common.Location) error {

	if !s.valid {
		return nil
	}

	if !UseSyncOrchestrator {
		return errors.New("sync orchestrator options should only be used when UseSyncOrchestrator is true")
	}

	if from != common.ELocation.Local() && from != common.ELocation.S3() && from != common.ELocation.Blob() && from != common.ELocation.BlobFS() && from != common.ELocation.File() {
		return errors.New("sync optimizations using timestamps should only be used for supported source locations (Local, S3, Blob, BlobFS, File)")
	}

	if s.maxDirectoryDirectChildCount == 0 {
		return errors.New("maxDirectoryDirectChildCount must be greater than 0")
	}

	if s.parallelTraversers <= 0 {
		return errors.New("parallelTraversers must be greater than 0")
	}

	if s.optimizeEnumerationByCTime && s.lastSuccessfulSyncJobStartTime.IsZero() {
		return errors.New("lastSuccessfulSyncJobStartTime must be a valid time for CTime optimization")
	}

	return nil
}

func IsSyncOrchestratorOptionsValid(orchestratorOptions *SyncOrchestratorOptions) bool {
	return orchestratorOptions != nil && orchestratorOptions.valid
}

func (s *SyncOrchestratorOptions) IsMetaDataOnlySync() bool {
	return s.valid && s.metaDataOnlySync
}

func (s *SyncOrchestratorOptions) GetMaxDirectoryDirectChildCount() uint64 {
	if s.valid && s.maxDirectoryDirectChildCount > 0 {
		return s.maxDirectoryDirectChildCount
	}

	panic("maxDirectoryDirectChildCount is not valid or not set")
}

// NewSyncOrchestratorOptions initializes a SyncOrchestratorOptions struct with the provided parameters.
func NewSyncOrchestratorOptions(
	maxDirectoryDirectChildCount uint64,
	metaDataOnlySync bool,
	lastSuccessfulSyncJobStartTime time.Time,
	optimizeEnumerationByCTime bool,
	parallelTraversers int32,
) SyncOrchestratorOptions {

	return SyncOrchestratorOptions{
		maxDirectoryDirectChildCount:   maxDirectoryDirectChildCount,
		metaDataOnlySync:               metaDataOnlySync,
		lastSuccessfulSyncJobStartTime: lastSuccessfulSyncJobStartTime,
		optimizeEnumerationByCTime:     optimizeEnumerationByCTime,
		parallelTraversers:             parallelTraversers,
		valid:                          true,
	}
}

func (s *SyncOrchestratorOptions) ToStringMap() map[string]string {
	m := make(map[string]string)
	m["valid"] = fmt.Sprintf("%t", s.valid)
	m["maxDirectoryDirectChildCount"] = fmt.Sprintf("%d", s.maxDirectoryDirectChildCount)
	m["parallelTraversers"] = fmt.Sprintf("%d", s.parallelTraversers)
	m["metaDataOnlySync"] = fmt.Sprintf("%t", s.metaDataOnlySync)
	m["lastSuccessfulSyncJobStartTime"] = s.lastSuccessfulSyncJobStartTime.Format(time.RFC3339)
	m["optimizeEnumerationByCTime"] = fmt.Sprintf("%t", s.optimizeEnumerationByCTime)
	return m
}

// Function to initialize a default SyncEnumeratorOptions struct object
func NewDefaultSyncOrchestratorOptions() SyncOrchestratorOptions {
	return DefaultSyncOrchestratorOptions
}

// Function to initialize a test SyncEnumeratorOptions struct object
func NewTestSyncOrchestratorOptions() SyncOrchestratorOptions {
	utcString := "Sat Jun 28 22:30:15 UTC 2025"
	layout := "Mon Jan 2 15:04:05 MST 2006"

	customTime, err := time.Parse(layout, utcString)
	if err != nil {
		customTime = time.Now().Add(-5 * time.Minute)
	}

	customTime = time.Now().Add(-10 * time.Minute)

	return SyncOrchestratorOptions{
		maxDirectoryDirectChildCount:   1_000_000,
		metaDataOnlySync:               true,
		lastSuccessfulSyncJobStartTime: customTime,
		optimizeEnumerationByCTime:     true,
		valid:                          true,
		parallelTraversers:             64,
	}
}
