// Copyright © 2025 Microsoft <wastore@microsoft.com>
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
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// SyncOptions contains the optional parameters for the Sync operation.
type SyncOptions struct {
	Handler SyncHandler

	FromTo                  common.FromTo
	Recursive               *bool // Default true
	IncludeDirectoryStubs   bool
	PreserveInfo            *bool // Default true
	PreservePosixProperties bool
	PosixPropertiesStyle    common.PosixPropertiesStyle
	ForceIfReadOnly         bool
	BlockSizeMB             float64
	PutBlobSizeMB           float64
	IncludePatterns         []string
	ExcludePatterns         []string
	ExcludePaths            []string
	IncludeAttributes       []string
	ExcludeAttributes       []string
	IncludeRegex            []string
	ExcludeRegex            []string
	DeleteDestination       common.DeleteDestination
	PutHash                 bool
	CheckHash               common.HashValidationOption
	S2SPreserveAccessTier   *bool // Default true
	S2SPreserveBlobTags     bool
	CpkByName               string
	CpkByValue              bool
	MirrorMode              bool
	TrailingDot             common.TrailingDotOption
	IncludeRoot             bool
	CompareHash             common.SyncHashType
	HashMetaDir             string
	LocalHashStorageMode    *common.HashStorageMode // Default based on OS
	Symlinks                common.SymlinkHandlingType
	PreservePermissions     bool
	Hardlinks               common.HardlinkHandlingType

	dryrun                           bool
	deleteDestinationFileIfNecessary bool
	commandString                    string
	dryrunJobPartOrderHandler        func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	dryrunDeleteHandler              ObjectDeleter
}

type SyncHandler interface {
	OnStart(ctx JobContext)
	OnScanProgress(progress SyncScanProgress)
	OnTransferProgress(progress SyncProgress)
	OnComplete(result SyncResult)
}

type SyncScanProgress struct {
	SourceFilesScanned      uint64
	DestinationFilesScanned uint64
	Throughput              *float64
	JobID                   common.JobID
}

type SyncProgress struct {
	common.ListJobSummaryResponse
	DeleteTotalTransfers     uint32
	DeleteTransfersCompleted uint32
	Throughput               float64
	ElapsedTime              time.Duration
}

type SyncResult struct {
	SourceFilesScanned      uint64
	DestinationFilesScanned uint64
	common.ListJobSummaryResponse
	DeleteTotalTransfers     uint32
	DeleteTransfersCompleted uint32
	ElapsedTime              time.Duration
}

// SetInternalOptions is used to set options that are not meant to be exposed to the user through the public API.
// Note: This function is intended for internal use only and should not be used in user applications.
func (s *SyncOptions) SetInternalOptions(dryrun, deleteDestinationFileIfNecessary bool, cmd string, dryrunJobPartOrderHandler func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse, dryrunDeleteHandler ObjectDeleter) {
	s.dryrun = dryrun
	s.dryrunJobPartOrderHandler = dryrunJobPartOrderHandler
	s.dryrunDeleteHandler = dryrunDeleteHandler
	s.deleteDestinationFileIfNecessary = deleteDestinationFileIfNecessary
	s.commandString = cmd
}

func (c *Client) Sync(ctx context.Context, src, dest string, opts SyncOptions) (SyncResult, error) {
	// Input
	if src == "" || dest == "" {
		return SyncResult{}, fmt.Errorf("source and destination must be specified for sync")
	}

	// AzCopy CLI sets this globally before calling Sync.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	syncHandler := common.GetLifecycleMgr()
	if syncHandler == nil {
		syncHandler = common.NewJobUIHooks()
		common.SetUIHooks(syncHandler)
	}
	jobID := common.NewJobID()
	c.CurrentJobID = jobID
	timeAtPrestart := time.Now()
	common.AzcopyCurrentJobLogger = common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()
	defer common.AzcopyCurrentJobLogger.CloseLog()

	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, traverser.IncludeBeforeDateFilter{}.FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, traverser.IncludeAfterDateFilter{}.FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	traverser.EnumerationParallelism, traverser.EnumerationParallelStatFiles = jobsAdmin.JobsAdmin.GetConcurrencySettings()

	// set up the front end scanning logger
	common.AzcopyScanningLogger = common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "-scanning")
	common.AzcopyScanningLogger.OpenLog()
	defer common.AzcopyScanningLogger.CloseLog()

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	var s *syncer
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	s, err := newSyncer(ctx, jobID, src, dest, opts, c.GetUserOAuthTokenManagerInstance())
	if err != nil {
		return SyncResult{}, err
	}

	mgr := NewJobLifecycleManager(syncHandler)

	enumerator, err := s.initEnumerator(ctx, c.GetLogLevel(), mgr)
	if err != nil {
		return SyncResult{}, err
	}

	if !s.opts.dryrun {
		mgr.InitiateProgressReporting(ctx, s.spt)
	}
	err = enumerator.Enumerate()
	defer jobsAdmin.JobsAdmin.JobMgrCleanUp(jobID)

	if err != nil {
		return SyncResult{}, err
	}
	// if we are in dryrun mode, we don't want to actually run the job, so return here
	if s.opts.dryrun {
		return SyncResult{}, nil
	}

	err = mgr.Wait()
	if err != nil {
		return SyncResult{}, err
	}

	// Get final job summary
	finalSummary := jobsAdmin.GetJobSummary(s.spt.jobID)
	finalSummary.SkippedSymlinkCount = s.spt.getSkippedSymlinkCount()
	finalSummary.SkippedSpecialFileCount = s.spt.getSkippedSpecialFileCount()
	finalSummary.SkippedHardlinkCount = s.spt.getSkippedHardlinkCount()

	result := SyncResult{
		SourceFilesScanned:       s.spt.getSourceFilesScanned(),
		DestinationFilesScanned:  s.spt.getDestinationFilesScanned(),
		ListJobSummaryResponse:   finalSummary,
		DeleteTotalTransfers:     s.spt.getDeletionCount(),
		DeleteTransfersCompleted: s.spt.getDeletionCount(),
		ElapsedTime:              s.spt.GetElapsedTime(),
	}

	if common.AzcopyCurrentJobLogger != nil {
		common.AzcopyCurrentJobLogger.Log(common.LogInfo, GetSyncResult(result, true))
	}

	if opts.Handler != nil {
		opts.Handler.OnComplete(result)
	}

	return result, nil
}

type syncer struct {
	opts *cookedSyncOptions
	srp  *remoteProvider
	spt  *syncProgressTracker
}

func newSyncer(ctx context.Context, jobID common.JobID, src, dst string, opts SyncOptions, uotm *common.UserOAuthTokenManager) (s *syncer, err error) {
	cookedOpts, err := newCookedSyncOptions(src, dst, opts)
	if err != nil {
		return nil, err
	}
	syncRemote, err := newSyncRemoteProvider(ctx, uotm, cookedOpts.source, cookedOpts.destination,
		cookedOpts.fromTo, cookedOpts.cpkOptions, cookedOpts.trailingDot)
	if err != nil {
		return nil, err
	}
	progressTracker := newSyncProgressTracker(jobID, opts.Handler)
	return &syncer{opts: cookedOpts, srp: syncRemote, spt: progressTracker}, nil
}
