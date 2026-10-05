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
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// SyncOptions contains the optional parameters for the Sync operation.
type SyncOptions struct {
	Handler SyncHandler

	JobID                      common.JobID
	CredentialManager          cred.Manager
	SrcCredName                string
	DstCredName                string
	UseSyncOrchestrator        bool
	UseStreamingMergeJoin      bool
	OrchestratorOptions        *traverser.SyncOrchestratorOptions
	ErrorChannel               chan<- traverser.TraverserErrorItemInfo
	AllowLocalSymlinkFollowing bool
	BlobType                   common.BlobType
	BlockBlobTier              common.BlockBlobTier
	BackupMode                 bool
	PreserveOwner              *bool
	RetainJobState             bool

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

type MoverSyncStats = moverSyncStats

type SyncScanProgress struct {
	MoverSyncStats          `json:"MoverSyncStats"`
	SourceFilesScanned      uint64
	DestinationFilesScanned uint64
	Throughput              *float64
	JobID                   common.JobID
}

type SyncProgress struct {
	MoverSyncStats `json:"MoverSyncStats"`
	common.ListJobSummaryResponse
	DeleteTotalTransfers     uint32
	DeleteTransfersCompleted uint32
	Throughput               float64
	ElapsedTime              time.Duration
}

type SyncResult struct {
	MoverSyncStats          `json:"MoverSyncStats"`
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

func (c *Client) Sync(ctx context.Context, src, dest string, opts SyncOptions) (result SyncResult, err error) {
	// Input
	if ctx == nil {
		return SyncResult{}, fmt.Errorf("a context is required for sync")
	}
	if src == "" || dest == "" {
		return SyncResult{}, fmt.Errorf("source and destination must be specified for sync")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// AzCopy CLI sets this globally before calling Sync.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	syncHandler := common.GetLifecycleMgr()
	jobID := opts.JobID
	if jobID.IsEmpty() {
		jobID = common.NewJobID()
	}
	opts.JobID = jobID
	c.CurrentJobID = jobID
	timeAtPrestart := time.Now()
	jobLogger := common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "")
	jobLogger.OpenLog()
	retainResources := false
	defer func() {
		if !retainResources {
			jobLogger.CloseLog()
		}
	}()
	common.AzcopyCurrentJobLogger = jobLogger

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
	scanningLogger := common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "-scanning")
	scanningLogger.OpenLog()
	defer func() {
		if !retainResources {
			scanningLogger.CloseLog()
		}
	}()
	common.AzcopyScanningLogger = scanningLogger

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	var s *syncer
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	manager := opts.CredentialManager
	if manager == nil {
		manager = c.GetCredentialManager()
	}
	s, err = newSyncer(ctx, jobID, src, dest, opts, manager)
	if err != nil {
		return SyncResult{}, err
	}
	s.logger = scanningLogger
	mgr := NewJobLifecycleManager(syncHandler, jobLogger)
	cleanupAllowed := false
	defer func() {
		if retainResources {
			return
		}
		mgr.Stop()
		if closeErr := s.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
			retainResources = true
			return
		}
		if cleanupAllowed && !opts.RetainJobState && !s.opts.dryrun && s.spt.firstPartOrdered() {
			jobsAdmin.JobsAdmin.JobMgrCleanUp(jobID)
		}
	}()
	failSync := func(operationErr error) (SyncResult, error) {
		if drainErr := s.cancelAndDrain(cancel, jobID, mgr); drainErr != nil {
			retainResources = true
			return SyncResult{}, errors.Join(operationErr, fmt.Errorf("sync cancellation did not drain; job resources retained: %w", drainErr))
		}
		cleanupAllowed = true
		return SyncResult{}, operationErr
	}

	enumerator, err := s.initEnumerator(ctx, c.GetLogLevel(), mgr)
	if err != nil {
		return failSync(err)
	}

	if !s.opts.dryrun {
		mgr.InitiateProgressReporting(ctx, s.spt)
	}
	err = s.enumerate(ctx, enumerator)

	if err != nil {
		return failSync(err)
	}
	if ctx.Err() != nil {
		return failSync(ctx.Err())
	}
	// if we are in dryrun mode, we don't want to actually run the job, so return here
	if s.opts.dryrun {
		return SyncResult{}, nil
	}

	err = mgr.Wait()
	if err != nil {
		return failSync(err)
	}
	if ctx.Err() != nil {
		return failSync(ctx.Err())
	}
	cleanupAllowed = true

	// Get final job summary
	finalSummary := common.ListJobSummaryResponse{JobID: s.spt.jobID, JobStatus: common.EJobStatus.Completed()}
	if s.spt.firstPartOrdered() {
		finalSummary = jobsAdmin.GetJobSummary(s.spt.jobID, !buildmode.IsMover)
	}
	finalSummary.SkippedSymlinkCount = s.spt.getSkippedSymlinkCount()
	finalSummary.SkippedSpecialFileCount = s.spt.getSkippedSpecialFileCount()
	finalSummary.SkippedHardlinkCount = s.spt.getSkippedHardlinkCount()
	finalSummary.SkippedArchiveFileCount = s.spt.GetEnumerationStats().SkippedArchiveFileCount

	result = SyncResult{
		MoverSyncStats:           s.spt.Stats(),
		SourceFilesScanned:       s.spt.getSourceFilesScanned(),
		DestinationFilesScanned:  s.spt.getDestinationFilesScanned(),
		ListJobSummaryResponse:   finalSummary,
		DeleteTotalTransfers:     s.spt.getDeletionCount(),
		DeleteTransfersCompleted: s.spt.getDeletionCount(),
		ElapsedTime:              s.spt.GetElapsedTime(),
	}

	jobLogger.Log(common.LogInfo, GetSyncResult(result, true))

	if opts.Handler != nil {
		opts.Handler.OnComplete(result)
	}

	return result, nil
}

type syncer struct {
	logger     common.ILoggerResetable
	processor  *CopyTransferProcessor
	opts       *cookedSyncOptions
	srp        *remoteProvider
	spt        *syncProgressTracker
	inodeStore *common.InodeStore
}

// Close releases any resources held by the syncer, including the inode store.
func (s *syncer) Close() error {
	if s == nil || s.inodeStore == nil {
		return nil
	}
	err := s.inodeStore.Close()
	s.inodeStore = nil
	return err
}

func newSyncer(ctx context.Context, jobID common.JobID, src, dst string, opts SyncOptions, manager cred.Manager) (s *syncer, err error) {
	cookedOpts, err := newCookedSyncOptions(src, dst, opts)
	if err != nil {
		return nil, err
	}
	if err := common.SetBackupMode(cookedOpts.backupMode, cookedOpts.fromTo); err != nil {
		return nil, err
	}
	syncRemote, err := newSyncRemoteProvider(ctx, manager, cookedOpts.source, cookedOpts.destination,
		cookedOpts.fromTo, cookedOpts.cpkOptions, cookedOpts.trailingDot, opts.SrcCredName, opts.DstCredName)
	if err != nil {
		return nil, err
	}
	progressTracker := newSyncProgressTracker(jobID, opts.Handler, cookedOpts.fromTo)
	s = &syncer{opts: cookedOpts, srp: syncRemote, spt: progressTracker}
	if cookedOpts.hardlinks == common.PreserveHardlinkHandlingType {
		if cookedOpts.dryrun {
			s.inodeStore, err = common.NewTemporaryInodeStore()
		} else {
			s.inodeStore, err = common.NewInodeStoreForNewJob(jobID)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to initialize inode store: %w", err)
		}
	}
	return s, nil
}
