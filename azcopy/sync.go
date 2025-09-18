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

type SyncResult struct {
	SourceFilesScanned      uint64
	DestinationFilesScanned uint64
	common.ListJobSummaryResponse
	DeleteTotalTransfers     uint32
	DeleteTransfersCompleted uint32
	ElapsedTime              time.Duration
}

type SyncJobHandler interface {
	OnStart(ctx common.JobContext)
	OnTransferProgress(progress SyncJobProgress)
	OnScanProgress(progress common.ScanProgress)
}

type SyncJobProgress struct {
	common.ListJobSummaryResponse
	DeleteTotalTransfers     uint32
	DeleteTransfersCompleted uint32
	Throughput               float64
	ElapsedTime              time.Duration
}

type SyncScanProgress struct {
	SourceFilesScanned      uint64
	DestinationFilesScanned uint64
}

// SyncOptions contains the optional parameters for the Sync operation.
type SyncOptions struct {
	FromTo                  common.FromTo
	Recursive               *bool // Default true
	IncludeDirectoryStubs   bool
	PreserveInfo            *bool // Default true
	PreservePosixProperties bool
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
	PutMd5                  bool                        // TODO: (gapra) Should we make this an enum called PutHash for None/MD5? So user can set the HashType?
	CheckMd5                common.HashValidationOption // TODO (gapra) Same comment as above
	S2SPreserveAccessTier   *bool                       // Default true
	S2SPreserveBlobTags     bool
	CpkByName               string
	CpkByValue              bool
	MirrorMode              bool
	TrailingDot             common.TrailingDotOption
	IncludeRoot             bool
	CompareHash             common.SyncHashType
	HashMetaDir             string
	LocalHashStorageMode    *common.HashStorageMode // Default based on OS
	PreservePermissions     bool
	Hardlinks               common.HardlinkHandlingType

	dryrun                           bool
	deleteDestinationFileIfNecessary bool
	commandString                    string
	dryrunJobPartOrderHandler        func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	dryrunDeleteHandler              ObjectDeleter
}

/* AzCopy internal use only. Exposing these as setters to add a hurdle to their use. */

func (s *SyncOptions) SetInternalOptions(dryrun, deleteDestinationFileIfNecessary bool, cmd string, dryrunJobPartOrderHandler func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse, dryrunDeleteHandler ObjectDeleter) {
	s.dryrun = dryrun
	s.dryrunJobPartOrderHandler = dryrunJobPartOrderHandler
	s.dryrunDeleteHandler = dryrunDeleteHandler
	s.deleteDestinationFileIfNecessary = deleteDestinationFileIfNecessary
	s.commandString = cmd
}

// Sync synchronizes the contents between source and destination.
func (c *Client) Sync(ctx context.Context, src, dest string, opts SyncOptions, handler SyncJobHandler) (SyncResult, error) {
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
	defer func() {
		if common.AzcopyCurrentJobLogger != nil {
			common.AzcopyCurrentJobLogger.CloseLog()
		}
	}()

	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	traverser.EnumerationParallelism, traverser.EnumerationParallelStatFiles = jobsAdmin.JobsAdmin.GetConcurrencySettings()

	// set up the front end scanning logger
	common.AzcopyScanningLogger = common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "-scanning")
	common.AzcopyScanningLogger.OpenLog()
	defer func() {
		common.AzcopyScanningLogger.CloseLog()
	}()

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	var s *syncer
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	s, err := newSyncer(ctx, jobID, src, dest, opts, handler, c.GetUserOAuthTokenManagerInstance())
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

	// Log to job log
	if common.AzcopyCurrentJobLogger != nil {
		// TODO : I think some of this can be simplified after we are done with the copy job refactor.
		// There should be no need to transform these types so many times
		_, logStats := common.FormatExtraStats(common.EJobType.Sync(), finalSummary.AverageIOPS, finalSummary.AverageE2EMilliseconds, finalSummary.NetworkErrorPercentage, finalSummary.ServerBusyPercentage)
		common.AzcopyCurrentJobLogger.Log(common.LogInfo, logStats+"\n"+common.GetJobSummaryOutputBuilder(common.JobSummary{
			ListJobSummaryResponse:   finalSummary,
			DeleteTransfersCompleted: s.spt.getDeletionCount(),
			DeleteTotalTransfers:     s.spt.getDeletionCount(),
			SourceFilesScanned:       s.spt.getSourceFilesScanned(),
			DestinationFilesScanned:  s.spt.getDestinationFilesScanned(),
			ElapsedTime:              s.spt.GetElapsedTime(),
			JobType:                  common.EJobType.Sync(),
		})(common.EOutputFormat.Text()))
	}

	return SyncResult{
		SourceFilesScanned:       s.spt.getSourceFilesScanned(),
		DestinationFilesScanned:  s.spt.getDestinationFilesScanned(),
		ListJobSummaryResponse:   finalSummary,
		DeleteTotalTransfers:     s.spt.getDeletionCount(),
		DeleteTransfersCompleted: s.spt.getDeletionCount(),
		ElapsedTime:              s.spt.GetElapsedTime(),
	}, nil
}
