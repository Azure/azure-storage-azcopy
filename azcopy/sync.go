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
	Handler SyncJobHandler

	FromTo                  common.FromTo
	Recursive               *bool // Default true
	IncludeDirectoryStubs   bool
	PreserveInfo            *bool
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
	S2SPreserveAccessTier   *bool
	S2SPreserveBlobTags     bool
	CpkByName               string
	CpkByValue              bool
	MirrorMode              bool
	TrailingDot             common.TrailingDotOption
	IncludeRoot             bool
	CompareHash             common.SyncHashType
	HashMetaDir             string
	LocalHashStorageMode    *common.HashStorageMode // has custom default logic
	PreservePermissions     bool
	Hardlinks               common.HardlinkHandlingType

	// AzCopy CLI only options TODO (gapra): Should I prefix these names with Internal to make it clear?
	DryRun                           bool
	DeleteDestinationFileIfNecessary bool

	// internal use only
	source               common.ResourceString
	destination          common.ResourceString
	preservePermissions  common.PreservePermissionsOption
	blockSize            int64
	putBlobSize          int64
	cpkOptions           common.CpkOptions
	commandString        string
	filters              []traverser.ObjectFilter
	folderPropertyOption common.FolderPropertyOption

	srcServiceClient  *common.ServiceClient
	srcCredType       common.CredentialType
	destServiceClient *common.ServiceClient
	destCredType      common.CredentialType
}

func (s SyncOptions) clone() SyncOptions {
	clone := s // shallow copy everything first

	clone.source = s.source
	clone.destination = s.destination

	// Deep copy pointer fields
	if s.Recursive != nil {
		v := *s.Recursive
		clone.Recursive = &v
	}
	if s.PreserveInfo != nil {
		v := *s.PreserveInfo
		clone.PreserveInfo = &v
	}
	if s.S2SPreserveAccessTier != nil {
		v := *s.S2SPreserveAccessTier
		clone.S2SPreserveAccessTier = &v
	}
	if s.LocalHashStorageMode != nil {
		v := *s.LocalHashStorageMode
		clone.LocalHashStorageMode = &v
	}

	// Deep copy slices
	clone.IncludePatterns = append([]string(nil), s.IncludePatterns...)
	clone.ExcludePatterns = append([]string(nil), s.ExcludePatterns...)
	clone.ExcludePaths = append([]string(nil), s.ExcludePaths...)
	clone.IncludeAttributes = append([]string(nil), s.IncludeAttributes...)
	clone.ExcludeAttributes = append([]string(nil), s.ExcludeAttributes...)
	clone.IncludeRegex = append([]string(nil), s.IncludeRegex...)
	clone.ExcludeRegex = append([]string(nil), s.ExcludeRegex...)

	return clone
}

// WithCommandString sets the command string for the sync operation (for logging purposes).
// This is set internally by AzCopy CLI.
func (s *SyncOptions) WithCommandString(cmd string) {
	s.commandString = cmd
}

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

// Sync syncs the source and destination.
func (c *Client) Sync(ctx context.Context, src, dest string, opts SyncOptions) (result SyncResult, err error) {
	// Input
	if src == "" || dest == "" {
		return SyncResult{}, fmt.Errorf("source and destination must be specified for sync")
	}

	// AzCopy CLI sets this globally before calling ResumeJob.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	syncHandler := common.GetLifecycleMgr()
	if syncHandler == nil {
		syncHandler = common.NewJobUIHooks()
		common.SetUIHooks(syncHandler)
	}

	job := newSyncer()
	c.CurrentJobID = job.jobID

	// ValidateAndInferFromTo
	userFromTo := common.Iff(opts.FromTo == common.EFromTo.Unknown(), "", opts.FromTo.String())
	fromTo, err := InferAndValidateFromTo(src, dest, userFromTo)
	if err != nil {
		return SyncResult{}, err
	}
	common.SetNFSFlag(fromTo.IsNFSAware())

	job.opts, err = applyDefaultsAndInferSyncOptions(src, dest, fromTo, opts)
	if err != nil {
		return SyncResult{}, err
	}

	err = job.validate()
	if err != nil {
		return SyncResult{}, err
	}

	// Service Client
	job.opts.srcServiceClient, job.opts.srcCredType, err = getSourceServiceClient(
		ctx, job.opts.source, job.opts.FromTo.From(), job.opts.TrailingDot, job.opts.cpkOptions, c.GetUserOAuthTokenManagerInstance())
	if err != nil {
		return SyncResult{}, err
	}
	job.opts.destServiceClient, job.opts.destCredType, err = getDestinationServiceClient(
		ctx, job.opts.destination, job.opts.FromTo, job.opts.srcCredType, job.opts.TrailingDot, job.opts.cpkOptions, c.GetUserOAuthTokenManagerInstance())
	if err != nil {
		return SyncResult{}, err
	}

	// Validate
	if job.opts.FromTo.IsS2S() && job.opts.srcCredType != common.ECredentialType.Anonymous() {
		if job.opts.srcCredType.IsAzureOAuth() && job.opts.FromTo.To().CanForwardOAuthTokens() {
			// no-op, this is OK
		} else if job.opts.srcCredType == common.ECredentialType.GoogleAppCredentials() || job.opts.srcCredType == common.ECredentialType.S3AccessKey() || job.opts.srcCredType == common.ECredentialType.S3PublicBucket() {
			// this too, is OK
		} else if job.opts.srcCredType == common.ECredentialType.Anonymous() {
			// this is OK
		} else {
			return SyncResult{}, fmt.Errorf("the source of a %s->%s sync must either be public, or authorized with a SAS token; blob destinations can forward OAuth", job.opts.FromTo.From(), job.opts.FromTo.To())
		}
	}
	// Check protocol compatibility for File Shares
	if err = ValidateProtocolCompatibility(ctx, job.opts.FromTo, job.opts.source, job.opts.destination, job.opts.srcServiceClient, job.opts.destServiceClient); err != nil {
		return SyncResult{}, err
	}

	// Job
	timeAtPrestart := time.Now()

	common.AzcopyCurrentJobLogger = common.NewJobLogger(c.CurrentJobID, c.GetLogLevel(), common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()
	defer common.AzcopyCurrentJobLogger.CloseLog()
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

	// set up the scanning logger
	common.AzcopyScanningLogger = common.NewJobLogger(c.CurrentJobID, c.GetLogLevel(), common.LogPathFolder, "-scanning")
	common.AzcopyScanningLogger.OpenLog()
	defer common.AzcopyScanningLogger.CloseLog()

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	// TODO : Command string for library users?

	mgr := NewJobLifecycleManager(syncHandler)

	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	se, err := job.initEnumerator(ctx, c.GetLogLevel(), mgr)
	if err != nil {
		return SyncResult{}, err
	}

	mgr.InitiateProgressReporting(ctx, job)

	err = se.Enumerate()

	if err != nil {
		return SyncResult{}, err
	}
	// if we are in dryrun mode, we don't want to actually run the job, so return here
	if job.opts.DryRun {
		return SyncResult{}, nil
	}

	err = mgr.Wait()
	if err != nil {
		return SyncResult{}, err
	}

	// Get final job summary
	finalSummary := jobsAdmin.GetJobSummary(job.jobID)
	finalSummary.SkippedSymlinkCount = job.getSkippedSymlinkCount()
	finalSummary.SkippedSpecialFileCount = job.getSkippedSpecialFileCount()

	// Log to job log
	if common.AzcopyCurrentJobLogger != nil {
		// TODO : I think some of this can be simplified after we are done with the copy job refactor.
		// There should be no need to transform these types so many times
		_, logStats := common.FormatExtraStats(common.EJobType.Sync(), finalSummary.AverageIOPS, finalSummary.AverageE2EMilliseconds, finalSummary.NetworkErrorPercentage, finalSummary.ServerBusyPercentage)
		common.AzcopyCurrentJobLogger.Log(common.LogInfo, logStats+"\n"+common.GetJobSummaryOutputBuilder(common.JobSummary{
			ListJobSummaryResponse:   finalSummary,
			DeleteTransfersCompleted: job.getDeletionCount(),
			DeleteTotalTransfers:     job.getDeletionCount(),
			SourceFilesScanned:       job.getSourceFilesScanned(),
			DestinationFilesScanned:  job.getDestinationFilesScanned(),
			ElapsedTime:              job.GetElapsedTime(),
			JobType:                  common.EJobType.Sync(),
		})(common.EOutputFormat.Text()))
	}

	return SyncResult{
		SourceFilesScanned:       job.getSourceFilesScanned(),
		DestinationFilesScanned:  job.getDestinationFilesScanned(),
		ListJobSummaryResponse:   finalSummary,
		DeleteTotalTransfers:     job.getDeletionCount(),
		DeleteTransfersCompleted: job.getDeletionCount(),
		ElapsedTime:              job.GetElapsedTime(),
	}, nil
}

func getSourceAndDestination(src, dst string, fromTo common.FromTo) (source, destination common.ResourceString, err error) {
	// TODO : (gapra) Why not just use fromTo.IsUpload/IsDownload/IsS2S?
	switch fromTo {
	case common.EFromTo.Unknown():
		err = fmt.Errorf("unable to infer the source '%s' / destination '%s'. ", src, dst)
		return
	case common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.LocalBlobFS(), common.EFromTo.LocalFileNFS():
		destination, err = traverser.SplitResourceString(dst, fromTo.To())
		common.PanicIfErr(err)
	case common.EFromTo.BlobLocal(), common.EFromTo.FileLocal(), common.EFromTo.BlobFSLocal(), common.EFromTo.FileNFSLocal():
		source, err = traverser.SplitResourceString(src, fromTo.From())
		common.PanicIfErr(err)
	case common.EFromTo.BlobBlob(), common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS(), common.EFromTo.BlobFile(), common.EFromTo.FileBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobFSFile(), common.EFromTo.BlobBlobFS(), common.EFromTo.FileBlobFS():
		destination, err = traverser.SplitResourceString(dst, fromTo.To())
		common.PanicIfErr(err)
		source, err = traverser.SplitResourceString(src, fromTo.From())
		common.PanicIfErr(err)
	default:
		err = fmt.Errorf("source '%s' / destination '%s' combination '%s' not supported for sync command ", src, dst, fromTo)
		return
	}

	// Do this check separately so we don't end up with a bunch of code duplication when new src/dstn are added
	if fromTo.From() == common.ELocation.Local() {
		source = common.ResourceString{Value: common.ToExtendedPath(common.CleanLocalPath(src))}
	} else if fromTo.To() == common.ELocation.Local() {
		destination = common.ResourceString{Value: common.ToExtendedPath(common.CleanLocalPath(dst))}
	}
	return
}
