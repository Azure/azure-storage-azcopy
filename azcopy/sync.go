package azcopy

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// SyncOptions contains the optional parameters for the Sync operation.
type SyncOptions struct {
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
	preservePermissions common.PreservePermissionsOption
	blockSize           int64
	putBlobSize         int64
	cpkOptions          common.CpkOptions
	commandString       string
}

func (s SyncOptions) clone() SyncOptions {
	clone := s // shallow copy everything first

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

// Sync syncs the source and destination.
func (c *Client) Sync(ctx context.Context, src, dest string, opts SyncOptions) (result SyncResult, err error) {
	// Input
	if src == "" || dest == "" {
		return SyncResult{}, fmt.Errorf("source and destination must be specified for sync")
	}

	c.CurrentJobID = common.NewJobID()
	job := syncer{
		jobID: c.CurrentJobID,
	}

	// ValidateAndInferFromTo
	userFromTo := common.Iff(opts.FromTo == common.EFromTo.Unknown(), "", opts.FromTo.String())
	fromTo, err := InferAndValidateFromTo(src, dest, userFromTo)
	if err != nil {
		return SyncResult{}, err
	}
	common.SetNFSFlag(fromTo.IsNFSAware())
	job.source, job.destination, err = getSourceAndDestination(src, dest, fromTo)
	if err != nil {
		return SyncResult{}, err
	}
	job.opts, err = applyDefaultsAndInferSyncOptions(opts, fromTo)
	if err != nil {
		return SyncResult{}, err
	}
	err = job.validate()
	if err != nil {
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

	return SyncResult{}, nil
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
