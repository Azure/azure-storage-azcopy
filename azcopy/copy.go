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

type CopyResult struct {
	common.ListJobSummaryResponse
	ElapsedTime time.Duration
}

type CopyJobHandler interface {
	OnStart(ctx common.JobContext)
}

// CopyOptions contains the optional parameters for the Copy operation.
type CopyOptions struct {
	IncludeBefore               *time.Time
	IncludeAfter                *time.Time
	IncludePatterns             []string
	IncludePaths                []string
	ExcludePaths                []string
	IncludeRegex                []string
	ExcludeRegex                []string
	ExcludePatterns             []string
	Overwrite                   common.OverwriteOption
	AutoDecompress              bool
	Recursive                   bool
	FromTo                      common.FromTo
	ExcludeBlobTypes            []common.BlobType
	BlockSizeMB                 float64
	PutBlobSizeMB               float64
	BlobType                    common.BlobType
	BlockBlobTier               common.BlockBlobTier
	PageBlobTier                common.PageBlobTier
	Metadata                    map[string]string
	ContentType                 string
	ContentEncoding             string
	ContentDisposition          string
	ContentLanguage             string
	CacheControl                string
	NoGuessMimeType             bool
	PreserveLastModifiedTime    bool
	PreservePermissions         bool
	AsSubDir                    *bool //Default true
	PreserveOwner               *bool // Default true
	PreserveInfo                *bool // Custom default logic
	PreservePosixProperties     bool
	Symlinks                    common.SymlinkHandlingType
	ForceIfReadOnly             bool
	BackupMode                  bool
	PutMd5                      bool // TODO: (gapra) Should we make this an enum called PutHash for None/MD5? So user can set the HashType?
	CheckMd5                    common.HashValidationOption
	IncludeAttributes           []string
	ExcludeAttributes           []string
	ExcludeContainers           []string
	CheckLength                 bool
	S2SPreserveProperties       *bool // Default true
	S2SPreserveAccessTier       *bool // Default true
	S2SDetectSourceChanged      bool
	S2SHandleInvalidateMetadata common.InvalidMetadataHandleOption
	ListOfVersionIds            string
	BlobTags                    map[string]string
	S2SPreserveBlobTags         bool
	IncludeDirectoryStubs       bool
	DisableAutoDecoding         bool
	TrailingDot                 common.TrailingDotOption
	CpkByName                   string
	CpkByValue                  bool
	Hardlinks                   common.HardlinkHandlingType

	listOfFiles                      string
	dryrun                           bool
	commandString                    string
	dryrunJobPartOrderHandler        func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	s2SGetPropertiesInBackend        *bool // Default true
	deleteDestinationFileIfNecessary bool
}

// WithInternalOptions is used to set options that are not meant to be exposed to the user through the public API.
// Note: This function is intended for internal use only and should not be used in user applications.
func (c *CopyOptions) WithInternalOptions(listOfFiles string, s2sGetPropertiesInBackend *bool, dryrun bool, dryrunJobPartOrderHandler func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse, deleteDestinationFileIfNecessary bool, cmd string) {
	c.listOfFiles = listOfFiles // this is not allowed to be set in conjunction with include-path
	c.s2SGetPropertiesInBackend = s2sGetPropertiesInBackend
	c.dryrun = dryrun
	c.dryrunJobPartOrderHandler = dryrunJobPartOrderHandler
	c.deleteDestinationFileIfNecessary = deleteDestinationFileIfNecessary
	c.commandString = cmd
}

// Copy copies the contents from source to destination.
func (c *Client) Copy(ctx context.Context, src, dest string, opts CopyOptions, handler CopyJobHandler) (CopyResult, error) {
	// Input
	if src == "" || dest == "" {
		return CopyResult{}, fmt.Errorf("source and destination must be specified for copy")
	}

	// AzCopy CLI sets this globally before calling Sync.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	copyHandler := common.GetLifecycleMgr()
	if copyHandler == nil {
		copyHandler = common.NewJobUIHooks()
		common.SetUIHooks(copyHandler)
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

	var t *transferExecutor
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	t, err := newCopyTransferExecutor(ctx, jobID, src, dest, opts, handler, c.GetUserOAuthTokenManagerInstance())
	if err != nil {
		return CopyResult{}, err
	}

	mgr := NewJobLifecycleManager(copyHandler)
	common.GetLifecycleMgr().Info("Scanning...")

	return CopyResult{}, nil
}
