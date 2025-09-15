package azcopy

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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
}

/* AzCopy internal use only. Exposing these as setters to add a hurdle to their use. */

func (s *SyncOptions) SetInternalOptions(dryrun, deleteDestinationFileIfNecessary bool) {
	s.dryrun = dryrun
	s.deleteDestinationFileIfNecessary = deleteDestinationFileIfNecessary
}

// TODO : (gapra) These will be removed as we refactor more of the sync code.
func (s *SyncOptions) GetDryrun() bool {
	return s.dryrun
}

func (s *SyncOptions) GetDeleteDestinationFileIfNecessary() bool {
	return s.deleteDestinationFileIfNecessary
}

// Sync
// 1. Phase 1 will implement arg processing and validation only.
// 2. Phase 2 will implement enumerator initialization
// 3. Phase 3 will implement the sync progress tracking
func (c *Client) Sync(ctx context.Context, src, dest string, opts SyncOptions) (ret *Syncer, err error) {
	// Input
	if src == "" || dest == "" {
		return nil, fmt.Errorf("source and destination must be specified for sync")
	}

	// AzCopy CLI sets this globally before calling Sync.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	syncHandler := common.GetLifecycleMgr()
	if syncHandler == nil {
		syncHandler = common.NewJobUIHooks()
		common.SetUIHooks(syncHandler)
	}

	c.CurrentJobID = common.NewJobID()
	timeAtPrestart := time.Now()
	common.AzcopyCurrentJobLogger = common.NewJobLogger(c.CurrentJobID, c.GetLogLevel(), common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()
	//glcm.RegisterCloseFunc(func() {
	//	if common.AzcopyCurrentJobLogger != nil {
	//		common.AzcopyCurrentJobLogger.CloseLog()
	//	}
	//})

	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	ret, err = newSyncer(src, dest, opts)

	return
}
