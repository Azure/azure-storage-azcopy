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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

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
	ExcludeBlobTypes            []blob.BlobType
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
	PosixPropertiesStyle        common.PosixPropertiesStyle
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

type CopyJobHandler interface {
	OnStart(ctx JobContext)
	OnTransferProgress(progress CopyJobProgress)
}

type CopyJobProgress struct {
	common.ListJobSummaryResponse
	Throughput  float64
	ElapsedTime time.Duration
}

type CopyResult struct {
	common.ListJobSummaryResponse
	ElapsedTime time.Duration
}

// SetInternalOptions is used to set options that are not meant to be exposed to the user through the public API.
// Note: This function is intended for internal use only and should not be used in user applications.
func (c *CopyOptions) SetInternalOptions(listOfFiles string, s2sGetPropertiesInBackend *bool, dryrun bool, dryrunJobPartOrderHandler func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse, deleteDestinationFileIfNecessary bool, cmd string) {
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

	var t *transferExecutor
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	t, err := newCopyTransferExecutor(ctx, jobID, src, dest, opts, handler, c.GetUserOAuthTokenManagerInstance())
	if err != nil {
		return CopyResult{}, err
	}

	// handle from/to pipe
	if t.opts.fromTo.IsRedirection() {
		err = t.redirectionTransfer(ctx)
		return CopyResult{}, err
	} else {

		// Make AUTO default for Azure Files since Azure Files throttles too easily unless user specified concurrency value
		if jobsAdmin.JobsAdmin != nil &&
			(t.opts.fromTo.From().IsFile() || (t.opts.fromTo.To().IsFile() &&
				common.GetEnvironmentVariable(common.EEnvironmentVariable.ConcurrencyValue()) == "")) {
			jobsAdmin.JobsAdmin.SetConcurrencySettingsToAuto()
		}

		mgr := NewJobLifecycleManager(copyHandler)

		enumerator, err := t.initCopyEnumerator(ctx, c.GetLogLevel(), mgr)
		if err != nil {
			return CopyResult{}, err
		}
		if !t.opts.dryrun {
			common.GetLifecycleMgr().Info("Scanning...")
			mgr.InitiateProgressReporting(ctx, t.tpt)
		}
		err = enumerator.Enumerate()

		if err != nil {
			return CopyResult{}, err
		}
		// if we are in dryrun mode, we don't want to actually run the job, so return here
		if t.opts.dryrun {
			return CopyResult{}, nil
		}

		err = mgr.Wait()
		if err != nil {
			return CopyResult{}, err
		}

		// Get final job summary
		finalSummary := jobsAdmin.GetJobSummary(t.tpt.jobID)
		finalSummary.SkippedSymlinkCount = t.tpt.getSkippedSymlinkCount()
		finalSummary.SkippedSpecialFileCount = t.tpt.getSkippedSpecialFileCount()
		finalSummary.SkippedHardlinkCount = t.tpt.getSkippedHardlinkCount()
		return CopyResult{
			ListJobSummaryResponse: finalSummary,
			ElapsedTime:            t.tpt.GetElapsedTime(),
		}, nil
	}
}

type transferExecutor struct {
	opts *CookedTransferOptions
	trp  *remoteProvider
	tpt  *transferProgressTracker
}

func newCopyTransferExecutor(ctx context.Context, jobID common.JobID, src, dst string, opts CopyOptions, handler CopyJobHandler, uotm *common.UserOAuthTokenManager) (t *transferExecutor, err error) {
	cookedOpts, err := newCookedCopyOptions(src, dst, opts)
	if err != nil {
		return nil, err
	}

	copyRemote, err := newCopyRemoteProvider(ctx, uotm, cookedOpts.source, cookedOpts.destination,
		cookedOpts.fromTo, cookedOpts.cpkOptions, cookedOpts.trailingDot)
	if err != nil {
		return nil, err
	}

	progressTracker := newTransferProgressTracker(jobID, handler, cookedOpts.fromTo)

	return &transferExecutor{opts: cookedOpts, trp: copyRemote, tpt: progressTracker}, nil
}
