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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// CopyOptions contains the optional parameters for the Copy operation.
type CopyOptions struct {
	Handler                   CopyHandler
	JobID                     common.JobID
	CredentialManager         cred.Manager
	RetainJobState            bool
	SourceCredentialName      string
	DestinationCredentialName string
	SrcCredName               string
	DstCredName               string
	S3CredentialProvider      credentials.Provider

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
	compatibilityListOfFiles         chan string
	compatibilityListOfVersions      chan string
	compatibilityStripTopDir         bool
	operationContext                 context.Context
	onSourceDirectory                func(bool)
	onCredentials                    func(cred.CredentialInfo, cred.CredentialInfo)
}

type CopyHandler interface {
	OnStart(ctx JobContext)
	OnTransferProgress(progress CopyProgress)
	OnComplete(result CopyResult)
}

type CopyProgress struct {
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

// SetCookedOptions preserves already prepared command inputs without recooking channels or paths.
// This is intended for command adapters, not new library consumers.
func (c *CopyOptions) SetCookedOptions(jobID common.JobID, files, versions chan string, stripTopDir bool, sourceDirectoryCallback ...func(bool)) {
	if jobID != (common.JobID{}) {
		c.JobID = jobID
	}
	c.compatibilityListOfFiles = files
	c.compatibilityListOfVersions = versions
	c.compatibilityStripTopDir = stripTopDir
	if len(sourceDirectoryCallback) > 0 {
		c.onSourceDirectory = sourceDirectoryCallback[0]
	}
}

// SetCookedCredentialCallback reports resolved credentials to an existing command adapter.
func (c *CopyOptions) SetCookedCredentialCallback(callback func(cred.CredentialInfo, cred.CredentialInfo)) {
	c.onCredentials = callback
}

// Copy copies the contents from source to destination.
func (c *Client) Copy(ctx context.Context, src, dest string, opts CopyOptions) (CopyResult, error) {
	if ctx == nil {
		return CopyResult{}, fmt.Errorf("a context is required for copy")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts.operationContext = ctx
	// Input
	if src == "" || dest == "" {
		return CopyResult{}, fmt.Errorf("source and destination must be specified for copy")
	}

	// AzCopy CLI sets this globally before calling Sync.
	// If in library mode, this will not be set and we will use the user-provided handler.
	// Note: It is not ideal that this is a global, but keeping it this way for now to avoid a larger refactor than this already is.
	copyHandler := common.GetLifecycleMgr()
	if copyHandler == nil {
		return CopyResult{}, fmt.Errorf("a lifecycle manager must be configured before copy")
	}
	jobID := common.NewJobID()
	if opts.JobID != (common.JobID{}) {
		jobID = opts.JobID
	}
	c.CurrentJobID = jobID
	timeAtPrestart := time.Now()
	jobLogger := common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger = jobLogger
	jobLogger.OpenLog()
	keepJobLoggerOpen := false
	defer func() {
		if !keepJobLoggerOpen {
			jobLogger.CloseLog()
		}
	}()

	// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
	// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
	// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
	// or after this job
	adjustedTime := timeAtPrestart.Add(-5 * time.Second)
	startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
		common.IncludeBeforeFlagName, traverser.IncludeBeforeDateFilter{}.FormatAsUTC(adjustedTime),
		common.IncludeAfterFlagName, traverser.IncludeAfterDateFilter{}.FormatAsUTC(adjustedTime))
	common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)

	if jobsAdmin.JobsAdmin == nil {
		return CopyResult{}, fmt.Errorf("jobs administrator must be initialized before copy")
	}
	traverser.EnumerationParallelism, traverser.EnumerationParallelStatFiles = jobsAdmin.JobsAdmin.GetConcurrencySettings()

	// set up the front end scanning logger
	scanningLogger := common.NewJobLogger(jobID, c.GetLogLevel(), common.LogPathFolder, "-scanning")
	common.AzcopyScanningLogger = scanningLogger
	scanningLogger.OpenLog()
	defer scanningLogger.CloseLog()

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	var t *transferExecutor
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	if opts.S3CredentialProvider != nil {
		ctx = context.WithValue(ctx, "customS3Creds", opts.S3CredentialProvider)
	}
	manager := opts.CredentialManager
	if manager == nil {
		manager = c.GetCredentialManager()
	}
	t, err := newCopyTransferExecutor(ctx, jobID, src, dest, opts, manager, jobLogger)
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
			(t.opts.fromTo.From().IsFile() || t.opts.fromTo.To().IsFile()) &&
			enum.EEnvironmentVariable.ConcurrencyValue().Get() == "" {
			jobsAdmin.JobsAdmin.SetConcurrencySettingsToAuto()
		}

		mgr := NewJobLifecycleManager(copyHandler, jobLogger)
		cleanupAllowed := false
		defer func() {
			if keepJobLoggerOpen {
				return
			}
			mgr.Stop()
			if cleanupAllowed && !opts.RetainJobState && !t.opts.dryrun && t.tpt.firstPartOrdered() {
				jobsAdmin.JobsAdmin.JobMgrCleanUp(jobID)
			}
		}()

		cancelAndDrain := func() error {
			if err := t.cancelAndDrain(cancel, jobID, mgr.CancelAndDrain); err != nil {
				keepJobLoggerOpen = true
				return fmt.Errorf("copy cancellation did not drain; job resources and logger retained: %w", err)
			}
			cleanupAllowed = true
			return nil
		}
		failCopy := func(operationErr error) (CopyResult, error) {
			if drainErr := cancelAndDrain(); drainErr != nil {
				return CopyResult{}, errors.Join(operationErr, drainErr)
			}
			return CopyResult{}, operationErr
		}

		enumerator, err := t.initCopyEnumerator(ctx, c.GetLogLevel(), mgr)
		if err != nil {
			return failCopy(err)
		}
		if !t.opts.dryrun {
			common.GetLifecycleMgr().Info("Scanning...")
			mgr.InitiateProgressReporting(ctx, t.tpt)
		}
		err = enumerator.Enumerate()
		if err != nil {
			return failCopy(err)
		}
		// if we are in dryrun mode, we don't want to actually run the job, so return here
		if t.opts.dryrun {
			return CopyResult{}, nil
		}

		err = mgr.Wait()
		if err != nil {
			return failCopy(err)
		}
		if !t.tpt.firstPartOrdered() {
			return CopyResult{}, NothingScheduledError
		}

		// Get final job summary
		finalSummary := jobsAdmin.GetJobSummary(t.tpt.jobID, !buildmode.IsMover)
		if ctx.Err() != nil || finalSummary.JobStatus == common.EJobStatus.Cancelled() || finalSummary.JobStatus == common.EJobStatus.Cancelling() {
			if err := cancelAndDrain(); err != nil {
				return CopyResult{}, err
			}
			finalSummary = jobsAdmin.GetJobSummary(t.tpt.jobID, !buildmode.IsMover)
		}
		cleanupAllowed = true
		finalSummary.SkippedSymlinkCount = t.tpt.getSkippedSymlinkCount()
		finalSummary.SkippedSpecialFileCount = t.tpt.getSkippedSpecialFileCount()
		finalSummary.SkippedHardlinkCount = t.tpt.getSkippedHardlinkCount()
		finalSummary.SkippedArchiveFileCount = t.tpt.getSkippedArchiveFileCount()

		result := CopyResult{
			ListJobSummaryResponse: finalSummary,
			ElapsedTime:            t.tpt.GetElapsedTime(),
		}

		if jobManager, exists := jobsAdmin.JobsAdmin.JobMgr(jobID); exists {
			jobManager.Log(common.LogInfo, GetCopyResult(result, true))
		} else {
			jobLogger.Log(common.LogInfo, GetCopyResult(result, true))
		}

		if opts.Handler != nil {
			opts.Handler.OnComplete(result)
		}

		return result, nil
	}
}

type transferExecutor struct {
	opts      *CookedTransferOptions
	trp       *remoteProvider
	tpt       *transferProgressTracker
	processor *CopyTransferProcessor
}

func (t *transferExecutor) cancelAndDrain(cancel context.CancelFunc, jobID common.JobID, drain func(context.Context, common.JobID) error) error {
	cancel()
	cleanupCtx, stopCleanup := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCleanup()
	if !t.opts.dryrun {
		if err := jobsAdmin.RequestJobCancellation(jobID); err != nil {
			return err
		}
	}
	if t.processor != nil {
		if err := t.processor.AbortAndWait(cleanupCtx); err != nil {
			return err
		}
	}
	if t.opts.dryrun {
		return nil
	}
	return drain(cleanupCtx, jobID)
}

func newCopyTransferExecutor(ctx context.Context, jobID common.JobID, src, dst string, opts CopyOptions, manager cred.Manager, loggers ...common.ILogger) (t *transferExecutor, err error) {
	sourceName, destinationName := opts.SourceCredentialName, opts.DestinationCredentialName
	if sourceName == "" {
		sourceName = opts.SrcCredName
	} else if opts.SrcCredName != "" && opts.SrcCredName != sourceName {
		return nil, fmt.Errorf("conflicting source credential names")
	}
	if destinationName == "" {
		destinationName = opts.DstCredName
	} else if opts.DstCredName != "" && opts.DstCredName != destinationName {
		return nil, fmt.Errorf("conflicting destination credential names")
	}
	cookedOpts, err := newCookedCopyOptions(src, dst, opts)
	if err != nil {
		return nil, err
	}

	copyRemote, err := newCopyRemoteProvider(ctx, manager, cookedOpts.source, cookedOpts.destination,
		cookedOpts.fromTo, cookedOpts.cpkOptions, cookedOpts.trailingDot, sourceName, destinationName)
	if err != nil {
		return nil, err
	}
	if opts.onCredentials != nil {
		opts.onCredentials(copyRemote.srcCredInfo, copyRemote.dstCredInfo)
	}

	progressTracker := newTransferProgressTracker(jobID, opts.Handler, cookedOpts.fromTo, loggers...)

	return &transferExecutor{opts: cookedOpts, trp: copyRemote, tpt: progressTracker}, nil
}
