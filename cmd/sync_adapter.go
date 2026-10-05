package cmd

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type CustomSyncHandlerFunc func(*cookedSyncCmdArgs, *syncEnumerator, context.Context) error

var UseSyncOrchestrator = traverser.UseSyncOrchestrator
var CustomSyncHandler CustomSyncHandlerFunc = syncOrchestratorHandler

func GetCustomSyncHandlerInfo() string {
	return traverser.GetCustomSyncHandlerInfo()
}

func syncOrchestratorHandler(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error {
	if cca.preparedSync == nil || cca.preparedSync.Enumerator() != enumerator {
		return fmt.Errorf("sync enumeration must be prepared by InitEnumerator")
	}
	ctx, cancel := WithJobCancellation(ctx)
	cca.orchestratorCancel = cancel
	defer cancel()
	err := cca.preparedSync.Enumerate(ctx)
	cca.refreshPreparedStats()
	return err
}

func (cca *cookedSyncCmdArgs) refreshPreparedStats() {
	if cca.preparedSync == nil {
		return
	}
	cca.applyPreparedState(cca.preparedSync.State())
}

func (cca *cookedSyncCmdArgs) applyPreparedState(state azcopy.PreparedSyncState) {
	(cliSyncHandler{cooked: cca}).OnEnumerationStats(state.SyncEnumerationStats)
	var firstPartOrdered, scanningComplete uint32
	if state.FirstPartOrdered {
		firstPartOrdered = 1
	}
	if state.ScanningComplete {
		scanningComplete = 1
	}
	atomic.StoreUint32(&cca.atomicFirstPartOrdered, firstPartOrdered)
	atomic.StoreUint32(&cca.atomicScanningStatus, scanningComplete)
	atomic.StoreUint32(&cca.atomicDeletionCount, state.DeletionCount)
	atomic.StoreUint32(&cca.atomicSkippedSymlinkCount, state.SkippedSymlinkCount)
	atomic.StoreUint32(&cca.atomicSkippedSpecialFileCount, state.SkippedSpecialFileCount)
	atomic.StoreUint32(&cca.atomicSkippedHardlinkCount, state.SkippedHardlinkCount)
}

func (raw rawSyncCmdArgs) toOptions() (azcopy.SyncOptions, error) {
	cooked, err := raw.cook()
	if err != nil {
		return azcopy.SyncOptions{}, err
	}
	cooked.commandString = ConstructCommandStringFromArgs()
	return cooked.librarySyncOptions(NewSyncDefaultEnumeratorOptions()), nil
}

func (cca *cookedSyncCmdArgs) syncResourceStrings() (string, string, error) {
	stringify := func(resource common.ResourceString, location common.Location) (string, error) {
		if location.IsLocal() {
			if resource.SAS != "" || resource.ExtraQuery != "" {
				return "", fmt.Errorf("local sync resource cannot contain SAS or query parameters")
			}
			return resource.Value, nil
		}
		return resource.String()
	}
	source, err := stringify(cca.source, cca.fromTo.From())
	if err != nil {
		return "", "", fmt.Errorf("invalid sync source: %w", err)
	}
	destination, err := stringify(cca.destination, cca.fromTo.To())
	if err != nil {
		return "", "", fmt.Errorf("invalid sync destination: %w", err)
	}
	if cca.stripTopDir && cca.fromTo.From().IsLocal() && !strings.HasSuffix(source, "/*") {
		source = strings.TrimRight(source, "/\\") + "/*"
	}
	return source, destination, nil
}

func (cca *cookedSyncCmdArgs) librarySyncOptions(enumeratorOptions *SyncEnumeratorOptions) azcopy.SyncOptions {
	enumeratorOptions = enumeratorOptions.forJob(cca.fromTo)
	preserveOwner := cca.preservePermissions == common.EPreservePermissionsOption.OwnershipAndACLs()
	opts := azcopy.SyncOptions{
		Handler:                    cliSyncHandler{cooked: cca},
		JobID:                      cca.jobID,
		CredentialManager:          GetCredentialManager(),
		SrcCredName:                cca.SrcCredName,
		DstCredName:                cca.DstCredName,
		UseSyncOrchestrator:        UseSyncOrchestrator,
		UseStreamingMergeJoin:      cca.useStreamingMergeJoin,
		OrchestratorOptions:        enumeratorOptions.SyncOrchOptions,
		ErrorChannel:               enumeratorOptions.ErrorChannel,
		AllowLocalSymlinkFollowing: cca.allowLocalSymlinkFollowing,
		BlobType:                   cca.blobType,
		BlockBlobTier:              cca.blockBlobTier,
		BackupMode:                 cca.backupMode,
		PreserveOwner:              &preserveOwner,
		RetainJobState:             buildmode.IsMover,
		FromTo:                     cca.fromTo,
		Recursive:                  &cca.recursive,
		IncludeDirectoryStubs:      cca.includeDirectoryStubs,
		PreserveInfo:               &cca.preserveInfo,
		PreservePosixProperties:    cca.preservePOSIXProperties,
		PosixPropertiesStyle:       cca.posixPropertiesStyle,
		ForceIfReadOnly:            cca.forceIfReadOnly,
		BlockSizeMB:                cca.blockSizeMB,
		PutBlobSizeMB:              cca.putBlobSizeMB,
		IncludePatterns:            cca.includePatterns,
		ExcludePatterns:            cca.excludePatterns,
		ExcludePaths:               cca.excludePaths,
		IncludeAttributes:          cca.includeFileAttributes,
		ExcludeAttributes:          cca.excludeFileAttributes,
		IncludeRegex:               cca.includeRegex,
		ExcludeRegex:               cca.excludeRegex,
		DeleteDestination:          cca.deleteDestination,
		PutHash:                    cca.putMd5,
		CheckHash:                  cca.md5ValidationOption,
		S2SPreserveAccessTier:      &cca.preserveAccessTier,
		S2SPreserveBlobTags:        cca.s2sPreserveBlobTags,
		CpkByName:                  cca.cpkByName,
		CpkByValue:                 cca.cpkByValue,
		MirrorMode:                 cca.mirrorMode,
		TrailingDot:                cca.trailingDot,
		IncludeRoot:                cca.includeRoot,
		CompareHash:                cca.compareHash,
		HashMetaDir:                cca.hashMetaDir,
		LocalHashStorageMode:       &cca.localHashStorageMode,
		Symlinks:                   cca.symlinkHandling,
		PreservePermissions:        cca.preservePermissions.IsTruthy(),
		Hardlinks:                  cca.hardlinks,
	}
	opts.SetInternalOptions(cca.dryrunMode, cca.deleteDestinationFileIfNecessary,
		cca.commandString, dryrunNewCopyJobPartOrder, dryrunDelete)
	return opts
}

type cliSyncHandler struct {
	cooked *cookedSyncCmdArgs
}

func (h cliSyncHandler) OnStart(job azcopy.JobContext) {
	if h.cooked != nil {
		h.cooked.jobID = job.JobID
		h.cooked.jobStartTime = time.Now()
		h.cooked.intervalStartTime = h.cooked.jobStartTime
	}
	StartSystemStatsMonitorForJobID(job.JobID)
	glcm.Init(GetStandardInitOutputBuilder(job.JobID.String(), job.LogPath, false, ""))
}

func (h cliSyncHandler) OnEnumerationStats(stats azcopy.SyncEnumerationStats) {
	if h.cooked == nil {
		return
	}
	c := h.cooked
	atomic.StoreUint64(&c.atomicSourceFilesScanned, stats.SourceFilesScanned)
	atomic.StoreUint64(&c.atomicDestinationFilesScanned, stats.DestinationFilesScanned)
	atomic.StoreUint64(&c.atomicSourceFoldersScanned, stats.SourceFoldersScanned)
	atomic.StoreUint64(&c.atomicDestinationFoldersScanned, stats.DestinationFoldersScanned)
	atomic.StoreUint64(&c.atomicSourceFilesTransferNotRequired, stats.SourceFilesTransferNotRequired)
	atomic.StoreUint64(&c.atomicSourceFoldersTransferNotRequired, stats.SourceFoldersTransferNotRequired)
	atomic.StoreUint64(&c.atomicSkippedArchiveFileCount, stats.SkippedArchiveFileCount)
	c.atomicSourceFileEnumerationFailed.Store(stats.SourceFileEnumerationFailed)
	c.atomicSourceFolderEnumerationFailed.Store(stats.SourceFolderEnumerationFailed)
	c.atomicDestinationFolderEnumerationFailed.Store(stats.DestinationFolderEnumerationFailed)
	c.atomicDestinationFolderEnumerationSkipped.Store(stats.DestinationFolderEnumerationSkipped)
	var firstPartOrdered, scanningComplete uint32
	if stats.FirstPartOrdered {
		firstPartOrdered = 1
	}
	if stats.ScanningComplete {
		scanningComplete = 1
	}
	atomic.StoreUint32(&c.atomicFirstPartOrdered, firstPartOrdered)
	atomic.StoreUint32(&c.atomicScanningStatus, scanningComplete)
	atomic.StoreUint32(&c.atomicDeletionCount, stats.DeletionCount)
	atomic.StoreUint32(&c.atomicSkippedSymlinkCount, stats.SkippedSymlinkCount)
	atomic.StoreUint32(&c.atomicSkippedSpecialFileCount, stats.SkippedSpecialFileCount)
	atomic.StoreUint32(&c.atomicSkippedHardlinkCount, stats.SkippedHardlinkCount)
}

func (h cliSyncHandler) applyMoverStats(stats azcopy.MoverSyncStats) {
	h.OnEnumerationStats(stats)
}

func (h cliSyncHandler) OnScanProgress(progress azcopy.SyncScanProgress) {
	if h.cooked == nil {
		return
	}
	h.applyMoverStats(progress.MoverSyncStats)
	atomic.StoreUint64(&h.cooked.atomicSourceFilesScanned, progress.SourceFilesScanned)
	atomic.StoreUint64(&h.cooked.atomicDestinationFilesScanned, progress.DestinationFilesScanned)
	var throughput float64
	if progress.Throughput != nil {
		throughput = *progress.Throughput
	}
	h.cooked.reportScanningProgress(glcm, throughput)
}

func (h cliSyncHandler) applySummary(summary common.ListJobSummaryResponse, deletions uint32) {
	if h.cooked == nil {
		return
	}
	c := h.cooked
	c.jobID = summary.JobID
	c.setScanningComplete()
	c.isEnumerationComplete = true
	if summary.TotalTransfers > 0 {
		c.setFirstPartOrdered()
	}
	atomic.StoreUint32(&c.atomicDeletionCount, deletions)
	atomic.StoreUint32(&c.atomicSkippedSymlinkCount, summary.SkippedSymlinkCount)
	atomic.StoreUint32(&c.atomicSkippedSpecialFileCount, summary.SkippedSpecialFileCount)
	atomic.StoreUint32(&c.atomicSkippedHardlinkCount, summary.SkippedHardlinkCount)
	atomic.StoreUint64(&c.atomicSkippedArchiveFileCount, summary.SkippedArchiveFileCount)
}

func (h cliSyncHandler) OnTransferProgress(progress azcopy.SyncProgress) {
	h.applySummary(progress.ListJobSummaryResponse, progress.DeleteTransfersCompleted)
	h.applyMoverStats(progress.MoverSyncStats)
	glcm.Progress(func(format common.OutputFormat) string {
		if format == EOutputFormat.Json() && h.cooked != nil {
			return h.cooked.getJsonOfSyncJobSummary(progress.ListJobSummaryResponse)
		}
		return azcopy.GetSyncProgress(progress)
	})
}

func (h cliSyncHandler) OnComplete(result azcopy.SyncResult) {
	h.applySummary(result.ListJobSummaryResponse, result.DeleteTransfersCompleted)
	h.applyMoverStats(result.MoverSyncStats)
	if h.cooked == nil {
		return
	}
	atomic.StoreUint64(&h.cooked.atomicSourceFilesScanned, result.SourceFilesScanned)
	atomic.StoreUint64(&h.cooked.atomicDestinationFilesScanned, result.DestinationFilesScanned)
	code := EExitCode.Success()
	if result.TransfersFailed > 0 || result.JobStatus == common.EJobStatus.Cancelled() ||
		(!buildmode.IsMover && result.JobStatus == common.EJobStatus.Cancelling()) {
		code = EExitCode.Error()
	}
	glcm.Exit(func(format common.OutputFormat) string {
		if format == EOutputFormat.Json() {
			return h.cooked.getJsonOfSyncJobSummary(result.ListJobSummaryResponse)
		}
		screenStats, _ := formatExtraStats(h.cooked.fromTo, result.AverageIOPS, result.AverageE2EMilliseconds,
			result.NetworkErrorPercentage, result.ServerBusyPercentage)
		if buildmode.IsMover {
			return h.cooked.GetElaborateOutputMessage(result.ListJobSummaryResponse, screenStats, result.ElapsedTime)
		}
		return h.cooked.GetDefaultOutputMessage(result.ListJobSummaryResponse, screenStats, result.ElapsedTime)
	}, code)
}
