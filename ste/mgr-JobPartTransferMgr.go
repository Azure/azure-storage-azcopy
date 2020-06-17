package ste

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

type IJobPartTransferMgr interface {
	FromTo() common.FromTo
	Info() TransferInfo
	ResourceDstData(dataFileToXfer []byte) (headers common.ResourceHTTPHeaders, metadata common.Metadata)
	LastModifiedTime() time.Time
	PreserveLastModifiedTime() (time.Time, bool)
	ShouldPutMd5() bool
	MD5ValidationOption() common.HashValidationOption
	BlobTypeOverride() common.BlobType
	BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier)
	JobHasLowFileCount() bool
	//ScheduleChunk(chunkFunc chunkFunc)
	Context() context.Context
	SlicePool() common.ByteSlicePooler
	CacheLimiter() common.CacheLimiter
	WaitUntilLockDestination(ctx context.Context) error
	EnsureDestinationUnlocked()
	HoldsDestinationLock() bool
	StartJobXfer()
	GetOverwriteOption() common.OverwriteOption
	GetForceIfReadOnly() bool
	ShouldDecompress() bool
	GetSourceCompressionType() (common.CompressionType, error)
	ReportChunkDone(id common.ChunkID) (lastChunk bool, chunksDone uint32)
	TransferStatusIgnoringCancellation() common.TransferStatus
	SetStatus(status common.TransferStatus)
	SetErrorCode(errorCode int32)
	SetNumberOfChunks(numChunks uint32)
	SetActionAfterLastChunk(f func())
	ReportTransferDone() uint32
	RescheduleTransfer()
	ScheduleChunks(chunkFunc chunkFunc)
	SetDestinationIsModified()
	Cancel()
	WasCanceled() bool
	IsLive() bool
	IsDeadBeforeStart() bool
	IsDeadInflight() bool
	// TODO: added for debugging purpose. remove later
	OccupyAConnection()
	// TODO: added for debugging purpose. remove later
	ReleaseAConnection()
	SourceProviderPipeline() pipeline.Pipeline
	FailActiveUpload(where string, err error)
	FailActiveDownload(where string, err error)
	FailActiveUploadWithStatus(where string, err error, failureStatus common.TransferStatus)
	FailActiveDownloadWithStatus(where string, err error, failureStatus common.TransferStatus)
	FailActiveS2SCopy(where string, err error)
	FailActiveS2SCopyWithStatus(where string, err error, failureStatus common.TransferStatus)
	// TODO: Cleanup FailActiveUpload/FailActiveUploadWithStatus & FailActiveS2SCopy/FailActiveS2SCopyWithStatus
	FailActiveSend(where string, err error)
	FailActiveSendWithStatus(where string, err error, failureStatus common.TransferStatus)
	LogUploadError(source, destination, errorMsg string, status int)
	LogDownloadError(source, destination, errorMsg string, status int)
	LogS2SCopyError(source, destination, errorMsg string, status int)
	LogSendError(source, destination, errorMsg string, status int)
	LogError(resource, context string, err error)
	LogTransferInfo(level pipeline.LogLevel, source, destination, msg string)
	LogTransferStart(source, destination, description string)
	LogChunkStatus(id common.ChunkID, reason common.WaitReason)
	ChunkStatusLogger() common.ChunkStatusLogger
	LogAtLevelForCurrentTransfer(level pipeline.LogLevel, msg string)
	GetOverwritePrompter() *overwritePrompter
	GetFolderCreationTracker() common.FolderCreationTracker
	common.ILogger
	DeleteSnapshotsOption() common.DeleteSnapshotsOption
	SecurityInfoPersistenceManager() *securityInfoPersistenceManager
	FolderDeletionManager() common.FolderDeletionManager
	GetDestinationRoot() string
}

type TransferInfo struct {
	BlockSize              uint32
	Source                 string
	SourceSize             int64
	Destination            string
	EntityType             common.EntityType
	PreserveSMBPermissions common.PreservePermissionsOption
	PreserveSMBInfo        bool

	// Transfer info for S2S copy
	SrcProperties
	S2SGetPropertiesInBackend      bool
	S2SSourceChangeValidation      bool
	DestLengthValidation           bool
	S2SInvalidMetadataHandleOption common.InvalidMetadataHandleOption

	// Blob
	SrcBlobType    azblob.BlobType       // used for both S2S and for downloads to local from blob
	S2SSrcBlobTier azblob.AccessTierType // AccessTierType (string) is used to accommodate service-side support matrix change.

	// NumChunks is the number of chunks in which transfer will be split into while uploading the transfer.
	// NumChunks is not used in case of AppendBlob transfer.
	NumChunks uint16
}

func (i TransferInfo) IsFolderPropertiesTransfer() bool {
	return i.EntityType == common.EEntityType.Folder()
}

// We don't preserve LMTs on folders.
// The main reason is that preserving folder LMTs at download time is very difficult, because it requires us to keep track of when the
// last file has been saved in each folder OR just do all the folders at the very end.
// This is because if we modify the contents of a folder after setting its LMT, then the LMT will change because Windows and Linux
//(and presumably MacOS) automatically update the folder LMT when the contents are changed.
// The possible solutions to this problem may become difficult on very large jobs (e.g. 10s or hundreds of millions of files,
// with millions of directories).
// The secondary reason is that folder LMT's don't actually tell the user anything particularly useful. Specifically,
// they do NOT tell you when the folder contents (recursively) were last updated: in Azure Files they are never updated
// when folder contents change; and in NTFS they are only updated when immediate children are changed (not grandchildren).
func (i TransferInfo) ShouldTransferLastWriteTime() bool {
	return !i.IsFolderPropertiesTransfer()
}

// entityTypeLogIndicator returns a string that can be used in logging to distinguish folder property transfers from "normal" transfers.
// It's purpose is to avoid any confusion from folks seeing a folder name in the log and thinking, "But I don't have a file with that name".
// It also makes it clear that the log record relates to the folder's properties, not its contained files.
func (i TransferInfo) entityTypeLogIndicator() string {
	if i.IsFolderPropertiesTransfer() {
		return "(folder properties) "
	} else {
		return ""
	}
}

type SrcProperties struct {
	SrcHTTPHeaders common.ResourceHTTPHeaders // User for S2S copy, where per transfer's src properties need be set in destination.
	SrcMetadata    common.Metadata
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type chunkFunc func(int)

// jobPartTransferMgr represents the runtime information for a Job Part's transfer
type jobPartTransferMgr struct {
	// how many bytes have been successfully transferred
	// (hard to infer from atomicChunksDone because that counts both successes and failures)
	atomicSuccessfulBytes int64

	// NumberOfChunksDone represents the number of chunks of a transfer
	// which are either completed or failed.
	// NumberOfChunksDone determines the final cancellation or completion of a transfer
	atomicChunksDone uint32

	// used defensively to protect against accidental double counting
	atomicCompletionIndicator uint32

	// used to show whether we have started doing things that may affect the destination
	atomicDestModifiedIndicator uint32

	// used to show whether THIS jptm holds the destination lock
	atomicDestLockHeldIndicator uint32

	jobPartMgr          IJobPartMgr // Refers to the "owning" Job Part
	jobPartPlanTransfer *JobPartPlanTransfer
	transferIndex       uint32

	// the context of this transfer; allows any failing chunk to cancel the whole transfer
	ctx context.Context

	// Call cancel to cancel the transfer
	cancel context.CancelFunc

	numChunks uint32

	actionAfterLastChunk func()

	/*
		@Parteek removed 3/23 morning, as jeff ad equivalent
		// transfer chunks are put into this channel and execution engine takes chunk out of this channel.
		chunkChannel chan<- ChunkMsg*/
}

func (jptm *jobPartTransferMgr) GetOverwritePrompter() *overwritePrompter {
	return jptm.jobPartMgr.getOverwritePrompter()
}

func (jptm *jobPartTransferMgr) GetFolderCreationTracker() common.FolderCreationTracker {
	return jptm.jobPartMgr.getFolderCreationTracker()
}

func (jptm *jobPartTransferMgr) FromTo() common.FromTo {
	return jptm.jobPartMgr.Plan().FromTo
}

func (jptm *jobPartTransferMgr) StartJobXfer() {
	jptm.jobPartMgr.StartJobXfer(jptm)
}

func (jptm *jobPartTransferMgr) GetOverwriteOption() common.OverwriteOption {
	return jptm.jobPartMgr.GetOverwriteOption()
}

func (jptm *jobPartTransferMgr) GetForceIfReadOnly() bool {
	return jptm.jobPartMgr.GetForceIfReadOnly()
}

func (jptm *jobPartTransferMgr) ShouldDecompress() bool {
	if jptm.jobPartMgr.AutoDecompress() {
		ct, _ := jptm.GetSourceCompressionType()
		return ct != common.ECompressionType.None()
	}
	return false
}

func (jptm *jobPartTransferMgr) GetSourceCompressionType() (common.CompressionType, error) {
	encoding := jptm.Info().SrcHTTPHeaders.ContentEncoding
	return common.GetCompressionType(encoding)
}

func (jptm *jobPartTransferMgr) Info() TransferInfo {
	plan := jptm.jobPartMgr.Plan()
	src, dst, _ := plan.TransferSrcDstStrings(jptm.transferIndex)
	dstBlobData := plan.DstBlobData

	srcHTTPHeaders, srcMetadata, srcBlobType, srcBlobTier, s2sGetPropertiesInBackend, DestLengthValidation, s2sSourceChangeValidation, s2sInvalidMetadataHandleOption, entityType :=
		plan.TransferSrcPropertiesAndMetadata(jptm.transferIndex)
	srcSAS, dstSAS := jptm.jobPartMgr.SAS()
	// If the length of destination SAS is greater than 0
	// it means the destination is remote url and destination SAS
	// has been stripped from the destination before persisting it in
	// part plan file.
	// SAS needs to be appended before executing the transfer
	if len(dstSAS) > 0 {
		dUrl, e := url.Parse(dst)
		if e != nil {
			panic(e)
		}
		if len(dUrl.RawQuery) > 0 {
			dUrl.RawQuery += "&" + dstSAS
		} else {
			dUrl.RawQuery = dstSAS
		}
		dst = dUrl.String()
	}

	// If the length of source SAS is greater than 0
	// it means the source is a remote url and source SAS
	// has been stripped from the source before persisting it in
	// part plan file.
	// SAS needs to be appended before executing the transfer
	if len(srcSAS) > 0 {
		sUrl, e := url.Parse(src)
		if e != nil {
			panic(e)
		}
		if len(sUrl.RawQuery) > 0 {
			sUrl.RawQuery += "&" + srcSAS
		} else {
			sUrl.RawQuery = srcSAS
		}
		src = sUrl.String()
	}

	sourceSize := plan.Transfer(jptm.transferIndex).SourceSize
	var blockSize = dstBlobData.BlockSize
	// If the blockSize is 0, then User didn't provide any blockSize
	// We need to set the blockSize in such way that number of blocks per blob
	// does not exceeds 50000 (max number of block per blob)
	if blockSize == 0 {
		blockSize = uint32(common.DefaultBlockBlobBlockSize)
		for ; uint32(sourceSize/int64(blockSize)) > common.MaxNumberOfBlocksPerBlob; blockSize = 2 * blockSize {
		}
	}
	blockSize = common.Iffuint32(blockSize > common.MaxBlockBlobBlockSize, common.MaxBlockBlobBlockSize, blockSize)

	return TransferInfo{
		BlockSize:                      blockSize,
		Source:                         src,
		SourceSize:                     sourceSize,
		Destination:                    dst,
		EntityType:                     entityType,
		PreserveSMBPermissions:         plan.PreserveSMBPermissions,
		PreserveSMBInfo:                plan.PreserveSMBInfo,
		S2SGetPropertiesInBackend:      s2sGetPropertiesInBackend,
		S2SSourceChangeValidation:      s2sSourceChangeValidation,
		S2SInvalidMetadataHandleOption: s2sInvalidMetadataHandleOption,
		DestLengthValidation:           DestLengthValidation,
		SrcProperties: SrcProperties{
			SrcHTTPHeaders: srcHTTPHeaders,
			SrcMetadata:    srcMetadata,
		},
		SrcBlobType:    srcBlobType,
		S2SSrcBlobTier: srcBlobTier,
	}
}

func (jptm *jobPartTransferMgr) Context() context.Context {
	return jptm.ctx
}

func (jptm *jobPartTransferMgr) SlicePool() common.ByteSlicePooler {
	return jptm.jobPartMgr.SlicePool()
}

func (jptm *jobPartTransferMgr) CacheLimiter() common.CacheLimiter {
	return jptm.jobPartMgr.CacheLimiter()
}

func (jptm *jobPartTransferMgr) FileCountLimiter() common.CacheLimiter {
	return jptm.jobPartMgr.FileCountLimiter()
}

// WaitUntilLockDestination does two things. It respects any limit that may be in place on the number of
// active destination files (by blocking until we are under the max count), and it
// registers the destination as "locked" in our internal map. The reason we
// lock internally in map like this is:
// (a) it is desirable to have some kind of locking because there are  edge cases where
// we may map two source files to one destination. This can happen in two situations: 1. when we move data from a
// case sensitive file system to a case insensitive one (and two source files map to the same destination).
// And 2. in the occasions where we mutate the destination name (since when doing such mutation we can't and don't check
// whether we are also transferring another file with a name that is already equal to the result of that mutation).
// We have chosen to lock only for the duration of the writing
// to the destination because we don't wait to maintain a huge dictionary of all files in the job and
// this much locking is enough to prevent data from both sources getting MIXED TOGETHER in the one file. It's not enough
// to prevent one source file completely overwriting the other at the destination... but that's a much more tolerable
// form of "corruption" than actually ending up with data from two sources in one file - which is what we can get if
// we don't have this lock. AND
// (b) Linux file locking is not consistently implemented, so it seems cleaner not to rely on OS file locking to accomplish (a)
// (and we need (a) on Linux for case (ii) below).
//
// As at Oct 2019, cases where we mutate destination names are
// (i)  when destination is Windows or Azure Files, and source contains characters unsupported at the destination
// (ii) when downloading with --decompress and there are two files that differ only in an extension that will will strip
//      e.g. foo.txt and foo.txt.gz (if we decompress the latter, we'll strip the extension and the names will collide)
// (iii) For completeness, there's also bucket->container name resolution when copying from S3, but that is not expected to ever
//      create collisions, since it already takes steps to prevent them.
func (jptm *jobPartTransferMgr) WaitUntilLockDestination(ctx context.Context) error {
	if strings.EqualFold(jptm.Info().Destination, common.Dev_Null) {
		return nil // nothing to lock
	}

	if jptm.useFileCountLimiter() {
		err := jptm.jobPartMgr.FileCountLimiter().WaitUntilAdd(ctx, 1, func() bool { return true })
		if err != nil {
			return err
		}
	}

	err := jptm.jobPartMgr.ExclusiveDestinationMap().Add(jptm.Info().Destination)
	if err == nil {
		atomic.StoreUint32(&jptm.atomicDestLockHeldIndicator, 1) // THIS jptm owns the dest lock (not some other jptm processing an file with the same name, and thereby preventing us from doing so)
	} else {
		if jptm.useFileCountLimiter() {
			jptm.jobPartMgr.FileCountLimiter().Remove(1) // since we are about to say that acquiring the "lock" failed
		}
	}

	return err
}

func (jptm *jobPartTransferMgr) EnsureDestinationUnlocked() {
	didHaveLock := atomic.CompareAndSwapUint32(&jptm.atomicDestLockHeldIndicator, 1, 0) // set to 0, but only if it is currently 1. Return true if changed
	// only unlock if THIS jptm actually had the lock. (So that we don't make unwanted removals from fileCountLimiter)
	if didHaveLock {
		jptm.jobPartMgr.ExclusiveDestinationMap().Remove(jptm.Info().Destination)
		if jptm.useFileCountLimiter() {
			jptm.jobPartMgr.FileCountLimiter().Remove(1)
		}
	}
}

func (jptm *jobPartTransferMgr) HoldsDestinationLock() bool {
	return atomic.LoadUint32(&jptm.atomicDestLockHeldIndicator) == 1
}

func (jptm *jobPartTransferMgr) useFileCountLimiter() bool {
	ft := jptm.FromTo()    // TODO: consider changing isDownload (and co) to have struct receiver instead of pointer receiver, so don't need variable like this
	return ft.IsDownload() // count-based limits are only applied for download a present
}

func (jptm *jobPartTransferMgr) RescheduleTransfer() {
	jptm.jobPartMgr.RescheduleTransfer(jptm)
}

func (jptm *jobPartTransferMgr) ScheduleChunks(chunkFunc chunkFunc) {
	jptm.jobPartMgr.ScheduleChunks(chunkFunc)
}

func (jptm *jobPartTransferMgr) ResourceDstData(dataFileToXfer []byte) (headers common.ResourceHTTPHeaders, metadata common.Metadata) {
	return jptm.jobPartMgr.(*jobPartMgr).resourceDstData(jptm.Info().Source, dataFileToXfer)
}

// TODO refactor into something like jptm.IsLastModifiedTimeEqual() so that there is NO LastModifiedTime method and people therefore CAN'T do it wrong due to time zone
func (jptm *jobPartTransferMgr) LastModifiedTime() time.Time {
	return time.Unix(0, jptm.jobPartPlanTransfer.ModifiedTime)
}

// PreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it returns the lastModifiedTime of the source.
func (jptm *jobPartTransferMgr) PreserveLastModifiedTime() (time.Time, bool) {
	if preserveLastModifiedTime := jptm.jobPartMgr.(*jobPartMgr).localDstData().PreserveLastModifiedTime; preserveLastModifiedTime {
		lastModifiedTime := jptm.jobPartPlanTransfer.ModifiedTime
		return time.Unix(0, lastModifiedTime), true
	}
	return time.Time{}, false
}

func (jptm *jobPartTransferMgr) ShouldPutMd5() bool {
	return jptm.jobPartMgr.ShouldPutMd5()
}

func (jptm *jobPartTransferMgr) MD5ValidationOption() common.HashValidationOption {
	return jptm.jobPartMgr.(*jobPartMgr).localDstData().MD5VerificationOption
}

func (jptm *jobPartTransferMgr) DeleteSnapshotsOption() common.DeleteSnapshotsOption {
	return jptm.jobPartMgr.(*jobPartMgr).deleteSnapshotsOption()
}

func (jptm *jobPartTransferMgr) BlobTypeOverride() common.BlobType {
	return jptm.jobPartMgr.BlobTypeOverride()
}

func (jptm *jobPartTransferMgr) BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier) {
	return jptm.jobPartMgr.BlobTiers()
}

// JobHasLowFileCount returns an estimate of whether we only have a very small number of files in the overall job
// (An "estimate" because it actually only looks at the current job part)
func (jptm *jobPartTransferMgr) JobHasLowFileCount() bool {
	// TODO: review this guestimated threshold
	// Threshold is chosen because for a single large file (in Windows-based test configuration with approx 9.5 Gps disks)
	// one file gets between 2 or 5 Gbps (depending on other factors), but we really want at least 4 times that throughput.
	// So a minimal threshold would be 4.
	const lowFileCountThreshold = 4
	return jptm.jobPartMgr.Plan().NumTransfers < lowFileCountThreshold
}

func (jptm *jobPartTransferMgr) SetNumberOfChunks(numChunks uint32) {
	jptm.numChunks = numChunks
}

func (jptm *jobPartTransferMgr) SetActionAfterLastChunk(f func()) {
	jptm.actionAfterLastChunk = f
}

// Call Done when a chunk has completed its transfer; this method returns the number of chunks completed so far
func (jptm *jobPartTransferMgr) ReportChunkDone(id common.ChunkID) (lastChunk bool, chunksDone uint32) {

	// Tell the id to remember that we (the jptm) have been told about its completion
	// Will panic if we've already been told about its completion before.
	// Why? As defensive programming, since if we accidentally counted one chunk twice, we'd complete
	// before another was finish. Which would be bad
	id.SetCompletionNotificationSent()

	// track progress
	if jptm.IsLive() {
		atomic.AddInt64(&jptm.atomicSuccessfulBytes, id.Length())
		JobsAdmin.AddSuccessfulBytesInActiveFiles(id.Length())
	}

	// Do our actual processing
	chunksDone = atomic.AddUint32(&jptm.atomicChunksDone, 1)
	lastChunk = chunksDone == jptm.numChunks
	if lastChunk {
		jptm.runActionAfterLastChunk()
		JobsAdmin.AddSuccessfulBytesInActiveFiles(-atomic.LoadInt64(&jptm.atomicSuccessfulBytes)) // subtract our bytes from the active files bytes, because we are done now
	}
	return lastChunk, chunksDone
}

// If an automatic action has been specified for after the last chunk, run it now
// (Prior to introduction of this routine, individual chunkfuncs had to check the return values
// of ReportChunkDone and then implement their own versions of the necessary transfer epilogue code.
// But that led to unwanted duplication of epilogue code, in the various types of chunkfunc. This routine
// makes it easier to create DRY epilogue code.)
func (jptm *jobPartTransferMgr) runActionAfterLastChunk() {
	if jptm.actionAfterLastChunk != nil {
		jptm.actionAfterLastChunk()     // Call the final action first,
		jptm.actionAfterLastChunk = nil // make sure it can't be run again, since epilogue methods are not expected to be idempotent,
	}
}

// TransferStatusIgnoringCancellation is the raw transfer status. Generally should use
// IsFailedOrCancelled or IsLive instead of this routine because they take cancellation into
// account
func (jptm *jobPartTransferMgr) TransferStatusIgnoringCancellation() common.TransferStatus {
	return jptm.jobPartPlanTransfer.TransferStatus()
}

// TransferStatus updates the status of given transfer for given jobId and partNumber
func (jptm *jobPartTransferMgr) SetStatus(status common.TransferStatus) {
	jptm.jobPartPlanTransfer.SetTransferStatus(status, false)
}

// SetErrorCode updates the errorcode of transfer for given jobId and partNumber.
func (jptm *jobPartTransferMgr) ErrorCode() int32 {
	return jptm.jobPartPlanTransfer.ErrorCode()
}

// SetErrorCode updates the errorcode of transfer for given jobId and partNumber.
func (jptm *jobPartTransferMgr) SetErrorCode(errorCode int32) {
	// If the given errorCode is 0, then errorCode doesn't needs to be updated since default value
	// of errorCode is 0.
	if errorCode == 0 {
		return
	}
	jptm.jobPartPlanTransfer.SetErrorCode(errorCode, false)
}

// TODO: Can we kill this method?
/*func (jptm *jobPartTransferMgr) ChunksDone() uint32 {
	return atomic.LoadUint32(&jptm.atomicChunksDone)
}*/

func (jptm *jobPartTransferMgr) Cancel()           { jptm.cancel() }
func (jptm *jobPartTransferMgr) WasCanceled() bool { return jptm.ctx.Err() != nil }

// SetDestinationIsModified tells the jptm that it should consider the destination to have been modified
func (jptm *jobPartTransferMgr) SetDestinationIsModified() {
	old := atomic.SwapUint32(&jptm.atomicDestModifiedIndicator, 1)
	// TODO: one day it might be cleaner to simply transition the TransferStatus
	//   from NotStarted to Started here. However, that's potentially a non-trivial change
	//   because the default is currently (2019) "Started".  So the NotStarted state is never used.
	//   Starting to use it would require analysis and testing that we don't have time for right now.
	if old == 0 {
		jptm.LogAtLevelForCurrentTransfer(pipeline.LogDebug, "destination modified flag is set to true")
	}
}

func (jptm *jobPartTransferMgr) hasStartedWork() bool {
	return atomic.LoadUint32(&jptm.atomicDestModifiedIndicator) == 1
}

// isDead covers all non-successful outcomes. It is necessary because
// the raw status values do not reflect possible cancellation.
// Do not call directly. Use IsDeadBeforeStart or IsDeadInflight
// instead because they usually require different handling
func (jptm *jobPartTransferMgr) isDead() bool {
	return jptm.TransferStatusIgnoringCancellation() < 0 || jptm.WasCanceled()
}

// IsDeadBeforeStart is true for transfers that fail or are cancelled before any action is taken
// that may affect the destination.
func (jptm *jobPartTransferMgr) IsDeadBeforeStart() bool {
	return jptm.isDead() && !jptm.hasStartedWork()
}

// IsDeadInflight is true for transfers that fail or are cancelled after they have
// (or may have) manipulated the destination
func (jptm *jobPartTransferMgr) IsDeadInflight() bool {
	return jptm.isDead() && jptm.hasStartedWork()
}

// IsLive is the inverse of isDead.  It doesn't mean "success", just means "not failed yet"
// (e.g. something still in progress will return true from IsLive.)
func (jptm *jobPartTransferMgr) IsLive() bool {
	return !jptm.isDead()
}

func (jptm *jobPartTransferMgr) ShouldLog(level pipeline.LogLevel) bool {
	return jptm.jobPartMgr.ShouldLog(level)
}

func (jptm *jobPartTransferMgr) LogChunkStatus(id common.ChunkID, reason common.WaitReason) {
	jptm.jobPartMgr.ChunkStatusLogger().LogChunkStatus(id, reason)
}

func (jptm *jobPartTransferMgr) ChunkStatusLogger() common.ChunkStatusLogger {
	return jptm.jobPartMgr.ChunkStatusLogger()
}

// Add 1 to the active number of goroutine performing the transfer or executing the chunkFunc
// TODO: added for debugging purpose. remove later
func (jptm *jobPartTransferMgr) OccupyAConnection() {
	jptm.jobPartMgr.OccupyAConnection()
}

// Sub 1 from the active number of goroutine performing the transfer or executing the chunkFunc
// TODO: added for debugging purpose. remove later
func (jptm *jobPartTransferMgr) ReleaseAConnection() {
	jptm.jobPartMgr.ReleaseAConnection()
}

func (jptm *jobPartTransferMgr) FailActiveUpload(where string, err error) {
	jptm.failActiveTransfer(transferErrorCodeUploadFailed, where, err, common.ETransferStatus.Failed())
}

func (jptm *jobPartTransferMgr) FailActiveDownload(where string, err error) {
	jptm.failActiveTransfer(transferErrorCodeDownloadFailed, where, err, common.ETransferStatus.Failed())
}

func (jptm *jobPartTransferMgr) FailActiveS2SCopy(where string, err error) {
	jptm.failActiveTransfer(transferErrorCodeCopyFailed, where, err, common.ETransferStatus.Failed())
}

func (jptm *jobPartTransferMgr) FailActiveUploadWithStatus(where string, err error, failureStatus common.TransferStatus) {
	jptm.failActiveTransfer(transferErrorCodeUploadFailed, where, err, failureStatus)
}

func (jptm *jobPartTransferMgr) FailActiveDownloadWithStatus(where string, err error, failureStatus common.TransferStatus) {
	jptm.failActiveTransfer(transferErrorCodeDownloadFailed, where, err, failureStatus)
}

func (jptm *jobPartTransferMgr) FailActiveS2SCopyWithStatus(where string, err error, failureStatus common.TransferStatus) {
	jptm.failActiveTransfer(transferErrorCodeCopyFailed, where, err, failureStatus)
}

// TODO: FailActive* need be further refactored with a seperate workitem.
func (jptm *jobPartTransferMgr) TempJudgeUploadOrCopy() (isUpload, isCopy bool) {
	fromTo := jptm.FromTo()

	isUpload = fromTo.IsUpload()
	isCopy = fromTo.IsS2S()

	return isUpload, isCopy
}

func (jptm *jobPartTransferMgr) FailActiveSend(where string, err error) {
	isUpload, isCopy := jptm.TempJudgeUploadOrCopy()

	if isUpload {
		jptm.FailActiveUpload(where, err)
	} else if isCopy {
		jptm.FailActiveS2SCopy(where, err)
	} else {
		// we used to panic here, but that was hard to maintain, e.g. if there was a failure path that wasn't exercised
		// by test suite, and it reached this point in the code, we'd get a panic, but really it's better to just fail the
		// transfer
		jptm.FailActiveDownload(where+" (check operation type, is it really download?)", err)
	}
}

func (jptm *jobPartTransferMgr) FailActiveSendWithStatus(where string, err error, failureStatus common.TransferStatus) {
	isUpload, isCopy := jptm.TempJudgeUploadOrCopy()

	if isUpload {
		jptm.FailActiveUploadWithStatus(where, err, failureStatus)
	} else if isCopy {
		jptm.FailActiveS2SCopyWithStatus(where, err, failureStatus)
	} else {
		panic("invalid state, FailActiveSendWithStatus used by illegal direction")
	}
}

// Use this to mark active transfers (i.e. those where chunk funcs have been scheduled) as failed.
// Unlike just setting the status to failed, this also handles cancellation correctly
func (jptm *jobPartTransferMgr) failActiveTransfer(typ transferErrorCode, descriptionOfWhereErrorOccurred string, err error, failureStatus common.TransferStatus) {
	// TODO here we only act if the transfer is not yet canceled
	// 	however, it's possible that this function is called simultaneously by different chunks
	//  in that case, the logs would be repeated
	//  as of april 9th, 2019, there's no obvious solution without adding more complexity into this part of the code, which is already not pretty and kind of everywhere
	//  consider redesign the lifecycle management in ste
	if !jptm.WasCanceled() {
		jptm.Cancel()
		serviceCode, status, msg := ErrorEx{err}.ErrorCodeAndString()

		if serviceCode == common.CPK_ERROR_SERVICE_CODE {
			cpkAccessFailureLogGLCM.Do(func() {
				common.GetLifecycleMgr().Info("One or more transfers have failed because AzCopy currently does not support blobs encrypted with customer provided keys (CPK). " +
					"If you wish to access CPK-encrypted blobs, we recommend using one of the Azure Storage SDKs to do so.")
			})
		}

		requestID := ErrorEx{err}.MSRequestID()
		fullMsg := fmt.Sprintf("%s. When %s. X-Ms-Request-Id: %s\n", msg, descriptionOfWhereErrorOccurred, requestID) // trailing \n to separate it better from any later, unrelated, log lines
		jptm.logTransferError(typ, jptm.Info().Source, jptm.Info().Destination, fullMsg, status)
		jptm.SetStatus(failureStatus)
		jptm.SetErrorCode(int32(status)) // TODO: what are the rules about when this needs to be set, and doesn't need to be (e.g. for earlier failures)?
		// If the status code was 403, it means there was an authentication error and we exit.
		// User can resume the job if completely ordered with a new sas.
		if status == http.StatusForbidden {
			// quit right away, since without proper authentication no work can be done
			// display a clear message
			common.GetLifecycleMgr().Info(fmt.Sprintf("Authentication failed, it is either not correct, or expired, or does not have the correct permission %s", err.Error()))
			// and use the normal cancelling mechanism so that we can exit in a clean and controlled way
			jobId := jptm.jobPartMgr.Plan().JobID
			CancelPauseJobOrder(jobId, common.EJobStatus.Cancelling())
			// TODO: this results in the final job output line being: Final Job Status: Cancelled
			//     That's not ideal, because it would be better if it said Final Job Status: Failed
			//     However, we don't have any way to distinguish "user cancelled after some failed files" from
			//     from "application cancelled itself after an auth failure".  The former should probably be reported as
			//     Cancelled, so we can't just make a sweeping change to reporting both as Failed.
			//     For now, let's live with it being reported as cancelled, since that's still better than not reporting any
			//     status at all, which is what it did previously (when we called glcm.Error here)
		}
	}
	// TODO: right now the convention re cancellation seems to be that if you cancel, you MUST both call cancel AND
	// TODO: ... call ReportChunkDone (with the latter being done for ALL the expected chunks). Is that maintainable?
	// TODO: ... Is that really ideal, having to call ReportChunkDone for all the chunks AFTER cancellation?
	// TODO: ... but it is currently necessary,because of the way the transfer is only considered done (and automatic epilogue only triggers)
	// TODO: ... if all expected chunks report as done
}

func (jptm *jobPartTransferMgr) PipelineLogInfo() pipeline.LogOptions {
	return jptm.jobPartMgr.(*jobPartMgr).jobMgr.(*jobMgr).PipelineLogInfo()
}

func (jptm *jobPartTransferMgr) Log(level pipeline.LogLevel, msg string) {
	plan := jptm.jobPartMgr.Plan()
	jptm.jobPartMgr.Log(level, fmt.Sprintf("%s: [P#%d-T#%d] ", common.LogLevel(level), plan.PartNum, jptm.transferIndex)+msg)
}

func (jptm *jobPartTransferMgr) ErrorCodeAndString(err error) (int, string) {
	switch e := err.(type) {
	case azblob.StorageError:
		return e.Response().StatusCode, e.Response().Status
	case azfile.StorageError:
		return e.Response().StatusCode, e.Response().Status
	case azbfs.StorageError:
		return e.Response().StatusCode, e.Response().Status
	default:
		return 0, err.Error()
	}
}

type transferErrorCode string

const (
	transferErrorCodeUploadFailed   transferErrorCode = "UPLOADFAILED"
	transferErrorCodeDownloadFailed transferErrorCode = "DOWNLOADFAILED"
	transferErrorCodeCopyFailed     transferErrorCode = "COPYFAILED"
)

func (jptm *jobPartTransferMgr) LogAtLevelForCurrentTransfer(level pipeline.LogLevel, msg string) {
	// order of log elements here is mirrored, with some more added, in logTransferError
	info := jptm.Info()
	fullMsg := common.URLStringExtension(info.Source).RedactSecretQueryParamForLogging() + " " + info.entityTypeLogIndicator() +
		msg +
		" Dst: " + common.URLStringExtension(info.Destination).RedactSecretQueryParamForLogging()

	jptm.Log(level, fullMsg)
}

func (jptm *jobPartTransferMgr) logTransferError(errorCode transferErrorCode, source, destination, errorMsg string, status int) {
	// order of log elements here is mirrored, in subset, in LogForCurrentTransfer
	info := jptm.Info() // TODO we are getting a lot of Info calls and its (presumably) not well-optimized.  Profile that?
	msg := fmt.Sprintf("%v: %v", errorCode, info.entityTypeLogIndicator()) + common.URLStringExtension(source).RedactSecretQueryParamForLogging() +
		fmt.Sprintf(" : %03d : %s\n   Dst: ", status, errorMsg) + common.URLStringExtension(destination).RedactSecretQueryParamForLogging()
	jptm.Log(pipeline.LogError, msg)
}

func (jptm *jobPartTransferMgr) LogUploadError(source, destination, errorMsg string, status int) {
	jptm.logTransferError(transferErrorCodeUploadFailed, source, destination, errorMsg, status)
}

func (jptm *jobPartTransferMgr) LogDownloadError(source, destination, errorMsg string, status int) {
	jptm.logTransferError(transferErrorCodeDownloadFailed, source, destination, errorMsg, status)
}

func (jptm *jobPartTransferMgr) LogS2SCopyError(source, destination, errorMsg string, status int) {
	jptm.logTransferError(transferErrorCodeCopyFailed, source, destination, errorMsg, status)
}

// TODO: Log*Error need be further refactored with a seperate workitem.
func (jptm *jobPartTransferMgr) LogSendError(source, destination, errorMsg string, status int) {
	isUpload, isCopy := jptm.TempJudgeUploadOrCopy()

	if isUpload {
		jptm.LogUploadError(source, destination, errorMsg, status)
	} else if isCopy {
		jptm.LogS2SCopyError(source, destination, errorMsg, status)
	} else {
		panic("invalid state, LogSendError used by illegal direction")
	}
}

func (jptm *jobPartTransferMgr) LogError(resource, context string, err error) {
	_, status, msg := ErrorEx{err}.ErrorCodeAndString()
	MSRequestID := ErrorEx{err}.MSRequestID()
	jptm.Log(pipeline.LogError,
		fmt.Sprintf("%s: %d: %s-%s. X-Ms-Request-Id:%s\n", common.URLStringExtension(resource).RedactSecretQueryParamForLogging(), status, context, msg, MSRequestID))
}

func (jptm *jobPartTransferMgr) LogTransferStart(source, destination, description string) {
	jptm.Log(pipeline.LogInfo,
		fmt.Sprintf("Starting transfer: Source %q Destination %q. %s",
			common.URLStringExtension(source).RedactSecretQueryParamForLogging(),
			common.URLStringExtension(destination).RedactSecretQueryParamForLogging(),
			description))
}

func (jptm *jobPartTransferMgr) LogTransferInfo(level pipeline.LogLevel, source, destination, msg string) {
	jptm.Log(level,
		fmt.Sprintf("Transfer: Source %q Destination %q. %s",
			common.URLStringExtension(source).RedactSecretQueryParamForLogging(),
			common.URLStringExtension(destination).RedactSecretQueryParamForLogging(),
			msg))
}

func (jptm *jobPartTransferMgr) Panic(err error) { jptm.jobPartMgr.Panic(err) }

// Call ReportTransferDone to report when a Transfer for this Job Part has completed
// TODO: I feel like this should take the status & we kill SetStatus
func (jptm *jobPartTransferMgr) ReportTransferDone() uint32 {
	// In case of context leak in job part transfer manager.
	jptm.Cancel()

	// defensive programming check, to make sure this method is not called twice for the same transfer
	// (since if it was, job would count us as TWO completions, and maybe miss another transfer that
	// should have been counted but wasn't)
	// TODO: it would be nice if this protection was actually in jobPartMgr.ReportTransferDone,
	//    but that's harder to implement (would imply need for a threadsafe map there, to track
	//    status by transfer). So for now we are going with the check here. This is the only call
	//    to the jobPartManager anyway (as it Feb 2019)
	if atomic.SwapUint32(&jptm.atomicCompletionIndicator, 1) != 0 {
		panic("cannot report the same transfer done twice")
	}

	return jptm.jobPartMgr.ReportTransferDone(jptm.jobPartPlanTransfer.TransferStatus())
}

func (jptm *jobPartTransferMgr) SourceProviderPipeline() pipeline.Pipeline {
	return jptm.jobPartMgr.SourceProviderPipeline()
}

func (jptm *jobPartTransferMgr) SecurityInfoPersistenceManager() *securityInfoPersistenceManager {
	return jptm.jobPartMgr.SecurityInfoPersistenceManager()
}

func (jptm *jobPartTransferMgr) FolderDeletionManager() common.FolderDeletionManager {
	return jptm.jobPartMgr.FolderDeletionManager()
}

func (jptm *jobPartTransferMgr) GetDestinationRoot() string {
	p := jptm.jobPartMgr.Plan()
	return string(p.DestinationRoot[:p.DestinationRootLength])
}
