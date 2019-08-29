package ste

import (
	"context"
	"fmt"
	"net/http"
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
	BlobDstData(dataFileToXfer []byte) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata)
	FileDstData(dataFileToXfer []byte) (headers azfile.FileHTTPHeaders, metadata azfile.Metadata)
	BfsDstData(dataFileToXfer []byte) (headers azbfs.BlobFSHTTPHeaders)
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
	FileCountLimiter() common.CacheLimiter
	StartJobXfer()
	GetOverwriteOption() common.OverwriteOption
	ReportChunkDone(id common.ChunkID) (lastChunk bool, chunksDone uint32)
	UnsafeReportChunkDone() (lastChunk bool, chunksDone uint32)
	TransferStatus() common.TransferStatus
	SetStatus(status common.TransferStatus)
	SetErrorCode(errorCode int32)
	SetNumberOfChunks(numChunks uint32)
	SetActionAfterLastChunk(f func())
	ReportTransferDone() uint32
	RescheduleTransfer()
	ScheduleChunks(chunkFunc chunkFunc)
	Cancel()
	WasCanceled() bool
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
	common.ILogger
}

type TransferInfo struct {
	BlockSize   uint32
	Source      string
	SourceSize  int64
	Destination string

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

type SrcProperties struct {
	SrcHTTPHeaders common.ResourceHTTPHeaders // User for S2S copy, where per transfer's src properties need be set in destination.
	SrcMetadata    common.Metadata
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type chunkFunc func(int)

// jobPartTransferMgr represents the runtime information for a Job Part's transfer
type jobPartTransferMgr struct {

	// NumberOfChunksDone represents the number of chunks of a transfer
	// which are either completed or failed.
	// NumberOfChunksDone determines the final cancellation or completion of a transfer
	atomicChunksDone uint32

	// used defensively to protect against accidental double counting
	atomicCompletionIndicator uint32

	// how many bytes have been successfully transferred
	// (hard to infer from atomicChunksDone because that counts both successes and failures)
	atomicSuccessfulBytes uint64

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

func (jptm *jobPartTransferMgr) FromTo() common.FromTo {
	return jptm.jobPartMgr.Plan().FromTo
}

func (jptm *jobPartTransferMgr) StartJobXfer() {
	jptm.jobPartMgr.StartJobXfer(jptm)
}

func (jptm *jobPartTransferMgr) GetOverwriteOption() common.OverwriteOption {
	return jptm.jobPartMgr.GetOverwriteOption()
}

func (jptm *jobPartTransferMgr) Info() TransferInfo {
	plan := jptm.jobPartMgr.Plan()
	src, dst := plan.TransferSrcDstStrings(jptm.transferIndex)
	dstBlobData := plan.DstBlobData

	srcHTTPHeaders, srcMetadata, srcBlobType, srcBlobTier, s2sGetPropertiesInBackend, DestLengthValidation, s2sSourceChangeValidation, s2sInvalidMetadataHandleOption :=
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

func (jptm *jobPartTransferMgr) RescheduleTransfer() {
	jptm.jobPartMgr.RescheduleTransfer(jptm)
}

func (jptm *jobPartTransferMgr) ScheduleChunks(chunkFunc chunkFunc) {
	jptm.jobPartMgr.ScheduleChunks(chunkFunc)
}

func (jptm *jobPartTransferMgr) BlobDstData(dataFileToXfer []byte) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata) {
	return jptm.jobPartMgr.(*jobPartMgr).blobDstData(jptm.Info().Source, dataFileToXfer)
}

func (jptm *jobPartTransferMgr) FileDstData(dataFileToXfer []byte) (headers azfile.FileHTTPHeaders, metadata azfile.Metadata) {
	return jptm.jobPartMgr.(*jobPartMgr).fileDstData(jptm.Info().Source, dataFileToXfer)
}

func (jptm *jobPartTransferMgr) BfsDstData(dataFileToXfer []byte) (headers azbfs.BlobFSHTTPHeaders) {
	return jptm.jobPartMgr.(*jobPartMgr).bfsDstData(jptm.Info().Source, dataFileToXfer)
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
	if jptm.TransferStatus() > 0 {
		n := uint64(jptm.Info().BlockSize) // TODO: this is just an assumption/approximation (since last one in each file will be different). Maybe add Length into chunkID one day, and use that...
		atomic.AddUint64(&jptm.atomicSuccessfulBytes, n)
		JobsAdmin.AddSuccessfulBytesInActiveFiles(n)
	}

	// Do our actual processing
	chunksDone = atomic.AddUint32(&jptm.atomicChunksDone, 1)
	lastChunk = chunksDone == jptm.numChunks
	if lastChunk {
		jptm.runActionAfterLastChunk()
		JobsAdmin.AddSuccessfulBytesInActiveFiles(-atomic.LoadUint64(&jptm.atomicSuccessfulBytes)) // subtract our bytes from the active files bytes, because we are done now
	}
	return lastChunk, chunksDone
}

// TODO: phase this method out.  It's just here to support parts of the codebase that don't yet have chunk IDs
func (jptm *jobPartTransferMgr) UnsafeReportChunkDone() (lastChunk bool, chunksDone uint32) {
	return jptm.ReportChunkDone(common.NewChunkID("", 0))
}

// If an automatic action has been specified for after the last chunk, run it now
// (Prior to introduction of this routine, individual chunkfuncs had to check the return values
// of ReportChunkDone and then implement their own versions of the necessary transfer epilogue code.
// But that led to unwanted duplication of epilogue code, in the various types of chunkfunc. This routine
// makes it easier to create DRY epilogue code.)
func (jptm *jobPartTransferMgr) runActionAfterLastChunk() {
	if jptm.actionAfterLastChunk != nil {
		jptm.actionAfterLastChunk()
		jptm.actionAfterLastChunk = nil // make sure it can't be run again, since epilogue methods are not expected to be idempotent
	}
}

//
func (jptm *jobPartTransferMgr) TransferStatus() common.TransferStatus {
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

	fromIsLocal := fromTo.From() == common.ELocation.Local()
	toIsLocal := fromTo.To() == common.ELocation.Local()

	isUpload = fromIsLocal && !toIsLocal
	isCopy = !fromIsLocal && !toIsLocal

	return isUpload, isCopy
}

func (jptm *jobPartTransferMgr) FailActiveSend(where string, err error) {
	isUpload, isCopy := jptm.TempJudgeUploadOrCopy()

	if isUpload {
		jptm.FailActiveUpload(where, err)
	} else if isCopy {
		jptm.FailActiveS2SCopy(where, err)
	} else {
		panic("invalid state, FailActiveSend used by illegal direction")
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
		status, msg := ErrorEx{err}.ErrorCodeAndString()
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
			common.GetLifecycleMgr().Error(fmt.Sprintf("Authentication failed, it is either not correct, or expired, or does not have the correct permission %s", err.Error()))
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
	fullMsg := common.URLStringExtension(jptm.Info().Source).RedactSecretQueryParamForLogging() + " " +
		msg +
		" Dst: " + common.URLStringExtension(jptm.Info().Destination).RedactSecretQueryParamForLogging()

	jptm.Log(level, fullMsg)
}

func (jptm *jobPartTransferMgr) logTransferError(errorCode transferErrorCode, source, destination, errorMsg string, status int) {
	// order of log elements here is mirrored, in subset, in LogForCurrentTransfer
	msg := fmt.Sprintf("%v: ", errorCode) + common.URLStringExtension(source).RedactSecretQueryParamForLogging() +
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
	status, msg := ErrorEx{err}.ErrorCodeAndString()
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

	return jptm.jobPartMgr.ReportTransferDone()
}

func (jptm *jobPartTransferMgr) SourceProviderPipeline() pipeline.Pipeline {
	return jptm.jobPartMgr.SourceProviderPipeline()
}
