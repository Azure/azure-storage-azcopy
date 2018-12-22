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
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type IJobPartTransferMgr interface {
	FromTo() common.FromTo
	Info() TransferInfo
	BlobDstData(dataFileToXfer []byte) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata)
	FileDstData(dataFileToXfer []byte) (headers azfile.FileHTTPHeaders, metadata azfile.Metadata)
	PreserveLastModifiedTime() (time.Time, bool)
	BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier)
	//ScheduleChunk(chunkFunc chunkFunc)
	Context() context.Context
	SlicePool() common.ByteSlicePooler
	CacheLimiter() common.CacheLimiter
	StartJobXfer()
	IsForceWriteTrue() bool
	ReportChunkDone() (lastChunk bool, chunksDone uint32)
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
	FailActiveUpload(err error)
	FailActiveDownload(err error)
	FailActiveUploadWithDetails(err error, prefix string, failureStatus common.TransferStatus)
	FailActiveDownloadWithDetails(err error, prefix string, failureStatus common.TransferStatus)
	LogUploadError(source, destination, errorMsg string, status int)
	LogDownloadError(source, destination, errorMsg string, status int)
	LogS2SCopyError(source, destination, errorMsg string, status int)
	LogError(resource, context string, err error)
	LogTransferStart(source, destination, description string)
	LogChunkStatus(id common.ChunkID, reason common.WaitReason)
	common.ILogger
}

type TransferInfo struct {
	BlockSize   uint32
	Source      string
	SourceSize  int64
	Destination string

	SrcHTTPHeaders azblob.BlobHTTPHeaders // User for S2S copy, where per transfer's src properties need be set in destination.
	SrcMetadata    common.Metadata

	// Transfer info for blob only
	SrcBlobType azblob.BlobType

	// NumChunks is the number of chunks in which transfer will be split into while uploading the transfer.
	// NumChunks is not used in case of AppendBlob transfer.
	NumChunks uint16
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type chunkFunc func(int)

// jobPartTransferMgr represents the runtime information for a Job Part's transfer
type jobPartTransferMgr struct {
	jobPartMgr          IJobPartMgr // Refers to the "owning" Job Part
	jobPartPlanTransfer *JobPartPlanTransfer
	transferIndex       uint32

	// the context of this transfer; allows any failing chunk to cancel the whole transfer
	ctx context.Context

	// Call cancel to cancel the transfer
	cancel context.CancelFunc

	numChunks uint32

	actionAfterLastChunk func()

	// NumberOfChunksDone represents the number of chunks of a transfer
	// which are either completed or failed.
	// NumberOfChunksDone determines the final cancellation or completion of a transfer
	atomicChunksDone uint32

	/*
		@Parteek removed 3/23 morning, as jeff ad equivalent
		// transfer chunks are put into this channel and execution engine takes chunk out of this channel.
		chunkChannel chan<- ChunkMsg*/
}

func (jptm *jobPartTransferMgr) FromTo() common.FromTo {
	return jptm.jobPartMgr.Plan().FromTo
}

func (jptm *jobPartTransferMgr) StartJobXfer() {
	jptm.jobPartMgr.StartJobXfer(jptm)
}

func (jptm *jobPartTransferMgr) IsForceWriteTrue() bool {
	return jptm.jobPartMgr.IsForceWriteTrue()
}

func (jptm *jobPartTransferMgr) Info() TransferInfo {
	plan := jptm.jobPartMgr.Plan()
	src, dst := plan.TransferSrcDstStrings(jptm.transferIndex)
	dstBlobData := plan.DstBlobData

	srcHTTPHeaders, srcMetadata, srcBlobType := plan.TransferSrcPropertiesAndMetadata(jptm.transferIndex)
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
		BlockSize:      blockSize,
		Source:         src,
		SourceSize:     sourceSize,
		Destination:    dst,
		SrcHTTPHeaders: srcHTTPHeaders,
		SrcMetadata:    srcMetadata,
		SrcBlobType:    srcBlobType,
	}
}

func (jptm *jobPartTransferMgr) Context() context.Context {
	return jptm.ctx
}

func (jptm * jobPartTransferMgr) SlicePool() common.ByteSlicePooler {
	return jptm.jobPartMgr.SlicePool()
}

func (jptm * jobPartTransferMgr) CacheLimiter() common.CacheLimiter {
	return jptm.jobPartMgr.CacheLimiter()
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

// PreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it returns the lastModifiedTime of the source.
func (jptm *jobPartTransferMgr) PreserveLastModifiedTime() (time.Time, bool) {
	if preserveLastModifiedTime := jptm.jobPartMgr.(*jobPartMgr).localDstData(); preserveLastModifiedTime {
		lastModifiedTime := jptm.jobPartPlanTransfer.ModifiedTime
		return time.Unix(0, lastModifiedTime), true
	}
	return time.Time{}, false
}

func (jptm *jobPartTransferMgr) BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier) {
	return jptm.jobPartMgr.BlobTiers()
}

func (jptm *jobPartTransferMgr) SetNumberOfChunks(numChunks uint32) {
	jptm.numChunks = numChunks
}

func (jptm *jobPartTransferMgr) SetActionAfterLastChunk(f func()) {
	jptm.actionAfterLastChunk = f
}

// Call Done when a chunk has completed its transfer; this method returns the number of chunks completed so far
func (jptm *jobPartTransferMgr) ReportChunkDone() (lastChunk bool, chunksDone uint32) {
	chunksDone = atomic.AddUint32(&jptm.atomicChunksDone, 1)
	lastChunk = chunksDone == jptm.numChunks
	if lastChunk {
		jptm.runActionAfterLastChunk()
	}
	return lastChunk, chunksDone
}

// If an automatic action has been specified for after the last chunk, run it now
// (Prior to introduction of this routine, individual chunkfuncs had to check the return values
// of ReportChunkDone and then implement their own versions of the necessary transfer epilogue code.
// But that led to unwanted duplication of epilogue code, in the various types of chunkfunc. This routine
// makes it easier to create DRY epilogue code.)
func (jptm *jobPartTransferMgr) runActionAfterLastChunk(){
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

func (jptm *jobPartTransferMgr) LogChunkStatus(id common.ChunkID, reason common.WaitReason){
	jptm.jobPartMgr.LogChunkStatus(id, reason)
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

func (jptm *jobPartTransferMgr) FailActiveUpload(err error) {
	jptm.failActiveTransfer(err, "", common.ETransferStatus.Failed(), true)
}

func (jptm *jobPartTransferMgr) FailActiveDownload(err error) {
	jptm.failActiveTransfer(err, "", common.ETransferStatus.Failed(), false)
}

func (jptm *jobPartTransferMgr) FailActiveUploadWithDetails(err error, prefix string, failureStatus common.TransferStatus) {
	jptm.failActiveTransfer(err, prefix, failureStatus, true)
}

func (jptm *jobPartTransferMgr) FailActiveDownloadWithDetails(err error, prefix string, failureStatus common.TransferStatus) {
	jptm.failActiveTransfer(err, prefix, failureStatus, false)
}

// Use this to mark active transfers (i.e. those where chunk funcs have been scheduled) as failed.
// Unlike just setting the status to failed, this also handles cancellation correctly
func (jptm *jobPartTransferMgr) failActiveTransfer(err error, prefix string, failureStatus common.TransferStatus, isUpload bool){
	// TODO: question. Prior to refactoring some code did a debug level log when WasCancelled is true (e.g. blob upload did)
	// TODO: .. do we really need that? It's ommitted, for now.

	if !jptm.WasCanceled() {
		jptm.Cancel()
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		typ := transferErrorCodeDownloadFailed
		if isUpload {
			typ = transferErrorCodeUploadFailed
		}
		jptm.logTransferError(typ, jptm.Info().Source, jptm.Info().Destination, prefix + msg, status)
		jptm.SetStatus(failureStatus)
		jptm.SetErrorCode(int32(status)) // TODO: what are the rules about when this needs to be set, and doesn't need to be (e.g. for earlier failures)?
		// If the status code was 403, it means there was an authentication error and we exit.
		// User can resume the job if completely ordered with a new sas.
		if status == http.StatusForbidden {
			// TODO: should this really exit??? why not just log like everything else does???  We've Failed the transfer anyway....
			common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
		}
	}
	// TODO: right now the convention re cancellation seems to be that if you cancel, you MUST both call cancel AND
	// TODO: ... call ReportChunkDone (with the latter being done for ALL the expnected chunks). Is that maintainable?
	// TODO: ... Is that really ideal, having to call ReportChunkDone for all the chunks AFTER cancellation?
	// TODO: ... but it is currently necesary, beause of the way the transfer is only considered done (and automatic epilogue only triggers)
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

func (jptm *jobPartTransferMgr) logTransferError(errorCode transferErrorCode, source, destination, errorMsg string, status int) {
	msg := fmt.Sprintf("%v: ", errorCode) + common.URLStringExtension(source).RedactSigQueryParamForLogging() +
		fmt.Sprintf(" : %03d : %s\n   Dst: ", status, errorMsg) + common.URLStringExtension(destination).RedactSigQueryParamForLogging()
	jptm.Log(pipeline.LogError, msg)
	//jptm.Log(pipeline.LogError, fmt.Sprintf("%v: %s: %03d : %s\n   Dst: %s",
	//	errorCode, common.URLStringExtension(source).RedactSigQueryParamForLogging(),
	//	status, errorMsg, common.URLStringExtension(destination).RedactSigQueryParamForLogging()))
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

func (jptm *jobPartTransferMgr) LogError(resource, context string, err error) {
	status, msg := ErrorEx{err}.ErrorCodeAndString()
	jptm.Log(pipeline.LogError,
		fmt.Sprintf("%s: %d: %s-%s", common.URLStringExtension(resource).RedactSigQueryParamForLogging(), status, context, msg))
}

func (jptm *jobPartTransferMgr) LogTransferStart(source, destination, description string) {
	jptm.Log(pipeline.LogInfo,
		fmt.Sprintf("Starting transfer: Source %q Destination %q. %s",
			common.URLStringExtension(source).RedactSigQueryParamForLogging(),
			common.URLStringExtension(destination).RedactSigQueryParamForLogging(),
			description))
}

func (jptm *jobPartTransferMgr) Panic(err error) { jptm.jobPartMgr.Panic(err) }

// Call ReportTransferDone to report when a Transfer for this Job Part has completed
// TODO: I feel like this should take the status & we kill SetStatus
func (jptm *jobPartTransferMgr) ReportTransferDone() uint32 {
	// In case of context leak in job part transfer manager.
	jptm.Cancel()

	return jptm.jobPartMgr.ReportTransferDone()
}
