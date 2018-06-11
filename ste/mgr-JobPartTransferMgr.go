package ste

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

type IJobPartTransferMgr interface {
	FromTo() common.FromTo
	Info() TransferInfo
	BlobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata)
	FileDstData(dataFileToXfer common.MMF) (headers azfile.FileHTTPHeaders, metadata azfile.Metadata)
	PreserveLastModifiedTime() (time.Time, bool)
	BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier)
	//ScheduleChunk(chunkFunc chunkFunc)
	Context() context.Context
	StartJobXfer()
	IsForceWriteTrue() bool
	ReportChunkDone() (lastChunk bool, chunksDone uint32)
	TransferStatus() common.TransferStatus
	SetStatus(status common.TransferStatus)
	SetNumberOfChunks(numChunks uint32)
	ReportTransferDone() uint32
	RescheduleTransfer()
	ScheduleChunks(chunkFunc chunkFunc)
	AddToBytesDone(value int64) int64
	Cancel()
	WasCanceled() bool
	common.ILogger
}

type TransferInfo struct {
	BlockSize   uint32
	Source      string
	SourceSize  int64
	Destination string

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
	return TransferInfo{
		BlockSize:   dstBlobData.BlockSize,
		Source:      src,
		SourceSize:  plan.Transfer(jptm.transferIndex).SourceSize,
		Destination: dst,
	}
}

func (jptm *jobPartTransferMgr) Context() context.Context {
	return jptm.ctx
}

func (jptm *jobPartTransferMgr) RescheduleTransfer() {
	jptm.jobPartMgr.RescheduleTransfer(jptm)
}

func (jptm *jobPartTransferMgr) ScheduleChunks(chunkFunc chunkFunc) {
	jptm.jobPartMgr.ScheduleChunks(chunkFunc)
}

func (jptm *jobPartTransferMgr) BlobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata) {
	return jptm.jobPartMgr.(*jobPartMgr).blobDstData(dataFileToXfer)
}

func (jptm *jobPartTransferMgr) FileDstData(dataFileToXfer common.MMF) (headers azfile.FileHTTPHeaders, metadata azfile.Metadata) {
	return jptm.jobPartMgr.(*jobPartMgr).fileDstData(dataFileToXfer)
}

func (jptm *jobPartTransferMgr) AddToBytesDone(value int64) int64 {
	return jptm.jobPartMgr.AddToBytesDone(value)
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

// Call Done when a chunk has completed its transfer; this method returns the number of chunks completed so far
func (jptm *jobPartTransferMgr) ReportChunkDone() (lastChunk bool, chunksDone uint32) {
	chunksDone = atomic.AddUint32(&jptm.atomicChunksDone, 1)
	return chunksDone == jptm.numChunks, chunksDone
}

//
func (jptm *jobPartTransferMgr) TransferStatus() common.TransferStatus {
	return jptm.jobPartPlanTransfer.TransferStatus()
}

// TransferStatus updates the status of given transfer for given jobId and partNumber
func (jptm *jobPartTransferMgr) SetStatus(status common.TransferStatus) {
	jptm.jobPartPlanTransfer.SetTransferStatus(status, false)
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

func (jptm *jobPartTransferMgr) PipelineLogInfo() pipeline.LogOptions {
	return jptm.jobPartMgr.(*jobPartMgr).jobMgr.(*jobMgr).PipelineLogInfo()
}

func (jptm *jobPartTransferMgr) Log(level pipeline.LogLevel, msg string) {
	plan := jptm.jobPartMgr.Plan()
	jptm.jobPartMgr.Log(level, fmt.Sprintf("JobID=%v, Part#=%d, Transfer#=%d: "+msg, plan.JobID, plan.PartNum, jptm.transferIndex))
}
func (jptm *jobPartTransferMgr) Panic(err error) { jptm.jobPartMgr.Panic(err) }

// Call ReportTransferDone to report when a Transfer for this Job Part has completed
// TODO: I feel like this should take the status & we kill SetStatus
func (jptm *jobPartTransferMgr) ReportTransferDone() uint32 {
	return jptm.jobPartMgr.ReportTransferDone()
}
