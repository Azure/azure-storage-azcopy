package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"sync/atomic"
	"time"
)

type IJobPartTransferMgr interface {
	Priority() common.JobPriority
	FromTo() common.FromTo
	Info() TransferInfo
	BlobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata)
	PreserveLastModifiedTime() (time.Time, bool)
	ScheduleChunk(chunkFunc chunkFunc)
	ReportChunkDone() (lastChunk bool, chunksDone uint32)
	SetStatus(status common.TransferStatus)
	ReportTransferDone() (lastTransfer bool, transfersDone uint32)

	/*
	TODO: @Parteek this is removed 3/23 morning
	SetChunkChannel(chunkChannel chan <- ChunkMsg)
	ChunkChannel() (chan <- ChunkMsg)
	RunPrologue(pacer *pacer)*/
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

type chunkFunc func()

// jobPartTransferMgr represents the runtime information for a Job Part's transfer
type jobPartTransferMgr struct {
	jobPartMgr          IJobPartMgr // Refers to the "owning" Job Part
	jobPartPlanTransfer *JobPartPlanTransfer
	transferIndex       uint32

	// the context of this transfer; allows any failing chunk to cancel the whole transfer
	ctx context.Context

	// Call cancel to cancel the transfer
	cancel context.CancelFunc

	newJobXfer newJobXfer // Method used to start the transfer
	priority   common.JobPriority
	chunkCh    chan chunkFunc // Channel used to queue transfer chunks
	pacer      pacer          // Pacer used by chunks when uploading data

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

func (jptm *jobPartTransferMgr) Priority() common.JobPriority {
	return jptm.priority
}

func (jptm *jobPartTransferMgr) FromTo() common.FromTo {
	return jptm.jobPartMgr.Plan().FromTo
}

func (jptm *jobPartTransferMgr) ScheduleChunk(chunkFunc chunkFunc) {
	JobsAdmin.ScheduleChunk(jptm, chunkFunc)
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

func (jptm *jobPartTransferMgr) BlobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata) {
	return jptm.jobPartMgr.(*jobPartMgr).blobDstData(dataFileToXfer)
}

/*func (jptm *jobPartTransferMgr) SetChunkChannel(chunkChannel chan <- ChunkMsg) {
	jptm.chunkChannel = chunkChannel
}

func (jptm *jobPartTransferMgr) ChunkChannel() (chan <- ChunkMsg){
	return jptm.chunkChannel
}*/

// PreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it returns the lastModifiedTime of the source.
func (jptm *jobPartTransferMgr) PreserveLastModifiedTime() (time.Time, bool) {
	if preserveLastModifiedTime := jptm.jobPartMgr.(*jobPartMgr).localDstData(); preserveLastModifiedTime {
		lastModifiedTime := jptm.jobPartPlanTransfer.ModifiedTime
		return time.Unix(0, int64(lastModifiedTime)), true
	}
	return time.Time{}, false
}

func (jptm *jobPartTransferMgr) RunPrologue(pacer *pacer){
	jptm.jobPartMgr.RunPrologue(jptm, pacer)
}

// Call Done when a chunk has completed its transfer; this method returns the number of chunks completed so far
func (jptm *jobPartTransferMgr) ReportChunkDone() (lastChunk bool, chunksDone uint32) {
	chunksDone = atomic.AddUint32(&jptm.atomicChunksDone, 1)
	return chunksDone == jptm.numChunks, chunksDone
}

// TransferStatus updates the status of given transfer for given jobId and partNumber
func (jptm *jobPartTransferMgr) SetStatus(status common.TransferStatus) {
	jptm.jobPartPlanTransfer.SetTransferStatus(status)
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

func (jptm *jobPartTransferMgr) PipelineLogInfo() (pipeline.LogOptions) {
	return jptm.jobPartMgr.(*jobPartMgr).jobMgr.(*jobMgr).PipelineLogInfo()
}

func (jptm *jobPartTransferMgr) Log(level pipeline.LogLevel, msg string) {
	plan := jptm.jobPartMgr.Plan()
	jptm.jobPartMgr.Log(level, fmt.Sprintf("JobID=%v, Part#=%d, Transfer#=%d: "+msg, plan.JobID, plan.PartNum, jptm.transferIndex))
}
func (jptm *jobPartTransferMgr) Panic(err error) { jptm.jobPartMgr.Panic(err) }

// Call ReportTransferDone to report when a Transfer for this Job Part has completed
// TODO: I feel like this should take the status & we kill SetStatus
func (jptm *jobPartTransferMgr) ReportTransferDone() (lastTransfer bool, transfersDone uint32) {
	return jptm.jobPartMgr.ReportTransferDone()
	// todo : last transfer should tell that part is done.
}
