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
	Locations() (srcLocation, dstLocation common.Location, blobType common.BlobType)
	Info() TransferInfo
	BlobDstData(dataFileToXfer common.MMF) (headers azblob.BlobHTTPHeaders, metadata azblob.Metadata)
	PreserveLastModifiedTime() (time.Time, bool)

	ReportChunkDone() (lastChunk bool, chunksDone uint32)
	SetStatus(status common.TransferStatus)
	ReportTransferDone() (lastTransfer bool, transfersDone uint32)

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
}

func (jptm *jobPartTransferMgr) Locations() (srcLocation, dstLocation common.Location, blobType common.BlobType) {
	plan := jptm.jobPartMgr.Plan()
	return plan.SrcLocation, plan.DstLocation, plan.DstBlobData.BlobType
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

// PreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it returns the lastModifiedTime of the source.
func (jptm *jobPartTransferMgr) PreserveLastModifiedTime() (time.Time, bool) {
	if preserveLastModifiedTime := jptm.jobPartMgr.(*jobPartMgr).localDstData(); preserveLastModifiedTime {
		lastModifiedTime := jptm.jobPartPlanTransfer.ModifiedTime
		return time.Unix(0, int64(lastModifiedTime)), true
	}
	return time.Time{}, false
}

// Call Done when a chunk has completed its transfer; this method returns the number of chunks completed so far
func (jptm *jobPartTransferMgr) ReportChunkDone() (lastChunk bool, chunksDone uint32) {
	chunksDone = atomic.AddUint32(&jptm.atomicChunksDone, 1)
	return chunksDone == jptm.numChunks, chunksDone
	// TODO: Tell the part that this transfer is done?
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
func (jptm *jobPartTransferMgr) ShouldLog(level pipeline.LogLevel) bool { 	return jptm.jobPartMgr.ShouldLog(level) }
func (jptm *jobPartTransferMgr) Log(level pipeline.LogLevel, msg string) {
	plan := jptm.jobPartMgr.Plan()
	jptm.jobPartMgr.Log(level, fmt.Sprintf("JobID=%v, Part#=%d, Transfer#=%d: "+msg, plan.JobID, plan.PartNum, jptm.transferIndex))
}
func (jptm *jobPartTransferMgr) Panic(err error) { jptm.jobPartMgr.Panic(err) }

// Call ReportTransferDone to report when a Transfer for this Job Part has completed
// TODO: I feel like this should take the status & we kill SetStatus
func (jptm *jobPartTransferMgr) ReportTransferDone() (lastTransfer bool, transfersDone uint32) {
	return jptm.jobPartMgr.ReportTransferDone()
}
