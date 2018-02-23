package ste

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/edsrzf/mmap-go"
	"sync/atomic"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/http"
	"strings"
	"time"
	"os"
	"errors"
)

// TransfersInfo represents the runtime information of a transfer of a JobPartOrder
type TransferInfo struct {
	// the context of the transfer
	ctx context.Context

	// cancel func is the func to be called to cancel the transfer
	cancel context.CancelFunc

	// NumberOfChunksDone represents the number of chunks of a transfer
	// which are either completed or failed.
	// NumberOfChunksDone determines the final cancellation or completion of a transfer
	NumberOfChunksDone uint32
}

func (t TransferInfo) getNumberOfChunksDone() (uint32){
	return atomic.LoadUint32(&t.NumberOfChunksDone)
}

// JobPartPlanInfo represents the runtime information of a JobPartOrder
type JobPartPlanInfo struct {

	// the context of a JobPartOrder
	// All the parts of Job share same context
	ctx context.Context

	// cancel func is the func to be called to cancel a Job
	cancel context.CancelFunc

	// filename is name of JobPartOrder file
	fileName string

	// memMap represents the memory map byte slice
	memMap mmap.MMap

	// TransfersInfo list of transfer info of the transfers of JobPartOrder
	TransfersInfo []TransferInfo

	// numberOfTransfersDone_doNotUse represents the number of transfer of JobPartOrder
	// which are either completed or failed
	// numberOfTransfersDone_doNotUse determines the final cancellation of JobPartOrder
	numberOfTransfersDone_doNotUse uint32
}

// numberOfTransfersDone returns the numberOfTransfersDone_doNotUse of JobPartPlanInfo
// instance in thread safe manner
func (jPartPlanInfo *JobPartPlanInfo) numberOfTransfersDone() uint32 {
	return atomic.LoadUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse)
}

// incrementNumberOfTransfersDone increment the numberOfTransfersDone_doNotUse of JobPartPlanInfo
// instance in thread safe manner by 1
func (jPartPlanInfo *JobPartPlanInfo) incrementNumberOfTransfersDone() uint32 {
	return atomic.AddUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, 1)
}

// setNumberOfTransfersDone sets the number of transfers done to a specific value
// in a thread safe manner
func (jPartPlanInfo *JobPartPlanInfo) setNumberOfTransfersDone(val uint32) {
	atomic.StoreUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, val)
}

// TransferMsg represents the transfer message for scheduling the transfers
// These messages are exchanged over transfer channel of Coordinator Channels
type TransferMsg struct {
	// jobId - JobId of job to which the transfer belongs to
	jobId common.JobID
	// partNumber is the part of the Job to which transfer belongs to
	partNumber common.PartNumber
	// transferIndex is the index of transfer in JobPartOrder
	transferIndex uint32
	// infoMap is the pointer to in memory JobsInfoMap
	infoMap *JobsInfoMap

	// TransferContext is the context of transfer to be scheduled
	TransferContext context.Context
}

// getTransferMsgDetail returns the details of a transfer of TransferMsg with JobId, part number and transfer index
func (t TransferMsg) getTransferMsgDetail () (TransferMsgDetail){
	// jHandler is the JobPartPlanInfo Pointer for given JobId and part number
	jHandler := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)

	// jPartPlanPointer is the memory map JobPartPlan for given JobId and part number
	jPartPlanPointer := jHandler.getJobPartPlanPointer()

	sourceType := jPartPlanPointer.SrcLocationType
	destinationType := jPartPlanPointer.DstLocationType
	source, destination := jHandler.getTransferSrcDstDetail(t.transferIndex)
	chunkSize := jPartPlanPointer.BlobData.BlockSize
	return TransferMsgDetail{JobId:t.jobId, PartNumber:t.partNumber, TransferId:t.transferIndex,
		ChunkSize:chunkSize, SourceType:sourceType, Source:source, DestinationType:destinationType,
		Destination:destination, TransferCtx:jHandler.TransfersInfo[t.transferIndex].ctx,
		TransferCancelFunc:jHandler.TransfersInfo[t.transferIndex].cancel,
		NumberOfChunksDone:&(jHandler.TransfersInfo[t.transferIndex].NumberOfChunksDone), JobHandlerMap:t.infoMap}
}

func (t TransferMsg) getJobInfo() (*JobInfo){
	return t.infoMap.LoadJobInfoForJob(t.jobId)
}

func (t TransferMsg) SourceDestination() (source, destination string){
	jHandler := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)
	source, destination = jHandler.getTransferSrcDstDetail(t.transferIndex)
	return source, destination
}

func (t TransferMsg) getSourceSize() (uint64){
	return t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber).Transfer(t.transferIndex).SourceSize
}

func (t TransferMsg) TransferCancelFunc() (func()){
	return t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber).TransfersInfo[t.transferIndex].cancel
}
// getNumChunks api returns the number of chunks depending on source Type and destination type
func (t TransferMsg) getNumberOfChunks() uint16 {
	// jHandler is the JobPartPlanInfo Pointer for given JobId and part number
	jHandler := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)

	// jPartPlanPointer is the memory map JobPartPlan for given JobId and part number
	jPartPlanPointer := jHandler.getJobPartPlanPointer()

	transfer := jHandler.Transfer(t.transferIndex)

	blockSize := jPartPlanPointer.BlobData.BlockSize

	if uint64(transfer.SourceSize)% blockSize== 0 {
		return uint16(uint64(transfer.SourceSize) / blockSize)
	} else {
		return uint16(uint64(transfer.SourceSize)/blockSize) + 1
	}
}

func (t TransferMsg) getTransferIdentifierString() (string){
	return fmt.Sprintf("transfer Id %d of Job with JobId %s and part number %d", t.transferIndex, t.jobId.String(), t.partNumber)
}

func (t TransferMsg) getBlockSize() (uint64){
	return t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber).
										getJobPartPlanPointer().BlobData.BlockSize
}

// UpdateNumTransferDone api increments the var numberOfTransfersDone_doNotUse by 1 atomically
// If this numberOfTransfersDone_doNotUse equals the number of transfer in a job part,
// all transfers of Job Part have either paused, cancelled or completed
func (t TransferMsg) updateNumberOfTransferDone() {
	jobInfo := t.infoMap.LoadJobInfoForJob(t.jobId)
	jHandler := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)
	jPartPlanInfo := jHandler.getJobPartPlanPointer()
	totalNumberofTransfersCompleted := jHandler.numberOfTransfersDone()
	jobInfo.Log(common.LogInfo, fmt.Sprintf("total number of transfers paused, cancelled or completed for Job %s and part number %d is %d", t.jobId, t.partNumber, totalNumberofTransfersCompleted))
	if jHandler.incrementNumberOfTransfersDone() == jPartPlanInfo.NumTransfers {
		updateNumberOfPartsDone(t.jobId, t.infoMap)
	}
}

func (t TransferMsg) incrementNumberOfChunksDone() (uint32){
	jPartPlanInfo := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)

	return atomic.AddUint32(&(jPartPlanInfo.TransfersInfo[t.transferIndex].NumberOfChunksDone), 1)
}

// updateTransferStatus updates the status of given transfer for given jobId and partNumber in thread safe manner
func (t TransferMsg) updateTransferStatus(transferStatus common.TransferStatus) {
	jHandler := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)
	transferHeader := jHandler.Transfer(t.transferIndex)
	transferHeader.setTransferStatus(transferStatus)
}

// getBlobHttpHeaders returns the azblob.BlobHTTPHeaders with blobData attributes of JobPart Order
func (t TransferMsg)getBlobHttpHeaders(sourceBytes []byte) azblob.BlobHTTPHeaders {

	// jPartPlanHeader is the JobPartPlan header for memory mapped JobPartOrder File
	jPartPlanHeader := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber).getJobPartPlanPointer()
	contentTpe := ""
	contentEncoding := ""
	// If NoGuessMimeType is set to true, then detecting the content type
	if jPartPlanHeader.BlobData.NoGuessMimeType {
		contentTpe = http.DetectContentType(sourceBytes)
	} else {
		// If the NoGuessMimeType is set to false, then using the user given content-type
		if jPartPlanHeader.BlobData.ContentEncodingLength > 0 {
			contentTpe = string(jPartPlanHeader.BlobData.ContentType[:])
		}
	}

	if jPartPlanHeader.BlobData.ContentEncodingLength > 0 {
		contentEncoding = string(jPartPlanHeader.BlobData.ContentEncoding[:])
	}
	httpHeaderProperties := azblob.BlobHTTPHeaders{ContentType: contentTpe, ContentEncoding: contentEncoding}
	return httpHeaderProperties
}

// getJobPartMetaData returns the meta data of JobPart Order store in following format
// "key1=val1;key2=val2;key3=val3"
func (t TransferMsg) getJobPartMetaData() azblob.Metadata {
	// jPartPlanHeader is the JobPartPlan header for memory mapped JobPartOrder File
	jPartPlanHeader := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber).getJobPartPlanPointer()
	if jPartPlanHeader.BlobData.MetaDataLength == 0 {
		return azblob.Metadata{}
	}
	var mData azblob.Metadata
	// metaDataString is meta data stored as string in JobPartOrder file
	metaDataString := string(jPartPlanHeader.BlobData.MetaData[:])
	// Split the meta data string using ';' to get key=value pairs
	metaDataKeyValues := strings.Split(metaDataString, ";")
	for index := 0; index < len(metaDataKeyValues); index++ {
		// Splitting each key=value pair to get key and values
		keyValue := strings.Split(metaDataKeyValues[index], "=")
		mData[keyValue[0]] = keyValue[1]
	}
	return mData
}

func (t TransferMsg) ifPreserveLastModifiedTime(resp *azblob.GetResponse){
	jPartPlanInfo := t.infoMap.LoadJobPartPlanInfoForJobPart(t.jobId, t.partNumber)
	if jPartPlanInfo.getJobPartPlanPointer().BlobData.PreserveLastModifiedTime{
		_, dst := jPartPlanInfo.getTransferSrcDstDetail(t.transferIndex)
		lastModifiedTime := resp.LastModified()
		err := os.Chtimes(dst, lastModifiedTime, lastModifiedTime)
		if err != nil {
			t.infoMap.LoadJobInfoForJob(t.jobId).Panic(errors.New(fmt.Sprintf("error changing the modified time of file %s to the time %s", dst, lastModifiedTime.String())))
			return
		}
		t.infoMap.LoadJobInfoForJob(t.jobId).Log(common.LogInfo, fmt.Sprintf("successfully changed the modified time of file %s to the time %s", dst, lastModifiedTime.String()))
	}
}

func setModifiedTime(file string, mTime time.Time, info *JobInfo) {

}
// TransferMsgDetail represents the details of the transfer message received from the transfer channels
type TransferMsgDetail struct {
	// jobId - JobId of job to which the transfer belongs to
	JobId common.JobID

	// partNumber is the part of the Job to which transfer belongs to
	PartNumber common.PartNumber

	// TransferId is the index of transfer in JobPartOrder
	TransferId uint32

	// ChunkSize is the max size a chunk can have in the transfer
	ChunkSize uint64

	SourceType         common.LocationType
	Source             string
	DestinationType    common.LocationType
	Destination        string
	TransferCtx        context.Context
	TransferCancelFunc func()
	NumberOfChunksDone *uint32
	//JobHandlerMap is the pointer to in memory JobsInfoMap
	JobHandlerMap *JobsInfoMap
}

type ChunkMsg struct {
	doTransfer chunkFunc
}

type CoordinatorChannels struct {
	HighTransfer chan<- TransferMsg
	MedTransfer  chan<- TransferMsg
	LowTransfer  chan<- TransferMsg
}

type EEChannels struct {
	HighTransfer         <-chan TransferMsg
	MedTransfer          <-chan TransferMsg
	LowTransfer          <-chan TransferMsg
	HighChunkTransaction chan ChunkMsg
	MedChunkTransaction  chan ChunkMsg
	LowChunkTransaction  chan ChunkMsg
	SuicideChannel       <-chan SuicideJob
}

type SuicideJob byte
type chunkFunc func(int)
type prologueFunc func(msg TransferMsg, chunkChannel chan<- ChunkMsg)

// throughputState struct holds the attribute to monitor the through of an existing JobOrder
type throughputState struct {
	lastCheckedTime  int64
	lastCheckedBytes int64
	currentBytes     int64
}

// getLastCheckedTime api returns the lastCheckedTime of throughputState instance in thread-safe manner
func (t *throughputState) getLastCheckedTime() int64 {
	return atomic.LoadInt64(&t.lastCheckedTime)
}

// updateLastCheckTime api updates the lastCheckedTime of throughputState instance in thread-safe manner
func (t *throughputState) updateLastCheckTime(currentTime int64) {
	atomic.StoreInt64(&t.lastCheckedTime, currentTime)
}

// getLastCheckedBytes api returns the lastCheckedBytes of throughputState instance in thread-safe manner
func (t *throughputState) getLastCheckedBytes() int64 {
	return atomic.LoadInt64(&t.lastCheckedBytes)
}

// updateLastCheckedBytes api updates the lastCheckedBytes of throughputState instance in thread-safe manner
func (t *throughputState) updateLastCheckedBytes(bytes int64) {
	atomic.StoreInt64(&t.lastCheckedBytes, bytes)
}

// getCurrentBytes api returns the currentBytes of throughputState instance in thread-safe manner
func (t *throughputState) getCurrentBytes() int64 {
	return atomic.LoadInt64(&t.currentBytes)
}

// updateCurrentBytes api adds the value in currentBytes of throughputState instance in thread-safe manner
func (t *throughputState) updateCurrentBytes(bytes int64) int64 {
	return atomic.AddInt64(&t.currentBytes, bytes)
}
