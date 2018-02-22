package ste

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/edsrzf/mmap-go"
	"sync/atomic"
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
	NumberOfChunksDone uint16
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
	// Id - JobId of job to which the transfer belongs to
	Id common.JobID
	// PartNumber is the part of the Job to which transfer belongs to
	PartNumber common.PartNumber
	// TransferIndex is the index of transfer in JobPartOrder
	TransferIndex uint32
	// InfoMap is the pointer to in memory JobsInfoMap
	InfoMap *JobsInfoMap

	// TransferContext is the context of transfer to be scheduled
	TransferContext context.Context
}

// TransferMsgDetail represents the details of the transfer message received from the transfer channels
type TransferMsgDetail struct {
	// Id - JobId of job to which the transfer belongs to
	JobId common.JobID

	// PartNumber is the part of the Job to which transfer belongs to
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
type prologueFunc func(msg TransferMsgDetail, chunkChannel chan<- ChunkMsg)

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
