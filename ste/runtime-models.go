package ste

import (
	"context"
	"sync/atomic"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"os"
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

func (t *TransferInfo) getNumberOfChunksDone() uint32 {
	return atomic.LoadUint32(&t.NumberOfChunksDone)
}

func (t *TransferInfo) ChunksDone() uint32 {
	return atomic.AddUint32(&t.NumberOfChunksDone, 1)
}

// JobPartPlanInfo represents the runtime information of a JobPartOrder
type JobPartPlanInfo struct {

	// the context of a JobPartOrder
	// All the parts of Job share same context
	ctx context.Context

	// cancel func is the func to be called to cancel a Job
	cancel context.CancelFunc

	// file is the file descriptor for JobPartOrder file
	file *os.File

	// memMap represents the memory map byte slice
	memMap handlers.MMap

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

// TransfersDone increment the numberOfTransfersDone_doNotUse of JobPartPlanInfo
// instance in thread safe manner by 1
func (jPartPlanInfo *JobPartPlanInfo) TransfersDone() uint32 {
	return atomic.AddUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, 1)
}

// setNumberOfTransfersDone sets the number of transfers done to a specific value
// in a thread safe manner
func (jPartPlanInfo *JobPartPlanInfo) setNumberOfTransfersDone(val uint32) {
	atomic.StoreUint32(&jPartPlanInfo.numberOfTransfersDone_doNotUse, val)
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
	HighTransfer   <-chan TransferMsg
	LowTransfer    <-chan TransferMsg
	HighChunk      chan ChunkMsg
	LowChunk       chan ChunkMsg
	SuicideChannel <-chan SuicideJob
}

type SuicideJob byte
type chunkFunc func(int)
type xferFactory func(transfer *TransferMsg, pacer *pacer) xfer
type xfer interface {
	runPrologue(chunkChannel chan<- ChunkMsg)
}

// ThroughputState struct holds the attribute to monitor the through of an existing JobOrder
type ThroughputState struct {
	lastCheckedTime  int64
	lastCheckedBytes int64
	currentBytes     int64
}

// getLastCheckedTime api returns the lastCheckedTime of ThroughputState instance in thread-safe manner
func (t *ThroughputState) getLastCheckedTime() int64 {
	return atomic.LoadInt64(&t.lastCheckedTime)
}

// updateLastCheckTime api updates the lastCheckedTime of ThroughputState instance in thread-safe manner
func (t *ThroughputState) updateLastCheckTime(currentTime int64) {
	atomic.StoreInt64(&t.lastCheckedTime, currentTime)
}

// getLastCheckedBytes api returns the lastCheckedBytes of ThroughputState instance in thread-safe manner
func (t *ThroughputState) getLastCheckedBytes() int64 {
	return atomic.LoadInt64(&t.lastCheckedBytes)
}

// updateLastCheckedBytes api updates the lastCheckedBytes of ThroughputState instance in thread-safe manner
func (t *ThroughputState) updateLastCheckedBytes(bytes int64) {
	atomic.StoreInt64(&t.lastCheckedBytes, bytes)
}

// getCurrentBytes api returns the currentBytes of ThroughputState instance in thread-safe manner
func (t *ThroughputState) getCurrentBytes() int64 {
	return atomic.LoadInt64(&t.currentBytes)
}

// updateCurrentBytes api adds the value in currentBytes of ThroughputState instance in thread-safe manner
func (t *ThroughputState) updateCurrentBytes(bytes int64) int64 {
	return atomic.AddInt64(&t.currentBytes, bytes)
}
