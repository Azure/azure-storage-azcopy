package ste

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/edsrzf/mmap-go"
	"time"
)

type TransferInfo struct {
	ctx                context.Context
	cancel             context.CancelFunc
	NumChunksCompleted uint16
}

// TODO comments on all struct
type JobPartPlanInfo struct {
	ctx                        context.Context
	cancel                     context.CancelFunc
	fileName                   string
	memMap                     mmap.MMap
	TransferInfo               []TransferInfo
	NumberOfTransfersCompleted uint32
}

type TransferMsg struct {
	Id              common.JobID
	PartNumber      common.PartNumber
	TransferIndex   uint32
	InfoMap         *JobsInfoMap
	TransferContext context.Context
}

type TransferMsgDetail struct {
	JobId              common.JobID
	PartNumber         common.PartNumber
	TransferId         uint32
	ChunkSize          uint64
	SourceType         common.LocationType
	Source             string
	DestinationType    common.LocationType
	Destination        string
	TransferCtx        context.Context
	TransferCancelFunc func()
	JobHandlerMap      *JobsInfoMap
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

type throughputState struct {
	lastCheckedTime  time.Time
	lastCheckedBytes int64
	currentBytes     int64
}
