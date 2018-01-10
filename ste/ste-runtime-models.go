package ste

import (
	"context"
	"github.com/edsrzf/mmap-go"
	"github.com/Azure/azure-storage-azcopy/common"
)

type TransferInfo struct {
	ctx context.Context
	cancel context.CancelFunc
	NumChunksCompleted uint16
}

type JobPartPlanInfo struct {
	ctx          context.Context
	cancel       context.CancelFunc
	memMap       mmap.MMap
	TrasnferInfo []TransferInfo
}

type TransferMsg struct {
	Id common.JobID
	PartNumber common.PartNumber
	TransferIndex uint32
}

type TransferMsgDetail struct {
	TransferId      uint32
	ChunkSize       uint16
	SourceType      common.LocationType
	Source          string
	DestinationType common.LocationType
	Destination     string
}
type ChunkMsg struct {

}

type TEChannels struct{
	HighTransfer chan <- TransferMsg
	MedTransfer chan <- TransferMsg
	LowTransfer chan <- TransferMsg
}

type EEChannels struct {
	HighTransfer <- chan TransferMsg
	MedTransfer <- chan TransferMsg
	LowTransfer <- chan TransferMsg

	HighChunkTransaction chan ChunkMsg
	MedChunkTransaction chan ChunkMsg
	LowChunkTransaction chan ChunkMsg
}