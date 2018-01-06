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
