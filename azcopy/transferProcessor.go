package azcopy

import "github.com/Azure/azure-storage-azcopy/v10/common"

type TransferProcessingJob interface {
	OnFirstPartDispatched()
	OnLastPartDispatched()
}

type transferProcessor struct {
	numberOfTransfersPerPart int // number of transfers grouped before sending to ste as a part
	copyJobTemplate          *common.CopyJobPartOrderRequest
	source                   common.ResourceString
	destination              common.ResourceString
}
