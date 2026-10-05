package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// Compatibility adapter for callers which still construct a command processor.
type copyTransferProcessor struct {
	*azcopy.CopyTransferProcessor
	copyJobTemplate *common.CopyJobPartOrderRequest
}

func newCopyTransferProcessor(template *common.CopyJobPartOrderRequest, count int, source, destination common.ResourceString, first func(bool), final func(), preserveAccessTier, dryrun bool) *copyTransferProcessor {
	return &copyTransferProcessor{
		CopyTransferProcessor: azcopy.NewCopyTransferProcessor(false, template, count, source, destination, first, final, preserveAccessTier, dryrun, dryrunNewCopyJobPartOrder),
		copyJobTemplate:       template,
	}
}

func (s *copyTransferProcessor) scheduleCopyTransfer(object traverser.StoredObject) error {
	return s.ScheduleSyncRemoveSetPropertiesTransfer(object)
}

func (s *copyTransferProcessor) dispatchFinalPart() (bool, error) {
	return s.DispatchFinalPart()
}

var NothingScheduledError = azcopy.NothingScheduledError
var FinalPartCreatedMessage = azcopy.FinalPartCreatedMessage
