package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func copyCommandProcessor(order *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) *azcopy.CopyTransferProcessor {
	if cca.compatibilityProcessor != nil && cca.compatibilityOrder == order {
		return cca.compatibilityProcessor
	}
	first := func(started bool) {
		if started && !cca.dryrunMode {
			cca.waitUntilJobCompletion(false)
		}
	}
	final := func() { cca.isEnumerationComplete = true }
	order.JobProcessingMode = azcopy.GetJobProcessingMode(cca.FromTo)
	order.HardlinkHandlingType = cca.hardlinks
	cca.compatibilityOrder = order
	cca.compatibilityProcessor = azcopy.NewCopyTransferProcessor(true, order, azcopy.NumOfFilesPerDispatchJobPart,
		cca.Source, cca.Destination, first, final, cca.s2sPreserveAccessTier.Value(),
		cca.dryrunMode, dryrunNewCopyJobPartOrder)
	return cca.compatibilityProcessor
}

func addTransfer(order *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *CookedCopyCmdArgs) error {
	return copyCommandProcessor(order, cca).ScheduleTransfer(transfer)
}

func dispatchFinalPart(order *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {
	_, err := copyCommandProcessor(order, cca).DispatchFinalPart()
	return err
}
