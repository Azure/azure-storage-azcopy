package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func copyCommandProcessor(order *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) *azcopy.CopyTransferProcessor {
	first := func(started bool) {
		if started && !cca.dryrunMode {
			cca.waitUntilJobCompletion(false)
		}
	}
	final := func() { cca.isEnumerationComplete = true }
	return azcopy.NewCopyTransferProcessor(true, order, azcopy.NumOfFilesPerDispatchJobPart,
		cca.Source, cca.Destination, first, final, cca.s2sPreserveAccessTier.Value(),
		cca.dryrunMode, dryrunNewCopyJobPartOrder)
}

func addTransfer(order *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *CookedCopyCmdArgs) error {
	return copyCommandProcessor(order, cca).ScheduleTransfer(transfer)
}

func dispatchFinalPart(order *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {
	_, err := copyCommandProcessor(order, cca).DispatchFinalPart()
	return err
}
