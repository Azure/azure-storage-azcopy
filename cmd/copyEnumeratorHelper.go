package cmd

import (
	"fmt"
	"math/rand"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type CopyJobPartDispatcher struct {
	PendingTransfers          common.Transfers
	PendingHardlinksTransfers common.Transfers
}

func (d *CopyJobPartDispatcher) readyForDispatch() bool {
	return (len(d.PendingTransfers.List) == azcopy.NumOfFilesPerDispatchJobPart) ||
		(len(d.PendingHardlinksTransfers.List) == azcopy.NumOfFilesPerDispatchJobPart)
}

func (d *CopyJobPartDispatcher) appendTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer) error {
	if e.JobProcessingMode == common.EJobProcessingMode.NFS() &&
		transfer.EntityType == common.EEntityType.Hardlink() &&
		e.HardlinkHandlingType == common.EHardlinkHandlingType.Preserve() {

		d.PendingHardlinksTransfers.List = append(d.PendingHardlinksTransfers.List, transfer)
		d.PendingHardlinksTransfers.TotalSizeInBytes += uint64(transfer.SourceSize)
		d.PendingHardlinksTransfers.HardlinksTransferCount++

	} else {
		d.PendingTransfers.List = append(d.PendingTransfers.List, transfer)
		d.PendingTransfers.TotalSizeInBytes += uint64(transfer.SourceSize)
		switch transfer.EntityType {
		case common.EEntityType.File():
			d.PendingTransfers.FileTransferCount++
		case common.EEntityType.Folder():
			d.PendingTransfers.FolderTransferCount++
		case common.EEntityType.Symlink():
			d.PendingTransfers.SymlinkTransferCount++
		case common.EEntityType.Hardlink():
			d.PendingTransfers.HardlinksConvertedCount++
		}
	}

	return nil
}

// addTransfer accepts a new transfer, if the threshold is reached, dispatch a job part order.
func (d *CopyJobPartDispatcher) addTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *CookedCopyCmdArgs) error {
	// Source and destination paths are and should be relative paths.

	if d.readyForDispatch() {

		if e.JobProcessingMode == common.EJobProcessingMode.NFS() {
			if len(d.PendingTransfers.List) == azcopy.NumOfFilesPerDispatchJobPart {
				e.Transfers = d.PendingTransfers.Clone()
				e.JobPartType = common.EJobPartType.Mixed()
				d.dispatchPart(e, cca)
				d.PendingTransfers = common.Transfers{}
			}

			if len(d.PendingHardlinksTransfers.List) == azcopy.NumOfFilesPerDispatchJobPart {
				e.Transfers = d.PendingHardlinksTransfers.Clone()
				e.JobPartType = common.EJobPartType.Hardlink()
				d.dispatchPart(e, cca)
				d.PendingHardlinksTransfers = common.Transfers{}
			}
		} else {
			if len(d.PendingTransfers.List) == azcopy.NumOfFilesPerDispatchJobPart {
				e.Transfers = d.PendingTransfers.Clone()
				e.JobPartType = common.EJobPartType.Mixed()
				d.dispatchPart(e, cca)
				d.PendingTransfers = common.Transfers{}
			}
		}
	}
	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	d.appendTransfer(e, transfer)

	return nil
}

// dispatch the transfers once the number reaches NumOfFilesPerDispatchJobPart
// we do this so that in the case of large transfer, the transfer engine can get started
// while the frontend is still gathering more transfers
func (d *CopyJobPartDispatcher) dispatchPart(e *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {
	d.shuffleTransfers(e.Transfers.List)
	resp := jobsAdmin.ExecuteNewCopyJobPartOrder(*e)

	if !resp.JobStarted {
		return fmt.Errorf(
			"copy job part order with JobId %s, part number %d, transfer type %s, and transfer count %d failed because %s",
			e.JobID, e.PartNum, e.JobPartType, len(e.Transfers.List), resp.ErrorMsg)
	}
	// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
	if e.PartNum == 0 {
		cca.waitUntilJobCompletion(false)
	}
	e.Transfers = common.Transfers{}
	e.PartNum++
	return nil
}

// addTransfer accepts a new transfer, if the threshold is reached, dispatch a job part order.
// func addTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *CookedCopyCmdArgs) error {
// 	// Source and destination paths are and should be relative paths.

// 	// dispatch the transfers once the number reaches NumOfFilesPerDispatchJobPart
// 	// we do this so that in the case of large transfer, the transfer engine can get started
// 	// while the frontend is still gathering more transfers
// 	if len(e.Transfers.List) == azcopy.NumOfFilesPerDispatchJobPart {
// 		shuffleTransfers(e.Transfers.List)
// 		resp := jobsAdmin.ExecuteNewCopyJobPartOrder(*e)

// 		if !resp.JobStarted {
// 			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
// 		}
// 		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
// 		if e.PartNum == 0 {
// 			cca.waitUntilJobCompletion(false)
// 		}
// 		e.Transfers = common.Transfers{}
// 		e.PartNum++
// 	}

// 	// only append the transfer after we've checked and dispatched a part
// 	// so that there is at least one transfer for the final part
// 	{
// 		// Should this block be a function?
// 		e.Transfers.List = append(e.Transfers.List, transfer)
// 		e.Transfers.TotalSizeInBytes += uint64(transfer.SourceSize)
// 		switch transfer.EntityType {
// 		case common.EEntityType.File():
// 			e.Transfers.FileTransferCount++
// 		case common.EEntityType.Folder():
// 			e.Transfers.FolderTransferCount++
// 		case common.EEntityType.Symlink():
// 			e.Transfers.SymlinkTransferCount++
// 		case common.EEntityType.Hardlink():
// 			e.Transfers.HardlinksConvertedCount++
// 		}
// 	}

// 	return nil
// }

// this function shuffles the transfers before they are dispatched
// this is done to avoid hitting the same partition continuously in an append only pattern
// TODO this should probably be removed after the high throughput block blob feature is implemented on the service side
func (d *CopyJobPartDispatcher) shuffleTransfers(transfers []common.CopyTransfer) {
	rand.Shuffle(len(transfers), func(i, j int) { transfers[i], transfers[j] = transfers[j], transfers[i] })
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
// dispatchFinalPart sends a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent.
func (d *CopyJobPartDispatcher) dispatchFinalPart(e *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {

	if e.JobProcessingMode == common.EJobProcessingMode.NFS() {
		if len(d.PendingTransfers.List) > 0 && len(d.PendingHardlinksTransfers.List) > 0 {
			// if there are both kinds of transfers pending, first do the filmmixed transfers
			e.Transfers = d.PendingTransfers.Clone()
			e.JobPartType = common.EJobPartType.Mixed()
			d.dispatchPart(e, cca)
			d.PendingTransfers = common.Transfers{}
		}

		// Either file or folder transfers are pending. Whatever is pending will be the final part.
		if len(d.PendingTransfers.List) > 0 {
			e.Transfers = d.PendingTransfers.Clone()
			e.JobPartType = common.EJobPartType.Mixed()
		} else if len(d.PendingHardlinksTransfers.List) > 0 {
			e.Transfers = d.PendingHardlinksTransfers.Clone()
			e.JobPartType = common.EJobPartType.Hardlink()
		}
	} else {
		if len(d.PendingTransfers.List) > 0 {
			e.Transfers = d.PendingTransfers.Clone()
			e.JobPartType = common.EJobPartType.Mixed()
		}
	}

	d.shuffleTransfers(e.Transfers.List)
	e.IsFinalPart = true
	resp := jobsAdmin.ExecuteNewCopyJobPartOrder(*e)

	if !resp.JobStarted {
		// Output the log location if log-level is set to other then NONE
		var logPathFolder string
		if common.LogPathFolder != "" {
			logPathFolder = fmt.Sprintf("%s%s%s.log", common.LogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID)
		}
		glcm.Init(GetStandardInitOutputBuilder(cca.jobID.String(), logPathFolder, cca.isCleanupJob, cca.cleanupJobMessage))

		if cca.dryrunMode {
			return nil
		}

		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return azcopy.NothingScheduledError
		}

		return fmt.Errorf(
			"copy job part order with JobId %s, part number %d, transfer type %s, and transfer count %d failed because %s",
			e.JobID, e.PartNum, e.JobPartType, len(e.Transfers.List), resp.ErrorMsg)

	}

	common.LogToJobLogWithPrefix(azcopy.FinalPartCreatedMessage, common.LogInfo)

	// set the flag on cca, to indicate the enumeration is done
	cca.isEnumerationComplete = true

	// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
	if e.PartNum == 0 {
		cca.waitUntilJobCompletion(false)
	}
	return nil
}
