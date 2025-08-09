package cmd

import (
	"fmt"
	"math/rand"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

var EnumerationParallelism = 1
var EnumerationParallelStatFiles = false

type copyJobPartDispatcher struct {
	PendingTransfers       common.Transfers
	PendingFolderTransfers common.Transfers
}

func (d *copyJobPartDispatcher) readyForDispatch() bool {
	return (len(d.PendingTransfers.List) == NumOfFilesPerDispatchJobPart) ||
		(len(d.PendingFolderTransfers.List) == NumOfFilesPerDispatchJobPart)
}

func (d *copyJobPartDispatcher) appendTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer) error {

	if e.JobProcessingMode == common.EJobProcessingMode.FolderAfterFiles() &&
		transfer.EntityType == common.EEntityType.Folder() {
		d.PendingFolderTransfers.List = append(d.PendingFolderTransfers.List, transfer)
		d.PendingFolderTransfers.TotalSizeInBytes += uint64(transfer.SourceSize)
		d.PendingFolderTransfers.FolderTransferCount++
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
		case common.EEntityType.FileProperties():
			d.PendingTransfers.FilePropertyTransferCount++
		}
	}

	return nil
}

// addTransfer accepts a new transfer, if the threshold is reached, dispatch a job part order.
func (d *copyJobPartDispatcher) addTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *CookedCopyCmdArgs) error {
	// Source and destination paths are and should be relative paths.

	if d.readyForDispatch() {

		if e.JobProcessingMode == common.EJobProcessingMode.FolderAfterFiles() {
			if len(d.PendingTransfers.List) == NumOfFilesPerDispatchJobPart {
				e.Transfers = d.PendingTransfers.Clone()
				e.JobPartType = common.EJobPartType.Files()
				d.dispatchPart(e, cca)
				d.PendingTransfers = common.Transfers{}
			}

			if len(d.PendingFolderTransfers.List) == NumOfFilesPerDispatchJobPart {
				e.Transfers = d.PendingFolderTransfers.Clone()
				e.JobPartType = common.EJobPartType.Folders()
				d.dispatchPart(e, cca)
				d.PendingFolderTransfers = common.Transfers{}
			}
		} else {
			if len(d.PendingTransfers.List) == NumOfFilesPerDispatchJobPart {
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

// this function shuffles the transfers before they are dispatched
// this is done to avoid hitting the same partition continuously in an append only pattern
// TODO this should probably be removed after the high throughput block blob feature is implemented on the service side
func (d *copyJobPartDispatcher) shuffleTransfers(transfers []common.CopyTransfer) {
	rand.Shuffle(len(transfers), func(i, j int) { transfers[i], transfers[j] = transfers[j], transfers[i] })
}

// dispatch the transfers once the number reaches NumOfFilesPerDispatchJobPart
// we do this so that in the case of large transfer, the transfer engine can get started
// while the frontend is still gathering more transfers
func (d *copyJobPartDispatcher) dispatchPart(e *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {
	d.shuffleTransfers(e.Transfers.List)
	resp := common.CopyJobPartOrderResponse{}

	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

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

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
// dispatchFinalPart sends a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent.
func (d *copyJobPartDispatcher) dispatchFinalPart(e *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {

	if e.JobProcessingMode == common.EJobProcessingMode.FolderAfterFiles() {
		if len(d.PendingTransfers.List) > 0 && len(d.PendingFolderTransfers.List) > 0 {
			// if there are both kinds of transfers pending, first do the file transfers
			e.Transfers = d.PendingTransfers.Clone()
			e.JobPartType = common.EJobPartType.Files()
			d.dispatchPart(e, cca)
			d.PendingTransfers = common.Transfers{}
		}

		// Either file or folder transfers are pending. Whatever is pending will be the final part.
		if len(d.PendingTransfers.List) > 0 {
			e.Transfers = d.PendingTransfers.Clone()
			e.JobPartType = common.EJobPartType.Files()
		} else if len(d.PendingFolderTransfers.List) > 0 {
			e.Transfers = d.PendingFolderTransfers.Clone()
			e.JobPartType = common.EJobPartType.Folders()
		}
	} else {
		if len(d.PendingTransfers.List) > 0 {
			e.Transfers = d.PendingTransfers.Clone()
			e.JobPartType = common.EJobPartType.Mixed()
		}
	}

	d.shuffleTransfers(e.Transfers.List)
	e.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

	if !resp.JobStarted {
		// Output the log location if log-level is set to other then NONE
		var logPathFolder string
		if azcopyLogPathFolder != "" {
			logPathFolder = fmt.Sprintf("%s%s%s.log", azcopyLogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID)
		}
		glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), logPathFolder, cca.isCleanupJob, cca.cleanupJobMessage))

		if cca.dryrunMode {
			return nil
		}

		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return NothingScheduledError
		}

		return fmt.Errorf(
			"copy job part order with JobId %s, part number %d, transfer type %s, and transfer count %d failed because %s",
			e.JobID, e.PartNum, e.JobPartType, len(e.Transfers.List), resp.ErrorMsg)
	}

	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(FinalPartCreatedMessage, common.LogInfo)
	}

	// set the flag on cca, to indicate the enumeration is done
	cca.isEnumerationComplete = true

	// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
	if e.PartNum == 0 {
		cca.waitUntilJobCompletion(false)
	}
	return nil
}
