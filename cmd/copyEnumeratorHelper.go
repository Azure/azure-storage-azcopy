package cmd

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"math/rand"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var EnumerationParallelism = 1
var EnumerationParallelStatFiles = false

// addTransfer accepts a new transfer, if the threshold is reached, dispatch a job part order.
func addTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, cca *CookedCopyCmdArgs) error {
	// Remove the source and destination roots from the path to save space in the plan files
	transfer.Source = strings.TrimPrefix(transfer.Source, e.SourceRoot.Value)
	transfer.Destination = strings.TrimPrefix(transfer.Destination, e.DestinationRoot.Value)

	// dispatch the transfers once the number reaches NumOfFilesPerDispatchJobPart
	// we do this so that in the case of large transfer, the transfer engine can get started
	// while the frontend is still gathering more transfers
	if len(e.Transfers.List) == NumOfFilesPerDispatchJobPart {
		shuffleTransfers(e.Transfers.List)
		resp := common.CopyJobPartOrderResponse{}

		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNum == 0 {
			cca.waitUntilJobCompletion(false)
		}
		e.Transfers = common.Transfers{}
		e.PartNum++
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	{
		//Should this block be a function?
		e.Transfers.List = append(e.Transfers.List, transfer)
		e.Transfers.TotalSizeInBytes += uint64(transfer.SourceSize)
		if transfer.EntityType == common.EEntityType.File() {
			e.Transfers.FileTransferCount++
		} else {
			e.Transfers.FolderTransferCount++
		}
	}

	return nil
}

// this function shuffles the transfers before they are dispatched
// this is done to avoid hitting the same partition continuously in an append only pattern
// TODO this should probably be removed after the high throughput block blob feature is implemented on the service side
func shuffleTransfers(transfers []common.CopyTransfer) {
	rand.Shuffle(len(transfers), func(i, j int) { transfers[i], transfers[j] = transfers[j], transfers[i] })
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
// dispatchFinalPart sends a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent.
func dispatchFinalPart(e *common.CopyJobPartOrderRequest, cca *CookedCopyCmdArgs) error {
	shuffleTransfers(e.Transfers.List)
	e.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

	if !resp.JobStarted {
		// Output the log location and such
		glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), fmt.Sprintf("%s%s%s.log", azcopyLogPathFolder, common.OS_PATH_SEPARATOR, cca.jobID), cca.isCleanupJob, cca.cleanupJobMessage))

		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return NothingScheduledError
		}

		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
	}

	if ste.JobsAdmin != nil {
		ste.JobsAdmin.LogToJobLog(FinalPartCreatedMessage, pipeline.LogInfo)
	}

	// set the flag on cca, to indicate the enumeration is done
	cca.isEnumerationComplete = true

	// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
	if e.PartNum == 0 {
		cca.waitUntilJobCompletion(false)
	}
	return nil
}
