package cmd

import (
	"fmt"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
	"math/rand"
	"time"
)

// copyEnumerator is the interface for copy enumerators.
type CopyEnumerator interface {
	// enumerate enumerates entities
	enumerate(sourceURLString string, isRecursiveOn bool, destinationPath string,
		wg *sync.WaitGroup, waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error

	// partNum gets part number
	partNum() common.PartNumber
}

// addTransfer accepts a new transfer, if the threshold is reached, dispatch a job part order.
func addTransfer(e *common.CopyJobPartOrderRequest, transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	// dispatch the transfers once the number reaches NumOfFilesPerDispatchJobPart
	// we do this so that in the case of large transfer, the transfer engine can get started
	// while the frontend is still gathering more transfers
	if len(e.Transfers) == NumOfFilesPerDispatchJobPart {
		shuffleTransfers(e.Transfers)
		resp := common.CopyJobPartOrderResponse{}
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNum == 0 {
			wg.Add(1)
			go waitUntilJobCompletion(e.JobID, wg)
		}
		e.Transfers = []common.CopyTransfer{}
		e.PartNum++
	}

	e.Transfers = append(e.Transfers, transfer)

	return nil
}

// this function shuffles the transfers before they are dispatched
// this is done to avoid hitting the same partition continuously in an append only pattern
// TODO this should probably be removed after the high throughput block blob feature is implemented on the service side
func shuffleTransfers(transfers []common.CopyTransfer) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(transfers), func(i, j int) { transfers[i], transfers[j] = transfers[j], transfers[i] })
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
// dispatchFinalPart sends a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent.
func dispatchFinalPart(e *common.CopyJobPartOrderRequest) error {
	shuffleTransfers(e.Transfers)
	e.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
	}

	return nil
}
