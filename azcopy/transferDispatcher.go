package azcopy

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

const NumOfFilesPerDispatchJobPart = 10000

type TransferDispatchObserver interface {
	OnFirstPartDispatched()
	OnLastPartDispatched()
}

// transferDispatcher is responsible for dispatching transfers to STE in parts.
type transferDispatcher struct {
	transfersPerPart int // number of transfers grouped before sending to ste as a part
	template         *common.CopyJobPartOrderRequest
	source           common.ResourceString
	destination      common.ResourceString

	observer TransferDispatchObserver

	preserveAccessTier bool

	// TODO: dryrun will be implemented by overriding the jobsAdmin.ExecuteNewCopyJobPartOrder function
}

func newTransferDispatcher(transfersPerPart int, template *common.CopyJobPartOrderRequest,
	source, destination common.ResourceString, observer TransferDispatchObserver, preserveAccessTier bool) *transferDispatcher {
	return &transferDispatcher{
		transfersPerPart:   transfersPerPart,
		template:           template,
		source:             source,
		destination:        destination,
		observer:           observer,
		preserveAccessTier: preserveAccessTier,
	}
}

func (td *transferDispatcher) scheduleTransfer(storedObject traverser.StoredObject) error {

	// TODO : see if we can make a filter for this instead of checking it here.
	// Check if this transfer should be skipped
	if !storedObject.IsCompatibleWithEntitySettings(td.template.Fpo, td.template.SymlinkHandlingType, td.template.HardlinkHandlingType) {
		return nil
	}

	// Check if we have accumulated enough transfers to dispatch a part
	if len(td.template.Transfers.List) == td.transfersPerPart {
		resp := td.dispatchTransfers()
		if resp.ErrorMsg != "" {
			return errors.New(string(resp.ErrorMsg))
		}

		// reset the transfers list and increment part number
		td.template.Transfers = common.Transfers{}
		td.template.PartNum++
	}

	// Escape paths on destinations where the characters are invalid
	// And re-encode them where the characters are valid.
	var srcRelativePath, dstRelativePath string
	if storedObject.RelativePath == "\x00" { // Short circuit when we're talking about root/, because the STE is funky about this.
		srcRelativePath, dstRelativePath = storedObject.RelativePath, storedObject.RelativePath
	} else {
		srcRelativePath = PathEncodeRules(storedObject.RelativePath, td.template.FromTo, false, true)
		dstRelativePath = PathEncodeRules(storedObject.RelativePath, td.template.FromTo, false, false)
		if srcRelativePath != "" {
			srcRelativePath = "/" + srcRelativePath
		}
		if dstRelativePath != "" {
			dstRelativePath = "/" + dstRelativePath
		}
	}

	copyTransfer, _ := storedObject.ToNewCopyTransfer(false, srcRelativePath, dstRelativePath, td.preserveAccessTier, td.template.Fpo, td.template.SymlinkHandlingType, td.template.HardlinkHandlingType)

	// set-properties
	if td.template.FromTo.To() == common.ELocation.None() {
		copyTransfer.BlobTier = td.template.BlobAttributes.BlockBlobTier.ToAccessTierType()

		metadataString := td.template.BlobAttributes.Metadata
		metadataMap := common.Metadata{}
		if len(metadataString) > 0 {
			for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
				kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
				metadataMap[kv[0]] = &kv[1]
			}
		}
		copyTransfer.Metadata = metadataMap

		copyTransfer.BlobTags = common.ToCommonBlobTagsMap(td.template.BlobAttributes.BlobTagsString)
	}

	// Append the new transfer
	// Only append after we've checked and dispatched a part so that there is at least one transfer for the final part
	td.template.Transfers.List = append(td.template.Transfers.List, copyTransfer)
	td.template.Transfers.TotalSizeInBytes += uint64(copyTransfer.SourceSize)

	switch copyTransfer.EntityType {
	case common.EEntityType.File():
		td.template.Transfers.FileTransferCount++
	case common.EEntityType.Folder():
		td.template.Transfers.FolderTransferCount++
	case common.EEntityType.Symlink():
		td.template.Transfers.SymlinkTransferCount++
	case common.EEntityType.Hardlink():
		td.template.Transfers.HardlinksConvertedCount++
	}

	return nil
}

func (td *transferDispatcher) dispatchTransfers() common.CopyJobPartOrderResponse {
	resp := jobsAdmin.ExecuteNewCopyJobPartOrder(*td.template)

	// Update the observer on the first part dispatched
	if td.template.PartNum == 0 && td.observer != nil {
		td.observer.OnFirstPartDispatched()
	}

	return resp
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")

const FinalPartCreatedMessage = "Final job part has been created"

func (td *transferDispatcher) dispatchFinalPart() (bool, error) {
	td.template.IsFinalPart = true
	resp := td.dispatchTransfers()

	if !resp.JobStarted {
		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return false, NothingScheduledError
		}
		return false, fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s",
			td.template.JobID, td.template.PartNum, resp.ErrorMsg)
	}

	common.LogToJobLogWithPrefix(FinalPartCreatedMessage, common.LogInfo)

	// Notify the observer that the last part has been dispatched
	if td.observer != nil {
		td.observer.OnLastPartDispatched()
	}
	return true, nil
}
