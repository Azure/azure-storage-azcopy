// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package azcopy

import (
	"fmt"
	"math/rand"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type syncJobPartDispatcher struct {
	PendingTransfers          common.Transfers
	PendingHardlinksTransfers common.Transfers
}

const NumOfFilesPerDispatchJobPart = 10000

type CopyTransferProcessor struct {
	numOfTransfersPerPart int
	CopyJobTemplate       *common.CopyJobPartOrderRequest
	isCopy                bool
	source                common.ResourceString
	destination           common.ResourceString

	// handles for progress tracking
	reportFirstPartDispatched func(jobStarted bool)
	reportFinalPartDispatched func()

	preserveAccessTier     bool
	folderPropertiesOption common.FolderPropertyOption
	symlinkHandlingType    common.SymlinkHandlingType
	hardlinkHandlingType   common.HardlinkHandlingType

	dryrunMode                bool
	dryrunJobPartOrderHandler func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	// Separate tracking for files/folder/symlink and hardlink based on processing mode
	dispatcher     syncJobPartDispatcher
	processingMode common.JobProcessingMode
}

func NewCopyTransferProcessor(isCopy bool, copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int, source, destination common.ResourceString, reportFirstPartDispatched func(bool), reportFinalPartDispatched func(), preserveAccessTier bool, dryrun bool, dryrunJobPartOrderHandler func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse) *CopyTransferProcessor {
	processor := &CopyTransferProcessor{
		numOfTransfersPerPart:     numOfTransfersPerPart,
		CopyJobTemplate:           copyJobTemplate,
		isCopy:                    isCopy,
		source:                    source,
		destination:               destination,
		reportFirstPartDispatched: reportFirstPartDispatched,
		reportFinalPartDispatched: reportFinalPartDispatched,
		preserveAccessTier:        preserveAccessTier,
		folderPropertiesOption:    copyJobTemplate.Fpo,
		symlinkHandlingType:       copyJobTemplate.SymlinkHandlingType,
		dryrunMode:                dryrun,
		dryrunJobPartOrderHandler: dryrunJobPartOrderHandler,
		processingMode:            copyJobTemplate.JobProcessingMode,
		dispatcher: syncJobPartDispatcher{
			PendingTransfers:          common.Transfers{},
			PendingHardlinksTransfers: common.Transfers{},
		},
	}
	return processor
}

func (s *CopyTransferProcessor) ScheduleSyncRemoveSetPropertiesTransfer(storedObject traverser.StoredObject) (err error) {
	// Escape paths on destinations where the characters are invalid
	// And re-encode them where the characters are valid.
	var srcRelativePath, dstRelativePath string
	if storedObject.RelativePath == "\x00" { // Short circuit when we're talking about root/, because the STE is funky about this.
		srcRelativePath, dstRelativePath = storedObject.RelativePath, storedObject.RelativePath
	} else {
		srcRelativePath = PathEncodeRules(storedObject.RelativePath, s.CopyJobTemplate.FromTo, false, true)
		dstRelativePath = PathEncodeRules(storedObject.RelativePath, s.CopyJobTemplate.FromTo, false, false)
		if srcRelativePath != "" {
			srcRelativePath = "/" + srcRelativePath
		}
		if dstRelativePath != "" {
			dstRelativePath = "/" + dstRelativePath
		}
	}

	return s.scheduleTransfer(srcRelativePath, dstRelativePath, storedObject)
}

func (s *CopyTransferProcessor) scheduleTransfer(srcRelativePath, dstRelativePath string, storedObject traverser.StoredObject) (err error) {
	copyTransfer, shouldSendToSte := storedObject.ToNewCopyTransfer(false, srcRelativePath, dstRelativePath, s.preserveAccessTier, s.folderPropertiesOption, s.symlinkHandlingType, s.hardlinkHandlingType)

	// set properties specific code
	if s.CopyJobTemplate.FromTo.To() == common.ELocation.None() {
		copyTransfer.BlobTier = s.CopyJobTemplate.BlobAttributes.BlockBlobTier.ToAccessTierType()

		metadataString := s.CopyJobTemplate.BlobAttributes.Metadata
		metadataMap := common.Metadata{}
		if len(metadataString) > 0 {
			// Use the proper metadata parsing function that handles escaped semicolons
			parsedMetadata, err := common.StringToMetadata(metadataString)
			if err != nil {
				return fmt.Errorf("invalid metadata format: %w", err)
			}
			metadataMap = parsedMetadata
		}
		copyTransfer.Metadata = metadataMap

		copyTransfer.BlobTags = common.ToCommonBlobTagsMap(s.CopyJobTemplate.BlobAttributes.BlobTagsString)
	}

	// copy specific code
	if s.isCopy && !s.CopyJobTemplate.S2SPreserveBlobTags {
		copyTransfer.BlobTags = common.ToCommonBlobTagsMap(s.CopyJobTemplate.BlobAttributes.BlobTagsString)
	}

	if !shouldSendToSte {
		return nil // skip this one
	}

	//added
	s.dispatchPartIfReady()

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.appendTransfer(copyTransfer)

	return nil
}

func (s *CopyTransferProcessor) readyForDispatch() bool {
	return (len(s.dispatcher.PendingTransfers.List) == s.numOfTransfersPerPart) ||
		(len(s.dispatcher.PendingHardlinksTransfers.List) == s.numOfTransfersPerPart)
}

func (s *CopyTransferProcessor) dispatchPartIfReady() error {
	if !s.readyForDispatch() {
		return nil
	}

	var err error
	if len(s.dispatcher.PendingTransfers.List) == s.numOfTransfersPerPart {
		s.CopyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
		err = s.dispatchPart()
		if err != nil {
			return err
		}
		s.dispatcher.PendingTransfers = common.Transfers{}
	}

	if len(s.dispatcher.PendingHardlinksTransfers.List) == s.numOfTransfersPerPart {
		s.CopyJobTemplate.Transfers = s.dispatcher.PendingHardlinksTransfers.Clone()
		err = s.dispatchPart()
		if err != nil {
			return err
		}
		s.dispatcher.PendingHardlinksTransfers = common.Transfers{}
	}

	return err
}

func (s *CopyTransferProcessor) dispatchPart() error {
	resp := s.sendPartToSte()

	// TODO: If we ever do launch errors outside of the final "no transfers" error, make them output nicer things here.
	if resp.ErrorMsg != "" {
		return errors.New(string(resp.ErrorMsg))
	}

	// reset the transfers buffer
	s.CopyJobTemplate.Transfers = common.Transfers{}
	s.CopyJobTemplate.PartNum++

	return nil
}

func (s *CopyTransferProcessor) appendTransfer(copyTransfer common.CopyTransfer) error {

	if s.processingMode == common.EJobProcessingMode.NFS() &&
		copyTransfer.EntityType == common.EEntityType.Hardlink() {
		s.dispatcher.PendingHardlinksTransfers.List = append(s.dispatcher.PendingHardlinksTransfers.List, copyTransfer)
		// Note: Check on this. Do e need to apped the size for hardlink files. As we will only be creating the hardlink
		s.dispatcher.PendingHardlinksTransfers.TotalSizeInBytes += uint64(copyTransfer.SourceSize)
		if s.hardlinkHandlingType == common.EHardlinkHandlingType.Preserve() {
			s.dispatcher.PendingHardlinksTransfers.HardlinksTransferCount++
		} else if s.hardlinkHandlingType == common.EHardlinkHandlingType.Follow() {
			s.dispatcher.PendingHardlinksTransfers.HardlinksConvertedCount++
		}
	} else {

		s.dispatcher.PendingTransfers.List = append(s.dispatcher.PendingTransfers.List, copyTransfer)
		s.dispatcher.PendingTransfers.TotalSizeInBytes += uint64(copyTransfer.SourceSize)

		switch copyTransfer.EntityType {
		case common.EEntityType.File():
			s.dispatcher.PendingTransfers.FileTransferCount++
		case common.EEntityType.Folder():
			s.dispatcher.PendingTransfers.FolderTransferCount++
		case common.EEntityType.Symlink():
			s.dispatcher.PendingTransfers.SymlinkTransferCount++
		case common.EEntityType.Hardlink():
			s.dispatcher.PendingTransfers.HardlinksConvertedCount++
		}
	}

	return nil
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")
var FinalPartCreatedMessage = "Final job part has been created"

func (s *CopyTransferProcessor) DispatchFinalPart() (copyJobInitiated bool, err error) {
	// Handle separate batch mode with remaining file and folder batches
	if s.processingMode == common.EJobProcessingMode.NFS() {
		if len(s.dispatcher.PendingTransfers.List) > 0 && len(s.dispatcher.PendingHardlinksTransfers.List) > 0 {
			// if there are both kinds of transfers pending, first do the file transfers
			s.CopyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
			err = s.dispatchPart()
			if err != nil {
				return false, fmt.Errorf("failed to send final file job part with job Id %s and part number %d: %s",
					s.CopyJobTemplate.JobID, s.CopyJobTemplate.PartNum, err.Error())
			}
			s.dispatcher.PendingTransfers = common.Transfers{}
		}

		// Either file or folder transfers are pending. Whatever is pending will be the final part.
		if len(s.dispatcher.PendingTransfers.List) > 0 {
			s.CopyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
		} else if len(s.dispatcher.PendingHardlinksTransfers.List) > 0 {
			s.CopyJobTemplate.JobPartType = common.EJobPartType.Hardlink()
			s.CopyJobTemplate.Transfers = s.dispatcher.PendingHardlinksTransfers.Clone()
		}
	} else {
		if len(s.dispatcher.PendingTransfers.List) > 0 {
			s.CopyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
		}
	}

	var resp common.CopyJobPartOrderResponse
	s.CopyJobTemplate.IsFinalPart = true
	resp = s.sendPartToSte()

	if !resp.JobStarted {
		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return false, NothingScheduledError
		}

		return false, fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s",
			s.CopyJobTemplate.JobID, s.CopyJobTemplate.PartNum, resp.ErrorMsg)
	}

	common.LogToJobLogWithPrefix(FinalPartCreatedMessage, common.LogInfo)

	if s.reportFinalPartDispatched != nil {
		s.reportFinalPartDispatched()
	}
	return true, nil
}

// only test the response on the final dispatch to help diagnose root cause of test failures from 0 transfers
func (s *CopyTransferProcessor) sendPartToSte() (resp common.CopyJobPartOrderResponse) {
	if s.dryrunMode {
		resp = s.dryrunJobPartOrderHandler(*s.CopyJobTemplate)
	} else {
		if s.isCopy {
			shuffleTransfers(s.CopyJobTemplate.Transfers.List)
		}
		resp = jobsAdmin.ExecuteNewCopyJobPartOrder(*s.CopyJobTemplate)
	}

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.CopyJobTemplate.PartNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched(resp.JobStarted)
	}

	return resp
}

// shuffleTransfers shuffles the transfers before they are dispatched
// this is done to avoid hitting the same partition continuously in an append only pattern
// TODO this should probably be removed after the high throughput block blob feature is implemented on the service side
func shuffleTransfers(transfers []common.CopyTransfer) {
	rand.Shuffle(len(transfers), func(i, j int) { transfers[i], transfers[j] = transfers[j], transfers[i] })
}
