package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"strings"
)

type copyTransferProcessor struct {
	numOfTransfersPerPart int
	copyJobTemplate       *common.CopyJobPartOrderRequest
	source                string
	destination           string

	// handles for progress tracking
	reportFirstPartDispatched func()
	reportFinalPartDispatched func()
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int,
	source string, destination string, reportFirstPartDispatched func(), reportFinalPartDispatched func()) *copyTransferProcessor {
	return &copyTransferProcessor{
		numOfTransfersPerPart:     numOfTransfersPerPart,
		copyJobTemplate:           copyJobTemplate,
		source:                    source,
		destination:               destination,
		reportFirstPartDispatched: reportFirstPartDispatched,
		reportFinalPartDispatched: reportFinalPartDispatched,
	}
}

func (s *copyTransferProcessor) scheduleCopyTransfer(storedObject storedObject) (err error) {
	if len(s.copyJobTemplate.Transfers) == s.numOfTransfersPerPart {
		err = s.sendPartToSte()
		if err != nil {
			return err
		}

		// reset the transfers buffer
		s.copyJobTemplate.Transfers = []common.CopyTransfer{}
		s.copyJobTemplate.PartNum++
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.copyJobTemplate.Transfers = append(s.copyJobTemplate.Transfers, common.CopyTransfer{
		Source:           s.appendObjectPathToResourcePath(storedObject.relativePath, s.source),
		Destination:      s.appendObjectPathToResourcePath(storedObject.relativePath, s.destination),
		SourceSize:       storedObject.size,
		LastModifiedTime: storedObject.lastModifiedTime,
		ContentMD5:       storedObject.md5,
	})
	return nil
}

func (s *copyTransferProcessor) appendObjectPathToResourcePath(storedObjectPath, parentPath string) string {
	if storedObjectPath == "" {
		return parentPath
	}

	return strings.Join([]string{parentPath, storedObjectPath}, common.AZCOPY_PATH_SEPARATOR_STRING)
}

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	numberOfCopyTransfers := len(s.copyJobTemplate.Transfers)

	// if the number of transfer to copy is 0
	// and no part was dispatched, then it means there is no work to do
	if s.copyJobTemplate.PartNum == 0 && numberOfCopyTransfers == 0 {
		return false, nil
	}

	if numberOfCopyTransfers > 0 {
		s.copyJobTemplate.IsFinalPart = true
		err = s.sendPartToSte()
		if err != nil {
			return false, err
		}
	}

	if s.reportFinalPartDispatched != nil {
		s.reportFinalPartDispatched()
	}
	return true, nil
}

func (s *copyTransferProcessor) sendPartToSte() error {
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), s.copyJobTemplate, &resp)
	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed to dispatch because %s",
			s.copyJobTemplate.JobID, s.copyJobTemplate.PartNum, resp.ErrorMsg)
	}

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.copyJobTemplate.PartNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched()
	}

	return nil
}
