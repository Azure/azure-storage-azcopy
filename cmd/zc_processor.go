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

package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
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

	// In the case of single file transfers, relative path is empty and we must use the object name.
	var source string
	var destination string
	if storedObject.relativePath == "" {
		source = storedObject.name
		destination = storedObject.name
	} else {
		source = storedObject.relativePath
		destination = storedObject.relativePath
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.copyJobTemplate.Transfers = append(s.copyJobTemplate.Transfers, common.CopyTransfer{
		Source:           source,
		Destination:      destination,
		SourceSize:       storedObject.size,
		LastModifiedTime: storedObject.lastModifiedTime,
		ContentMD5:       storedObject.md5,
	})
	return nil
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
