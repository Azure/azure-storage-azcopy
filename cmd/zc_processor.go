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
	"net/url"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/common"
)

type copyTransferProcessor struct {
	numOfTransfersPerPart int
	copyJobTemplate       *common.CopyJobPartOrderRequest
	source                string
	destination           string

	// specify whether source/destination object names need to be URL encoded before dispatching
	shouldEscapeSourceObjectName      bool
	shouldEscapeDestinationObjectName bool

	// handles for progress tracking
	reportFirstPartDispatched func()
	reportFinalPartDispatched func()
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int,
	source string, destination string, shouldEscapeSourceObjectName bool, shouldEscapeDestinationObjectName bool,
	reportFirstPartDispatched func(), reportFinalPartDispatched func()) *copyTransferProcessor {
	return &copyTransferProcessor{
		numOfTransfersPerPart:             numOfTransfersPerPart,
		copyJobTemplate:                   copyJobTemplate,
		source:                            source,
		destination:                       destination,
		shouldEscapeSourceObjectName:      shouldEscapeSourceObjectName,
		shouldEscapeDestinationObjectName: shouldEscapeDestinationObjectName,
		reportFirstPartDispatched:         reportFirstPartDispatched,
		reportFinalPartDispatched:         reportFinalPartDispatched,
	}
}

func (s *copyTransferProcessor) scheduleCopyTransfer(storedObject storedObject) (err error) {
	if len(s.copyJobTemplate.Transfers) == s.numOfTransfersPerPart {
		resp := s.sendPartToSte()

		// TODO: If we ever do launch errors outside of the final "no transfers" error, make them output nicer things here.
		if resp.ErrorMsg != "" {
			return errors.New(string(resp.ErrorMsg))
		}

		// reset the transfers buffer
		s.copyJobTemplate.Transfers = []common.CopyTransfer{}
		s.copyJobTemplate.PartNum++
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.copyJobTemplate.Transfers = append(s.copyJobTemplate.Transfers, common.CopyTransfer{
		Source:           s.escapeIfNecessary(storedObject.relativePath, s.shouldEscapeSourceObjectName),
		Destination:      s.escapeIfNecessary(storedObject.relativePath, s.shouldEscapeDestinationObjectName),
		SourceSize:       storedObject.size,
		LastModifiedTime: storedObject.lastModifiedTime,
		ContentMD5:       storedObject.md5,
		BlobType:         storedObject.blobType,
	})
	return nil
}

func (s *copyTransferProcessor) escapeIfNecessary(path string, shouldEscape bool) string {
	if shouldEscape {
		return url.PathEscape(path)
	}

	return path
}

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	var resp common.CopyJobPartOrderResponse
	s.copyJobTemplate.IsFinalPart = true
	resp = s.sendPartToSte()

	if !resp.JobStarted {
		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return false, errors.New("no transfers were scheduled")
		}

		return false, fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s",
			s.copyJobTemplate.JobID, s.copyJobTemplate.PartNum, resp.ErrorMsg)
	}

	if s.reportFinalPartDispatched != nil {
		s.reportFinalPartDispatched()
	}
	return true, nil
}

// only test the response on the final dispatch as previous ones may for some reason not have transfers.
func (s *copyTransferProcessor) sendPartToSte() common.CopyJobPartOrderResponse {
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), s.copyJobTemplate, &resp)

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.copyJobTemplate.PartNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched()
	}

	return resp
}
