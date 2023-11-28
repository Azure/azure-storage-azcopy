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
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"net/url"
	"runtime"
	"strings"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type copyTransferProcessor struct {
	numOfTransfersPerPart int
	copyJobTemplate       *common.CopyJobPartOrderRequest
	source                common.ResourceString
	destination           common.ResourceString

	// handles for progress tracking
	reportFirstPartDispatched func(jobStarted bool)
	reportFinalPartDispatched func()

	preserveAccessTier     bool
	folderPropertiesOption common.FolderPropertyOption
	symlinkHandlingType    common.SymlinkHandlingType
	dryrunMode             bool
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int, source, destination common.ResourceString, reportFirstPartDispatched func(bool), reportFinalPartDispatched func(), preserveAccessTier, dryrunMode bool) *copyTransferProcessor {
	return &copyTransferProcessor{
		numOfTransfersPerPart:     numOfTransfersPerPart,
		copyJobTemplate:           copyJobTemplate,
		source:                    source,
		destination:               destination,
		reportFirstPartDispatched: reportFirstPartDispatched,
		reportFinalPartDispatched: reportFinalPartDispatched,
		preserveAccessTier:        preserveAccessTier,
		folderPropertiesOption:    copyJobTemplate.Fpo,
		symlinkHandlingType:       copyJobTemplate.SymlinkHandlingType,
		dryrunMode:                dryrunMode,
	}
}

func (s *copyTransferProcessor) scheduleCopyTransfer(storedObject StoredObject) (err error) {

	// Escape paths on destinations where the characters are invalid
	// And re-encode them where the characters are valid.
	srcRelativePath := pathEncodeRules(storedObject.relativePath, s.copyJobTemplate.FromTo, false, true)
	dstRelativePath := pathEncodeRules(storedObject.relativePath, s.copyJobTemplate.FromTo, false, false)
	if srcRelativePath != "" {
		srcRelativePath = "/" + srcRelativePath
	}
	if dstRelativePath != "" {
		dstRelativePath = "/" + dstRelativePath
	}
	copyTransfer, shouldSendToSte := storedObject.ToNewCopyTransfer(false, srcRelativePath, dstRelativePath, s.preserveAccessTier, s.folderPropertiesOption, s.symlinkHandlingType)

	if s.copyJobTemplate.FromTo.To() == common.ELocation.None() {
		copyTransfer.BlobTier = s.copyJobTemplate.BlobAttributes.BlockBlobTier.ToAccessTierType()

		metadataString := s.copyJobTemplate.BlobAttributes.Metadata
		metadataMap := common.Metadata{}
		if len(metadataString) > 0 {
			for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
				kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
				metadataMap[kv[0]] = &kv[1]
			}
		}
		copyTransfer.Metadata = metadataMap

		copyTransfer.BlobTags = common.ToCommonBlobTagsMap(s.copyJobTemplate.BlobAttributes.BlobTagsString)
	}

	if !shouldSendToSte {
		return nil // skip this one
	}

	if s.dryrunMode {
		glcm.Dryrun(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(copyTransfer)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				prettySrcRelativePath, err := url.QueryUnescape(srcRelativePath)
				common.PanicIfErr(err)
				prettyDstRelativePath, err := url.QueryUnescape(dstRelativePath)
				common.PanicIfErr(err)

				// if remove then To() will equal to common.ELocation.Unknown()
				if s.copyJobTemplate.FromTo.To() == common.ELocation.Unknown() { // remove
					return fmt.Sprintf("DRYRUN: remove %v/%v",
						s.copyJobTemplate.SourceRoot.Value,
						prettySrcRelativePath)
				}
				if s.copyJobTemplate.FromTo.To() == common.ELocation.None() { // set-properties
					return fmt.Sprintf("DRYRUN: set-properties %v/%v",
						s.copyJobTemplate.SourceRoot.Value,
						prettySrcRelativePath)
				} else { // copy for sync
					if s.copyJobTemplate.FromTo.From() == common.ELocation.Local() {
						// formatting from local source
						dryrunValue := fmt.Sprintf("DRYRUN: copy %v", common.ToShortPath(s.copyJobTemplate.SourceRoot.Value))
						if runtime.GOOS == "windows" {
							dryrunValue += "\\" + strings.ReplaceAll(prettySrcRelativePath, "/", "\\")
						} else { // linux and mac
							dryrunValue += "/" + prettySrcRelativePath
						}
						dryrunValue += fmt.Sprintf(" to %v/%v", strings.Trim(s.copyJobTemplate.DestinationRoot.Value, "/"), prettyDstRelativePath)
						return dryrunValue
					} else if s.copyJobTemplate.FromTo.To() == common.ELocation.Local() {
						// formatting to local source
						dryrunValue := fmt.Sprintf("DRYRUN: copy %v/%v to %v",
							strings.Trim(s.copyJobTemplate.SourceRoot.Value, "/"), prettySrcRelativePath,
							common.ToShortPath(s.copyJobTemplate.DestinationRoot.Value))
						if runtime.GOOS == "windows" {
							dryrunValue += "\\" + strings.ReplaceAll(prettyDstRelativePath, "/", "\\")
						} else { // linux and mac
							dryrunValue += "/" + prettyDstRelativePath
						}
						return dryrunValue
					} else {
						return fmt.Sprintf("DRYRUN: copy %v/%v to %v/%v",
							s.copyJobTemplate.SourceRoot.Value,
							prettySrcRelativePath,
							s.copyJobTemplate.DestinationRoot.Value,
							prettyDstRelativePath)
					}
				}
			}
		})
		return nil
	}

	if len(s.copyJobTemplate.Transfers.List) == s.numOfTransfersPerPart {
		resp := s.sendPartToSte()

		// TODO: If we ever do launch errors outside of the final "no transfers" error, make them output nicer things here.
		if resp.ErrorMsg != "" {
			return errors.New(string(resp.ErrorMsg))
		}

		// reset the transfers buffer
		s.copyJobTemplate.Transfers = common.Transfers{}
		s.copyJobTemplate.PartNum++
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.copyJobTemplate.Transfers.List = append(s.copyJobTemplate.Transfers.List, copyTransfer)
	s.copyJobTemplate.Transfers.TotalSizeInBytes += uint64(copyTransfer.SourceSize)

	switch copyTransfer.EntityType {
	case common.EEntityType.File():
		s.copyJobTemplate.Transfers.FileTransferCount++
	case common.EEntityType.Folder():
		s.copyJobTemplate.Transfers.FolderTransferCount++
	case common.EEntityType.Symlink():
		s.copyJobTemplate.Transfers.SymlinkTransferCount++
	}

	return nil
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")
var FinalPartCreatedMessage = "Final job part has been created"

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	var resp common.CopyJobPartOrderResponse
	s.copyJobTemplate.IsFinalPart = true
	resp = s.sendPartToSte()

	if !resp.JobStarted {
		if resp.ErrorMsg == common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr() {
			return false, NothingScheduledError
		}

		return false, fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s",
			s.copyJobTemplate.JobID, s.copyJobTemplate.PartNum, resp.ErrorMsg)
	}

	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(FinalPartCreatedMessage, common.LogInfo)
	}

	if s.reportFinalPartDispatched != nil {
		s.reportFinalPartDispatched()
	}
	return true, nil
}

// only test the response on the final dispatch to help diagnose root cause of test failures from 0 transfers
func (s *copyTransferProcessor) sendPartToSte() common.CopyJobPartOrderResponse {
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), s.copyJobTemplate, &resp)

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.copyJobTemplate.PartNum == 0 && s.reportFirstPartDispatched != nil {
		s.reportFirstPartDispatched(resp.JobStarted)
	}

	return resp
}
