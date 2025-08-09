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
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

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
	hardlinkHandlingType   common.HardlinkHandlingType

	//XDM: This is only essential when sync is through syncOrchestrator
	syncTransferMutex sync.Mutex // mutex to synchronize access to the transfer scheduler

	// Separate tracking for files and folders based on processing mode
	processingMode common.JobProcessingMode
	dispatcher     syncJobPartDispatcher
}

type syncJobPartDispatcher struct {
	PendingTransfers       common.Transfers
	PendingFolderTransfers common.Transfers
}

func newCopyTransferProcessor(copyJobTemplate *common.CopyJobPartOrderRequest, numOfTransfersPerPart int, source, destination common.ResourceString, reportFirstPartDispatched func(bool), reportFinalPartDispatched func(), preserveAccessTier, dryrunMode bool) *copyTransferProcessor {
	processor := &copyTransferProcessor{
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
		processingMode:            copyJobTemplate.JobProcessingMode,
		dispatcher:                syncJobPartDispatcher{},
	}

	return processor
}

type DryrunTransfer struct {
	EntityType   common.EntityType
	BlobType     common.BlobType
	FromTo       common.FromTo
	Source       string
	Destination  string
	SourceSize   *int64
	HttpHeaders  blob.HTTPHeaders
	Metadata     common.Metadata
	BlobTier     *blob.AccessTier
	BlobVersion  *string
	BlobTags     common.BlobTags
	BlobSnapshot *string
}

type dryrunTransferSurrogate struct {
	EntityType         string
	BlobType           string
	FromTo             string
	Source             string
	Destination        string
	SourceSize         int64           `json:"SourceSize,omitempty"`
	ContentType        string          `json:"ContentType,omitempty"`
	ContentEncoding    string          `json:"ContentEncoding,omitempty"`
	ContentDisposition string          `json:"ContentDisposition,omitempty"`
	ContentLanguage    string          `json:"ContentLanguage,omitempty"`
	CacheControl       string          `json:"CacheControl,omitempty"`
	ContentMD5         []byte          `json:"ContentMD5,omitempty"`
	BlobTags           common.BlobTags `json:"BlobTags,omitempty"`
	Metadata           common.Metadata `json:"Metadata,omitempty"`
	BlobTier           blob.AccessTier `json:"BlobTier,omitempty"`
	BlobVersion        string          `json:"BlobVersion,omitempty"`
	BlobSnapshotID     string          `json:"BlobSnapshotID,omitempty"`
}

func (d *DryrunTransfer) UnmarshalJSON(bytes []byte) error {
	var surrogate dryrunTransferSurrogate

	err := json.Unmarshal(bytes, &surrogate)
	if err != nil {
		return fmt.Errorf("failed to parse dryrun transfer: %w", err)
	}

	err = d.FromTo.Parse(surrogate.FromTo)
	if err != nil {
		return fmt.Errorf("failed to parse fromto: %w", err)
	}

	err = d.EntityType.Parse(surrogate.EntityType)
	if err != nil {
		return fmt.Errorf("failed to parse entity type: %w", err)
	}

	err = d.BlobType.Parse(surrogate.BlobType)
	if err != nil {
		return fmt.Errorf("failed to parse entity type: %w", err)
	}

	d.Source = surrogate.Source
	d.Destination = surrogate.Destination

	d.SourceSize = &surrogate.SourceSize
	d.HttpHeaders.BlobContentType = &surrogate.ContentType
	d.HttpHeaders.BlobContentEncoding = &surrogate.ContentEncoding
	d.HttpHeaders.BlobCacheControl = &surrogate.CacheControl
	d.HttpHeaders.BlobContentDisposition = &surrogate.ContentDisposition
	d.HttpHeaders.BlobContentLanguage = &surrogate.ContentLanguage
	d.HttpHeaders.BlobContentMD5 = surrogate.ContentMD5
	d.BlobTags = surrogate.BlobTags
	d.Metadata = surrogate.Metadata
	d.BlobTier = &surrogate.BlobTier
	d.BlobVersion = &surrogate.BlobVersion
	d.BlobSnapshot = &surrogate.BlobSnapshotID

	return nil
}

func (d DryrunTransfer) MarshalJSON() ([]byte, error) {
	surrogate := dryrunTransferSurrogate{
		d.EntityType.String(),
		d.BlobType.String(),
		d.FromTo.String(),
		d.Source,
		d.Destination,
		common.IffNotNil(d.SourceSize, 0),
		common.IffNotNil(d.HttpHeaders.BlobContentType, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentEncoding, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentDisposition, ""),
		common.IffNotNil(d.HttpHeaders.BlobContentLanguage, ""),
		common.IffNotNil(d.HttpHeaders.BlobCacheControl, ""),
		d.HttpHeaders.BlobContentMD5,
		d.BlobTags,
		d.Metadata,
		common.IffNotNil(d.BlobTier, ""),
		common.IffNotNil(d.BlobVersion, ""),
		common.IffNotNil(d.BlobSnapshot, ""),
	}

	return json.Marshal(surrogate)
}

func (s *copyTransferProcessor) scheduleCopyTransfer(storedObject StoredObject) (err error) {

	// Escape paths on destinations where the characters are invalid
	// And re-encode them where the characters are valid.
	var srcRelativePath, dstRelativePath string
	if storedObject.relativePath == "\x00" { // Short circuit when we're talking about root/, because the STE is funky about this.
		srcRelativePath, dstRelativePath = storedObject.relativePath, storedObject.relativePath
	} else {
		srcRelativePath = pathEncodeRules(storedObject.relativePath, s.copyJobTemplate.FromTo, false, true)
		dstRelativePath = pathEncodeRules(storedObject.relativePath, s.copyJobTemplate.FromTo, false, false)
		if srcRelativePath != "" {
			srcRelativePath = "/" + srcRelativePath
		}
		if dstRelativePath != "" {
			dstRelativePath = "/" + dstRelativePath
		}
	}

	copyTransfer, shouldSendToSte := storedObject.ToNewCopyTransfer(false, srcRelativePath, dstRelativePath, s.preserveAccessTier, s.folderPropertiesOption, s.symlinkHandlingType, s.hardlinkHandlingType)

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
			prettySrcRelativePath, prettyDstRelativePath := srcRelativePath, dstRelativePath

			fromTo := s.copyJobTemplate.FromTo
			if fromTo.From().IsRemote() {
				prettySrcRelativePath, err = url.PathUnescape(prettySrcRelativePath)
				if err != nil {
					prettySrcRelativePath = srcRelativePath // Fall back, because it's better than failing.
				}
			}

			if fromTo.To().IsRemote() {
				prettyDstRelativePath, err = url.PathUnescape(prettyDstRelativePath)
				if err != nil {
					prettyDstRelativePath = dstRelativePath // Fall back, because it's better than failing.
				}
			}

			if format == common.EOutputFormat.Json() {
				tx := DryrunTransfer{
					EntityType:  storedObject.entityType,
					BlobType:    common.FromBlobType(storedObject.blobType),
					FromTo:      s.copyJobTemplate.FromTo,
					Source:      common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath),
					Destination: "",
					SourceSize:  &storedObject.size,
					HttpHeaders: blob.HTTPHeaders{
						BlobCacheControl:       &storedObject.cacheControl,
						BlobContentDisposition: &storedObject.contentDisposition,
						BlobContentEncoding:    &storedObject.contentEncoding,
						BlobContentLanguage:    &storedObject.contentLanguage,
						BlobContentMD5:         storedObject.md5,
						BlobContentType:        &storedObject.contentType,
					},
					Metadata:     storedObject.Metadata,
					BlobTier:     &storedObject.blobAccessTier,
					BlobVersion:  &storedObject.blobVersionID,
					BlobTags:     storedObject.blobTags,
					BlobSnapshot: &storedObject.blobSnapshotID,
				}

				if fromTo.To() != common.ELocation.None() && fromTo.To() != common.ELocation.Unknown() {
					tx.Destination = common.GenerateFullPath(s.copyJobTemplate.DestinationRoot.Value, prettyDstRelativePath)
				}

				jsonOutput, err := json.Marshal(tx)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				// if remove then To() will equal to common.ELocation.Unknown()
				if s.copyJobTemplate.FromTo.To() == common.ELocation.Unknown() { // remove
					return fmt.Sprintf("DRYRUN: remove %v",
						common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath))
				}
				if s.copyJobTemplate.FromTo.To() == common.ELocation.None() { // set-properties
					return fmt.Sprintf("DRYRUN: set-properties %v",
						common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath))
				} else { // copy for sync
					return fmt.Sprintf("DRYRUN: copy %v to %v",
						common.GenerateFullPath(s.copyJobTemplate.SourceRoot.Value, prettySrcRelativePath),
						common.GenerateFullPath(s.copyJobTemplate.DestinationRoot.Value, prettyDstRelativePath))
				}
			}
		})
		return nil
	}

	if UseSyncOrchestrator {
		s.syncTransferMutex.Lock()
		defer s.syncTransferMutex.Unlock()
	}

	s.dispatchPartIfReady()

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.appendTransfer(copyTransfer)

	return nil
}

func (s *copyTransferProcessor) readyForDispatch() bool {
	return (len(s.dispatcher.PendingTransfers.List) == s.numOfTransfersPerPart) ||
		(len(s.dispatcher.PendingFolderTransfers.List) == s.numOfTransfersPerPart)
}

func (s *copyTransferProcessor) dispatchPartIfReady() error {
	if !s.readyForDispatch() {
		return nil
	}

	var err error
	if len(s.dispatcher.PendingTransfers.List) == s.numOfTransfersPerPart {
		s.copyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
		err = s.dispatchPart()
		if err != nil {
			return err
		}
		s.dispatcher.PendingTransfers = common.Transfers{}
	}

	if len(s.dispatcher.PendingFolderTransfers.List) == s.numOfTransfersPerPart {
		s.copyJobTemplate.Transfers = s.dispatcher.PendingFolderTransfers.Clone()
		err = s.dispatchPart()
		if err != nil {
			return err
		}
		s.dispatcher.PendingFolderTransfers = common.Transfers{}
	}

	return err
}

func (s *copyTransferProcessor) dispatchPart() error {
	resp := s.sendPartToSte()

	// TODO: If we ever do launch errors outside of the final "no transfers" error, make them output nicer things here.
	if resp.ErrorMsg != "" {
		return errors.New(string(resp.ErrorMsg))
	}

	// reset the transfers buffer
	s.copyJobTemplate.Transfers = common.Transfers{}
	s.copyJobTemplate.PartNum++

	return nil
}

func (s *copyTransferProcessor) appendTransfer(copyTransfer common.CopyTransfer) error {

	if s.processingMode == common.EJobProcessingMode.FolderAfterFiles() &&
		copyTransfer.EntityType == common.EEntityType.Folder() {
		s.dispatcher.PendingFolderTransfers.List = append(s.dispatcher.PendingFolderTransfers.List, copyTransfer)
		s.dispatcher.PendingFolderTransfers.TotalSizeInBytes += uint64(copyTransfer.SourceSize)
		s.dispatcher.PendingFolderTransfers.FolderTransferCount++
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
		case common.EEntityType.FileProperties():
			s.dispatcher.PendingTransfers.FilePropertyTransferCount++
		}
	}

	return nil
}

var NothingScheduledError = errors.New("no transfers were scheduled because no files matched the specified criteria")
var FinalPartCreatedMessage = "Final job part has been created"

func (s *copyTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	// Handle separate batch mode with remaining file and folder batches
	if s.processingMode == common.EJobProcessingMode.FolderAfterFiles() {
		if len(s.dispatcher.PendingTransfers.List) > 0 && len(s.dispatcher.PendingFolderTransfers.List) > 0 {
			// if there are both kinds of transfers pending, first do the file transfers
			s.copyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
			err = s.dispatchPart()
			if err != nil {
				return false, fmt.Errorf("failed to send final file job part with job Id %s and part number %d: %s",
					s.copyJobTemplate.JobID, s.copyJobTemplate.PartNum, err.Error())
			}
			s.dispatcher.PendingTransfers = common.Transfers{}
		}

		// Either file or folder transfers are pending. Whatever is pending will be the final part.
		if len(s.dispatcher.PendingTransfers.List) > 0 {
			s.copyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
		} else if len(s.dispatcher.PendingFolderTransfers.List) > 0 {
			s.copyJobTemplate.Transfers = s.dispatcher.PendingFolderTransfers.Clone()
		}
	} else {
		if len(s.dispatcher.PendingTransfers.List) > 0 {
			s.copyJobTemplate.Transfers = s.dispatcher.PendingTransfers.Clone()
		}
	}

	// Original logic for non-NFS mode
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
