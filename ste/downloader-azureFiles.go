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

package ste

import (
	"errors"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type azureFilesDownloader struct {
	jptm   IJobPartTransferMgr
	txInfo TransferInfo
	sip    ISourceInfoProvider
}

func newAzureFilesDownloader() downloader {
	return &azureFilesDownloader{}
}

func (bd *azureFilesDownloader) init(jptm IJobPartTransferMgr) {
	bd.txInfo = jptm.Info()
	var err error
	bd.sip, err = newFileSourceInfoProvider(jptm)
	bd.jptm = jptm
	common.PanicIfErr(err) // This literally will never return an error in the first place.
	// It's not possible for newDefaultRemoteSourceInfoProvider to return an error,
	// and it's not possible for newFileSourceInfoProvider to return an error either.
}

func (bd *azureFilesDownloader) isInitialized() bool {
	// TODO: only day, do we really want this object to be able to exist in an uninitialized state?
	//   Could/should we refactor the construction...?
	return bd.jptm != nil
}

var errorNoSddlFound = errors.New("no SDDL found")

func (bd *azureFilesDownloader) preserveAttributes() (stage string, err error) {
	info := bd.jptm.Info()

	if info.PreserveSMBPermissions.IsTruthy() {
		// We're about to call into Windows-specific code.
		// Some functions here can't be called on other OSes, to the extent that they just aren't present in the library due to compile flags.
		// In order to work around this, we'll do some trickery with interfaces.
		// There is a windows-specific file (downloader-azureFiles_windows.go) that makes azureFilesDownloader satisfy the smbPropertyAwareDownloader interface.
		// This function isn't present on other OSes due to compile flags,
		// so in that way, we can cordon off these sections that would otherwise require filler functions.
		// To do that, we'll do some type wrangling:
		// bd can't directly be wrangled from a struct, so we wrangle it to an interface, then do so.
		if spdl, ok := interface{}(bd).(smbPropertyAwareDownloader); ok {
			// We don't need to worry about the sip not being a ISMBPropertyBearingSourceInfoProvider as Azure Files always is.
			err = spdl.PutSDDL(bd.sip.(ISMBPropertyBearingSourceInfoProvider), bd.txInfo)
			if err == errorNoSddlFound {
				bd.jptm.LogAtLevelForCurrentTransfer(pipeline.LogDebug, "No SMB permissions were downloaded because none were found at the source")
			} else if err != nil {
				return "Setting destination file SDDLs", err
			}
		}
	}

	if info.PreserveSMBInfo {
		// must be done AFTER we preserve the permissions (else some of the flags/dates set here may be lost)
		if spdl, ok := interface{}(bd).(smbPropertyAwareDownloader); ok {
			// We don't need to worry about the sip not being a ISMBPropertyBearingSourceInfoProvider as Azure Files always is.
			err := spdl.PutSMBProperties(bd.sip.(ISMBPropertyBearingSourceInfoProvider), bd.txInfo)

			if err != nil {
				return "Setting destination file SMB properties", err
			}
		}
	}

	return "", nil
}

func (bd *azureFilesDownloader) Prologue(jptm IJobPartTransferMgr, srcPipeline pipeline.Pipeline) {
	bd.init(jptm)
}

func (bd *azureFilesDownloader) Epilogue() {
	if !bd.isInitialized() {
		return // nothing we can do
	}
	if bd.jptm.IsLive() {
		stage, err := bd.preserveAttributes()
		if err != nil {
			bd.jptm.FailActiveDownload(stage, err)
		}
	}
}

// GenerateDownloadFunc returns a chunk-func for file downloads
func (bd *azureFilesDownloader) GenerateDownloadFunc(jptm IJobPartTransferMgr, srcPipeline pipeline.Pipeline, destWriter common.ChunkedFileWriter, id common.ChunkID, length int64, pacer pacer) chunkFunc {
	return createDownloadChunkFunc(jptm, id, func() {

		// step 1: Downloading the file from range startIndex till (startIndex + adjustedChunkSize)
		info := jptm.Info()
		u, _ := url.Parse(info.Source)
		srcFileURL := azfile.NewFileURL(*u, srcPipeline)
		// At this point we create an HTTP(S) request for the desired portion of the file, and
		// wait until we get the headers back... but we have not yet read its whole body.
		// The Download method encapsulates any retries that may be necessary to get to the point of receiving response headers.
		jptm.LogChunkStatus(id, common.EWaitReason.HeaderResponse())
		get, err := srcFileURL.Download(jptm.Context(), id.OffsetInFile(), length, false)
		if err != nil {
			jptm.FailActiveDownload("Downloading response body", err) // cancel entire transfer because this chunk has failed
			return
		}

		// Verify that the file has not been changed via a client side LMT check
		getLocation := get.LastModified().Location()
		if !get.LastModified().Equal(jptm.LastModifiedTime().In(getLocation)) {
			jptm.FailActiveDownload("Azure File modified during transfer",
				errors.New("Azure File modified during transfer"))
		}

		// step 2: Enqueue the response body to be written out to disk
		// The retryReader encapsulates any retries that may be necessary while downloading the body
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		retryReader := get.Body(azfile.RetryReaderOptions{
			MaxRetryRequests: MaxRetryPerDownloadBody,
			NotifyFailedRead: common.NewReadLogFunc(jptm, u),
		})
		defer retryReader.Close()
		err = destWriter.EnqueueChunk(jptm.Context(), id, length, newPacedResponseBody(jptm.Context(), retryReader, pacer), true)
		if err != nil {
			jptm.FailActiveDownload("Enqueuing chunk", err)
			return
		}
	})
}

func (bd *azureFilesDownloader) SetFolderProperties(jptm IJobPartTransferMgr) error {
	bd.init(jptm) // since Prologue doesn't get called for folders
	_, err := bd.preserveAttributes()
	return err
}
