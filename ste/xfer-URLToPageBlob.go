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
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type pageBlobCopy struct {
	jptm        IJobPartTransferMgr
	srcURL      url.URL
	destBlobURL azblob.BlobURL
	pacer       *pacer
}

// URLToPageBlob copies resource could be get through URL to Azure Blob.
func URLToPageBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	// step 1: Get the source, destination info for the transfer.
	info := jptm.Info()
	srcURL, _ := url.Parse(info.Source)
	destURL, _ := url.Parse(info.Destination)

	destBlobURL := azblob.NewBlobURL(*destURL, p)

	// step 2: Get size info for the copy.
	srcSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	// If the given chunk Size for the Job is invalild for page blob or greater than maximum page size,
	// then set chunkSize as maximum pageSize.
	chunkSize = common.Iffint64(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)

	// TODO: Validate Block Size, ensure it's validate for both source and destination
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Chunk size %d", chunkSize))
	}

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the blob exists or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := destBlobURL.GetProperties(jptm.Context(), azblob.BlobAccessConditions{})
		if err == nil {
			// If the error is nil, then blob exists and doesn't needs to be copied.
			jptm.LogS2SCopyError(info.Source, info.Destination, "Blob already exists", 0)
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	var azblobMetadata azblob.Metadata
	if info.SrcMetadata != nil {
		azblobMetadata = info.SrcMetadata.ToAzBlobMetadata()
	}

	// step 3: copy file to blob
	destPageBlobURL := destBlobURL.ToPageBlobURL()

	// Create Page Blob of the source size
	if _, err := destPageBlobURL.Create(jptm.Context(), srcSize,
		0, info.SrcHTTPHeaders, azblobMetadata, azblob.BlobAccessConditions{}); err != nil {
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(info.Source, info.Destination, "PageBlob Create-"+msg, status)
		jptm.Cancel()
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.SetErrorCode(int32(status))
		jptm.ReportTransferDone()
		// If the status code was 403, it means there was an authentication error and we exit.
		// User can resume the job if completely ordered with a new sas.
		if status == http.StatusForbidden {
			common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
		}
		return
	}

	// TODO: set blob tiers

	if srcSize == 0 {
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()
	}

	// Calculate the number of Page Ranges for the given PageSize.
	numPages := common.Iffuint32(srcSize%chunkSize == 0,
		uint32(srcSize/chunkSize),
		uint32(srcSize/chunkSize)+1)

	jptm.SetNumberOfChunks(numPages)

	pbc := &pageBlobCopy{
		jptm:        jptm,
		srcURL:      *srcURL,
		destBlobURL: destBlobURL,
		pacer:       pacer}

	adjustedChunkSize := chunkSize
	for startRange := int64(0); startRange < srcSize; startRange += chunkSize {
		// compute exact size of the chunk
		// startRange also equals to overall scheduled size
		if startRange+chunkSize > srcSize {
			adjustedChunkSize = srcSize - startRange
		}

		// schedule the download chunk job
		jptm.ScheduleChunks(pbc.generateCopyURLToPageBlobFunc(startRange, adjustedChunkSize))
	}
}

func (pbc *pageBlobCopy) generateCopyURLToPageBlobFunc(startRange int64, calculatedPageSize int64) chunkFunc {
	return func(workerId int) {
		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		pbc.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer pbc.jptm.ReleaseAConnection()
		// pageDone is the function called after success / failure of each page.
		// If the calling page is the last page of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func() {
			if lastPage, _ := pbc.jptm.ReportChunkDone(); lastPage {
				if pbc.jptm.ShouldLog(pipeline.LogDebug) {
					pbc.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Finalizing transfer"))
				}
				pbc.jptm.SetStatus(common.ETransferStatus.Success())
				// If the value of transfer Status is less than 0
				// transfer failed. Delete the page blob created
				if pbc.jptm.TransferStatus() <= 0 {
					// Deleting the created page blob
					_, err := pbc.destBlobURL.ToPageBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
					if err != nil {
						// Log the error if deleting the page blob failed.
						pbc.jptm.LogError(pbc.destBlobURL.String(), "DeletePageBlob ", err)
					}
				}
				pbc.jptm.ReportTransferDone()
			}
		}

		if pbc.jptm.WasCanceled() {
			if pbc.jptm.ShouldLog(pipeline.LogInfo) {
				pbc.jptm.Log(pipeline.LogInfo, "Transfer Not Started since it is cancelled")
			}
			pageDone()
		} else {
			// TODO: Using PutPageFromURL to fulfill the copy
		}
	}
}
