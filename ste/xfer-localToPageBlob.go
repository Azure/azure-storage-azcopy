package ste

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"unsafe"

	"net/http"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

type pageBlobUpload struct {
	jptm        IJobPartTransferMgr
	srcFile     *os.File
	source      string
	destination string
	blobUrl     azblob.BlobURL
	pacer       *pacer
}

func LocalToPageBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	// Get the transfer Information which include source, destination string, source size and other information.
	info := jptm.Info()
	blobSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	destURL, _ := url.Parse(info.Destination)

	// construct the blob Url using the destination url
	blobUrl := azblob.NewBlobURL(*destURL, p)

	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", chunkSize))
	}

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false, then check the blob exists or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := blobUrl.GetProperties(jptm.Context(), azblob.BlobAccessConditions{})
		if err == nil {
			// TODO: Confirm if this is an error condition or not
			// If the error is nil, then blob exists and it doesn't need to be uploaded.
			jptm.LogUploadError(info.Source, info.Destination, "Blob already exists", 0)
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	// Open the Source File.
	srcFile, err := os.Open(info.Source)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Couldn't open source-"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	pageBlobUrl := blobUrl.ToPageBlobURL()

	// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
	// then set maximum pageSize will be 4 MB.
	chunkSize = common.Iffint64(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)

	byteLength := common.Iffint64(blobSize > 512, 512, blobSize)
	byteBuffer := make([]byte, byteLength)
	_, err = srcFile.Read(byteBuffer)
	// Get http headers and meta data of page.
	blobHttpHeaders, metaData := jptm.BlobDstData(byteBuffer)

	// Create Page Blob of the source size
	_, err = pageBlobUrl.Create(jptm.Context(), blobSize,
		0, blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
	if err != nil {
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

	//set tier on pageBlob.
	//If set tier fails, then cancelling the job.
	_, pageBlobTier := jptm.BlobTiers()
	if pageBlobTier != common.EPageBlobTier.None() {
		// for blob tier, set the latest service version from sdk as service version in the context.
		ctxWithValue := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		_, err := pageBlobUrl.SetTier(ctxWithValue, pageBlobTier.ToAccessTierType())
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogUploadError(info.Source, info.Destination, "PageBlob SetTier-"+msg, status)
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
			jptm.SetErrorCode(int32(status))
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if status == http.StatusForbidden {
				common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
			}
			// Since transfer failed while setting the page blob tier
			// Deleting the created page blob
			_, err := pageBlobUrl.Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			if err != nil {
				// Log the error if deleting the page blob failed.
				jptm.LogError(pageBlobUrl.String(), "Deleting page blob", err)

			}
			jptm.ReportTransferDone()
			return
		}
	}
	// If the size of vhd is 0, then we need don't need to upload any ranges and
	// mark the transfer as successful
	if blobSize == 0 {
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()
	}
	// Calculate the number of Page Ranges for the given PageSize.
	numPages := common.Iffuint32(blobSize%chunkSize == 0,
		uint32(blobSize/chunkSize),
		uint32(blobSize/chunkSize)+1)

	jptm.SetNumberOfChunks(numPages)

	pbu := &pageBlobUpload{
		jptm:        jptm,
		srcFile:     srcFile,
		source:      info.Source,
		destination: info.Destination,
		blobUrl:     blobUrl,
		pacer:       pacer}

	// Scheduling page range update to the Page Blob created above.
	for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
		adjustedPageSize := chunkSize
		// compute actual size of the chunk
		if startIndex+chunkSize > blobSize {
			adjustedPageSize = blobSize - startIndex
		}
		// schedule the chunk job/msg
		jptm.ScheduleChunks(pbu.pageBlobUploadFunc(startIndex, adjustedPageSize))
	}
}

func (pbu *pageBlobUpload) pageBlobUploadFunc(startPage int64, calculatedPageSize int64) chunkFunc {
	return func(workerId int) {
		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		pbu.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer pbu.jptm.ReleaseAConnection()
		// pageDone is the function called after success / failure of each page.
		// If the calling page is the last page of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func() {
			if lastPage, _ := pbu.jptm.ReportChunkDone(); lastPage {
				if pbu.jptm.ShouldLog(pipeline.LogDebug) {
					pbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("Finalizing transfer"))
				}
				pbu.jptm.SetStatus(common.ETransferStatus.Success())
				pbu.srcFile.Close()
				// If the value of transfer Status is less than 0
				// transfer failed. Delete the page blob created
				if pbu.jptm.TransferStatus() <= 0 {
					// Deleting the created page blob
					_, err := pbu.blobUrl.ToPageBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
					if err != nil {
						// Log the error if deleting the page blob failed.
						pbu.jptm.LogError(pbu.blobUrl.String(), "DeletePageBlob ", err)
					}
				}
				pbu.jptm.ReportTransferDone()
			}
		}

		srcMMF, err := common.NewMMF(pbu.srcFile, false, startPage, calculatedPageSize)
		if err != nil {
			if pbu.jptm.WasCanceled() {
				pbu.jptm.LogError("Cancelled", "PutPageFailed ", err)
			} else {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				pbu.jptm.LogUploadError(pbu.source, pbu.destination, "UploadPages "+msg, status)
				// cancelling the transfer
				pbu.jptm.Cancel()
				pbu.jptm.SetStatus(common.ETransferStatus.Failed())
				pbu.jptm.SetErrorCode(int32(status))
			}
			pageDone()
			return
		}

		defer srcMMF.Unmap()

		if pbu.jptm.WasCanceled() {
			if pbu.jptm.ShouldLog(pipeline.LogInfo) {
				pbu.jptm.Log(pipeline.LogInfo, "Transfer Not Started since it is cancelled")
			}
			pageDone()
		} else {
			// pageBytes is the byte slice of Page for the given page range
			pageBytes := srcMMF.Slice()
			// converted the bytes slice to int64 array.
			// converting each of 8 bytes of byteSlice to an integer.
			int64Slice := (*(*[]int64)(unsafe.Pointer(&pageBytes)))[:len(pageBytes)/8]

			allBytesZero := true
			// Iterating though each integer of in64 array to check if any of the number is greater than 0 or not.
			// If any no is greater than 0, it means that the 8 bytes slice represented by that integer has atleast one byte greater than 0
			// If all integers are 0, it means that the 8 bytes slice represented by each integer has no byte greater than 0
			for index := 0; index < len(int64Slice); index++ {
				if int64Slice[index] != 0 {
					// If one number is greater than 0, then we need to perform the PutPage update.
					allBytesZero = false
					break
				}
			}

			// If all the bytes in the pageBytes is 0, then we do not need to perform the PutPage
			// Updating number of chunks done.
			if allBytesZero {
				if pbu.jptm.ShouldLog(pipeline.LogDebug) {
					pbu.jptm.Log(pipeline.LogDebug,
						fmt.Sprintf("All zero bytes. No Page Upload for range from %d to %d", startPage, startPage+calculatedPageSize))
				}
				pageDone()
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(pageBytes), pbu.pacer, srcMMF)
			pageBlobUrl := pbu.blobUrl.ToPageBlobURL()
			_, err := pageBlobUrl.UploadPages(pbu.jptm.Context(), startPage, body, azblob.BlobAccessConditions{})
			if err != nil {
				if pbu.jptm.WasCanceled() {
					pbu.jptm.LogError(pageBlobUrl.String(), "PutPageFailed ", err)
				} else {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					pbu.jptm.LogUploadError(pbu.source, pbu.destination, "UploadPages "+msg, status)
					// cancelling the transfer
					pbu.jptm.Cancel()
					pbu.jptm.SetStatus(common.ETransferStatus.Failed())
					pbu.jptm.SetErrorCode(int32(status))
					// If the status code was 403, it means there was an authentication error and we exit.
					// User can resume the job if completely ordered with a new sas.
					if status == http.StatusForbidden {
						common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
					}
				}
				pageDone()
				return
			}
			if pbu.jptm.ShouldLog(pipeline.LogDebug) {
				pbu.jptm.Log(pipeline.LogDebug,
					fmt.Sprintf("PUT page request successful: range %d to %d", startPage, startPage+calculatedPageSize))
			}
			pageDone()
		}
	}
}
