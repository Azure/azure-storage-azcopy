package ste

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"

	"net/http"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

func LocalToAppendBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {
	// Get the transfer Information which include source, destination string, source size and other information.
	info := jptm.Info()
	blobSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
	// then set maximum pageSize will be 4 MB.
	chunkSize = common.Iffint64(
		chunkSize > common.MaxAppendBlobBlockSize,
		common.MaxAppendBlobBlockSize,
		chunkSize)

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

	srcMMF, err := common.NewMMF(srcFile, false, 0, blobSize)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "failed while memory map the source-"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	appendBlobURL := blobUrl.ToAppendBlobURL()

	byteLength := common.Iffint64(blobSize > 512, 512, blobSize)
	byteBuffer := make([]byte, byteLength)
	_, err = srcFile.Read(byteBuffer)
	// Get http headers and meta data of page.
	blobHttpHeaders, metaData := jptm.BlobDstData(byteBuffer)

	// Create Page Blob of the source size
	_, err = appendBlobURL.Create(jptm.Context(), blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
	if err != nil {
		// If the transfer was not cancelled, set the status of transfer to Failed and cancel the transfer.
		if !jptm.WasCanceled() {
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Failed())
		}
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(info.Source, info.Destination, "PageBlob Create-"+msg, status)
		jptm.SetErrorCode(int32(status))
		jptm.ReportTransferDone()
		// If the status code was 403, it means there was an authentication error and we exit.
		// User can resume the job if completely ordered with a new sas.
		if status == http.StatusForbidden {
			common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
		}
		return
	}

	// If the size of vhd is 0, then we need don't need to upload any ranges and
	// mark the transfer as successful
	if blobSize == 0 {
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()
	}
	// Calculate the number of Page Ranges for the given PageSize.
	numBlocks := common.Iffuint32(blobSize%chunkSize == 0,
		uint32(blobSize/chunkSize),
		uint32(blobSize/chunkSize)+1)

	jptm.SetNumberOfChunks(numBlocks)

	// Scheduling page range update to the Page Blob created above.
	for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
		adjustedBlockSize := chunkSize
		// compute actual size of the chunk
		if startIndex+chunkSize > blobSize {
			adjustedBlockSize = blobSize - startIndex
		}
		// requesting (startIndex + adjustedChunkSize) bytes from pacer to be send to service.
		body := newRequestBodyPacer(bytes.NewReader(srcMMF.Slice()[startIndex:startIndex+adjustedBlockSize]), pacer, srcMMF)
		_, err := appendBlobURL.AppendBlock(jptm.Context(), body, azblob.BlobAccessConditions{})
		if err != nil {
			if !jptm.WasCanceled() {
				jptm.Cancel()
				jptm.SetStatus(common.ETransferStatus.Failed())
			}
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogUploadError(info.Source, info.Destination, "PageBlob Create-"+msg, status)
			jptm.SetErrorCode(int32(status))
			// before reporting the transfer done, try deleting the above created blob
			_, deletErr := appendBlobURL.Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			if err != nil {
				// Log the error if deleting the page blob failed.
				jptm.LogError(appendBlobURL.String(), "Delete Append Blob ", deletErr)
			}
			jptm.ReportTransferDone()
			// If the status code was 403, it means there was an authentication error and we exit.
			// User can resume the job if completely ordered with a new sas.
			if status == http.StatusForbidden {
				common.GetLifecycleMgr().Exit(fmt.Sprintf("Authentication Failed. The SAS is not correct or expired or does not have the correct permission %s", err.Error()), 1)
			}
			return
		}
	}
	// Log the successful message
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL ")
	}
	// Set the transfer status and report transfer done.
	jptm.SetStatus(common.ETransferStatus.Success())
	jptm.ReportTransferDone()
}
