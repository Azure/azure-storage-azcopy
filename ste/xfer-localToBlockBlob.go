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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"net/http"
	"net/url"
	"os"
	"strings"
	"unsafe"
)

type blockBlobUpload struct {
	jptm     IJobPartTransferMgr
	srcMmf   common.MMF
	blobURL  azblob.BlobURL
	pacer    *pacer
	blockIds []string
}

type pageBlobUpload struct {
	jptm    IJobPartTransferMgr
	srcMmf  common.MMF
	blobUrl azblob.BlobURL
	pacer   *pacer
}

func LocalToBlockBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	EndsWith := func(s string, t string) bool {
		return len(s) >= len(t) && strings.EqualFold(s[len(s)-len(t):], t)
	}

	// step 1. Get the transfer Information which include source, destination string, source size and other information.
	info := jptm.Info()
	blobSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	u, _ := url.Parse(info.Destination)
	blobUrl := azblob.NewBlobURL(*u, p)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the blob exists or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := blobUrl.GetProperties(jptm.Context(), azblob.BlobAccessConditions{})
		if err == nil {
			// If the error is nil, then blob exists and it doesn't needs to be uploaded.
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("skipping the transfer since blob already exists"))
			}
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.AddToBytesDone(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// step 2a: Open the Source File.
	srcFile, err := os.Open(info.Source)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error opening the source file %s", info.SourceSize))
		}
		jptm.AddToBytesDone(info.SourceSize)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	defer srcFile.Close()

	// 2b: Memory map the source file. If the file size if not greater than 0, then doesn't memory map the file.
	var srcMmf common.MMF
	if blobSize > 0 {
		// file needs to be memory mapped only when the file size is greater than 0.
		srcMmf, err = common.NewMMF(srcFile, false, 0, blobSize)
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("error memory mapping the source file %s. Failed with error %s", srcFile.Name(), err.Error()))
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.AddToBytesDone(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// log the transfer details.
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.Log(pipeline.LogInfo, fmt.Sprintf(" Source %s Destination %s Source Size %v is picked for processing",
			info.Source, info.Destination, info.SourceSize))
	}

	if EndsWith(info.Source, ".vhd") && (blobSize%azblob.PageBlobPageBytes == 0) {
		// step 3.b: If the Source is vhd file and its size is multiple of 512,
		// then upload the blob as a pageBlob.
		pageBlobUrl := blobUrl.ToPageBlobURL()

		// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
		// then set maximum pageSize will be 4 MB.
		chunkSize = common.Iffint64(
			chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
			common.DefaultPageBlobChunkSize,
			chunkSize)

		// Get http headers and meta data of page.
		blobHttpHeaders, metaData := jptm.BlobDstData(srcMmf)

		// Create Page Blob of the source size
		_, err := pageBlobUrl.Create(jptm.Context(), blobSize,
			0, blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf(" BlobUploadFailed failed since PageCreate failed due to %s", err.Error()))
			}
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			srcMmf.Unmap()
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
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("BlobUploadFailed failed since set blob-tier failed due to %s", err.Error()))
				}
				jptm.Cancel()
				jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				// Since transfer failed while setting the page blob tier
				// Deleting the created page blob
				_, err := pageBlobUrl.Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if err != nil {
					// Log the error if deleting the page blob failed.
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the page blob %s. Failed with error %s", pageBlobUrl, err.Error()))
					}
				}
				jptm.ReportTransferDone()
				srcMmf.Unmap()
				return
			}
		}

		// Calculate the number of Page Ranges for the given PageSize.
		numPages := common.Iffuint32(blobSize%chunkSize == 0,
			uint32(blobSize/chunkSize),
			uint32(blobSize/chunkSize)+1)

		jptm.SetNumberOfChunks(numPages)

		pbu := &pageBlobUpload{
			jptm:    jptm,
			srcMmf:  srcMmf,
			blobUrl: blobUrl,
			pacer:   pacer}

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
	} else if blobSize == 0 || blobSize <= chunkSize {
		// step 3.b: if blob size is smaller than chunk size and it is not a vhd file
		// we should do a put blob instead of chunk up the file
		PutBlobUploadFunc(jptm, srcMmf, blobUrl.ToBlockBlobURL(), pacer)
		return
	} else {
		// step 3.c: If the source is not a vhd and size is greater than chunk Size,
		// then uploading the source as block Blob.
		// calculating num of chunks using the source size and chunkSize.
		numChunks := common.Iffuint32(
			blobSize % chunkSize == 0,
			uint32(blobSize/chunkSize),
			uint32(blobSize/chunkSize)+1)

		// Set the number of chunk for the current transfer.
		jptm.SetNumberOfChunks(numChunks)

		// creating a slice to contain the blockIds
		blockIds := make([]string, numChunks)
		blockIdCount := int32(0)

		// creating block Blob struct which holds the srcFile, srcMmf memory map byte slice, pacer instance and blockId list.
		// Each chunk uses these details which uploading the block.
		bbu := &blockBlobUpload{
			jptm:     jptm,
			srcMmf:   srcMmf,
			blobURL:  blobUrl,
			pacer:    pacer,
			blockIds: blockIds}

		// go through the file and schedule chunk messages to upload each chunk
		for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
			adjustedChunkSize := chunkSize

			// compute actual size of the chunk
			if startIndex+chunkSize > blobSize {
				adjustedChunkSize = blobSize - startIndex
			}

			// schedule the chunk job/msg
			jptm.ScheduleChunks(bbu.blockBlobUploadFunc(blockIdCount, startIndex, adjustedChunkSize))

			blockIdCount += 1
		}
		// sanity check to verify the number of chunks scheduled
		if blockIdCount != int32(numChunks) {
			jptm.Panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, blockIdCount, info.Source, blobSize))
		}
	}
}

// This method blockBlobUploadFunc uploads the block of src data from given startIndex till the given chunkSize.
func (bbu *blockBlobUpload) blockBlobUploadFunc(chunkId int32, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {

		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		bbu.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer bbu.jptm.ReleaseAConnection()

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		defer func (jptm IJobPartTransferMgr) {
			r := recover()
			if r != nil {
				info := jptm.Info()
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startIndex %v chunkSize %v sourceMMF size %v",
						r, info.Source, info.Destination, info.SourceSize, startIndex, adjustedChunkSize, len(bbu.srcMmf)))
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
			}
		}(bbu.jptm)

		// and the chunkFunc has been changed to the version without param workId
		// transfer done is internal function which marks the transfer done, unmaps the src file and close the  source file.
		transferDone := func() {
			bbu.jptm.Log(pipeline.LogInfo, "Reported Done. Unmapping and cleanup in progress")
			bbu.srcMmf.Unmap()
			// Get the Status of the transfer
			// If the transfer status value < 0, then transfer failed with some failure
			// there is a possibility that some uncommitted blocks will be there
			// Delete the uncommitted blobs
			if bbu.jptm.TransferStatus() <= 0 {
				_, err := bbu.blobURL.ToBlockBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if stErr, ok := err.(azblob.StorageError); ok && stErr.Response().StatusCode != http.StatusNotFound {
					// If the delete failed with Status Not Found, then it means there were no uncommitted blocks.
					// Other errors report that uncommitted blocks are there
					if bbu.jptm.ShouldLog(pipeline.LogError) {
						bbu.jptm.Log(pipeline.LogError, fmt.Sprintf("error occurred while deleting the uncommitted "+
							"blocks of blob %s. Failed with error %s", bbu.blobURL.String(), err.Error()))
					}
				}
			}
			bbu.jptm.ReportTransferDone()
		}

		if bbu.jptm.WasCanceled() {
			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up chunkId %d", chunkId))
			}
			bbu.jptm.AddToBytesDone(adjustedChunkSize)
			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d finalizing cancellation of transfer", workerId))
				}
				transferDone()
			}
			return
		}
		// step 1: generate block ID
		blockId := common.NewUUID().String()
		encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

		// step 2: save the block ID into the list of block IDs
		(bbu.blockIds)[chunkId] = encodedBlockId

		// step 3: perform put block
		blockBlobUrl := bbu.blobURL.ToBlockBlobURL()
		body := newRequestBodyPacer(bytes.NewReader(bbu.srcMmf[startIndex:startIndex+adjustedChunkSize]), bbu.pacer)
		_, err := blockBlobUrl.StageBlock(bbu.jptm.Context(), encodedBlockId, body, azblob.LeaseAccessConditions{})
		if err != nil {
			// check if the transfer was cancelled while Stage Block was in process.
			if bbu.jptm.WasCanceled() {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which failed to upload chunkId %d because transfer was cancelled",
							workerId, chunkId))
				}
			} else {
				// cancel entire transfer because this chunk has failed
				bbu.jptm.Cancel()
				if bbu.jptm.ShouldLog(pipeline.LogError) {
					bbu.jptm.Log(pipeline.LogError,
						fmt.Sprintf(" BlobUploadFailed. worker %d is canceling transfer because upload of chunkId %d with startIndex %v and chunkSize %v failed with error %s",
							workerId, chunkId, startIndex, adjustedChunkSize, err.Error()))
				}
				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
			}

			//adding the chunk size to the bytes transferred to report the progress.
			bbu.jptm.AddToBytesDone(adjustedChunkSize)

			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d finalizing cancellation of transfer", workerId))
				}
				transferDone()
			}
			return
		}

		//adding the chunk size to the bytes transferred to report the progress.
		bbu.jptm.AddToBytesDone(adjustedChunkSize)

		// step 4: check if this is the last chunk
		if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
			// If the transfer gets cancelled before the putblock list
			if bbu.jptm.WasCanceled() {
				transferDone()
				return
			}
			// step 5: this is the last block, perform EPILOGUE
			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("has worker %d which is concluding upload transfer after processing chunkId %d with blocklist %s",
						workerId, chunkId, (bbu.blockIds)))
			}

			// fetching the blob http headers with content-type, content-encoding attributes
			// fetching the metadata passed with the JobPartOrder
			blobHttpHeader, metaData := bbu.jptm.BlobDstData(bbu.srcMmf)

			// commit the blocks.
			_, err := blockBlobUrl.CommitBlockList(bbu.jptm.Context(), bbu.blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
			if err != nil {
				if bbu.jptm.ShouldLog(pipeline.LogError) {
					bbu.jptm.Log(pipeline.LogError,
						fmt.Sprintf("BlobUploadFailed. worker %d failed to commit blockList with error %s",
							workerId, err.Error()))
				}
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
				transferDone()
				return
			}

			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo, "BlobUploadSuccessful. Commit block list completed successfully")
			}

			blockBlobTier, _ := bbu.jptm.BlobTiers()
			if blockBlobTier != common.EBlockBlobTier.None() {
				// for blob tier, set the latest service version from sdk as service version in the context.
				ctxWithValue := context.WithValue(bbu.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
				_, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType())
				if err != nil {
					if bbu.jptm.ShouldLog(pipeline.LogError) {
						bbu.jptm.Log(pipeline.LogError,
							fmt.Sprintf("BlobUploadFailed. worker %d failed to set tier %s on blob and failed with error %s",
								workerId, blockBlobTier, string(err.Error())))
					}
					bbu.jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				}
			}
			bbu.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
		}
	}
}

func PutBlobUploadFunc(jptm IJobPartTransferMgr, srcMmf common.MMF, blockBlobUrl azblob.BlockBlobURL, pacer *pacer) {

	// TODO: added the two operations for debugging purpose. remove later
	// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
	jptm.OccupyAConnection()
	// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
	defer jptm.ReleaseAConnection()

	// This function allows routine to manage behavior of unexpected panics.
	// The panic error along with transfer details are logged.
	// The transfer is marked as failed and is reported as done.
	defer func (jptm IJobPartTransferMgr) {
		r := recover()
		if r != nil {
			info := jptm.Info()
			lenSrcMmf := 0
			if info.SourceSize != 0 {
				lenSrcMmf = len(srcMmf)
			}
			if jptm.ShouldLog(pipeline.LogError) {
				jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v sourceMMF size %v",
					r, info.Source, info.Destination, info.SourceSize, lenSrcMmf))
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
		}
	}(jptm)

	// Get blob http headers and metadata.
	blobHttpHeader, metaData := jptm.BlobDstData(srcMmf)

	var err error

	// take care of empty blobs
	if jptm.Info().SourceSize == 0 {
		_, err = blockBlobUrl.Upload(jptm.Context(), bytes.NewReader(nil), blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	} else {
		body := newRequestBodyPacer(bytes.NewReader(srcMmf), pacer)
		_, err = blockBlobUrl.Upload(jptm.Context(), body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	}

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, " put blob failed because transfer was cancelled ")
			}
		} else {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobUploadFailed and cancelling the transfer. Failed with error %s", err.Error()))
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
		}
	} else {
		// if the put blob is a success, updating the transfer status to success
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, "put blob successful")
		}

		blockBlobTier, _ := jptm.BlobTiers()
		if blockBlobTier != common.EBlockBlobTier.None() {
			// for blob tier, set the latest service version from sdk as service version in the context.
			ctxWithValue := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
			_, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier.ToAccessTierType())
			if err != nil {
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError,
						fmt.Sprintf("BlobUploadFailed while seting tier %s on blob and failed with error %s", blockBlobTier, string(err.Error())))
				}
				jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				// since blob tier failed, the transfer failed
				// the blob created should be deleted
				_, err := blockBlobUrl.Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the blob %s. Failed with error %s", blockBlobUrl.String(), err.Error()))
					}
				}
			}
		}
		jptm.SetStatus(common.ETransferStatus.Success())
	}

	// adding the bytes transferred to report the progress of transfer.
	jptm.AddToBytesDone(jptm.Info().SourceSize)

	// updating number of transfers done for job part order
	jptm.ReportTransferDone()

	// close the memory map
	if jptm.Info().SourceSize != 0 {
		srcMmf.Unmap()
	}
}

func (pbu *pageBlobUpload) pageBlobUploadFunc(startPage int64, calculatedPageSize int64) chunkFunc {
	return func(workerId int) {
		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		pbu.jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer pbu.jptm.ReleaseAConnection()

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		defer func (jptm IJobPartTransferMgr) {
			r := recover()
			if r != nil {
				info := jptm.Info()
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startPage %v calculatedPageSize %v sourceMMF size %v",
						r, info.Source, info.Destination, info.SourceSize, startPage, calculatedPageSize, len(pbu.srcMmf)))
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
			}
		}(pbu.jptm)

		// pageDone is the function called after success / failure of each page.
		// If the calling page is the last page of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func() {
			// adding the page size to the bytes transferred.
			pbu.jptm.AddToBytesDone(calculatedPageSize)
			if lastPage, _ := pbu.jptm.ReportChunkDone(); lastPage {
				if pbu.jptm.ShouldLog(pipeline.LogInfo) {
					pbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is finalizing transfer", workerId))
				}
				pbu.jptm.SetStatus(common.ETransferStatus.Success())
				pbu.srcMmf.Unmap()
				// If the value of transfer Status is less than 0
				// transfer failed. Delete the page blob created
				if pbu.jptm.TransferStatus() <= 0 {
					// Deleting the created page blob
					_, err := pbu.blobUrl.ToPageBlobURL().Delete(context.TODO(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
					if err != nil {
						// Log the error if deleting the page blob failed.
						if pbu.jptm.ShouldLog(pipeline.LogInfo) {
							pbu.jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the page blob %s. Failed with error %s", pbu.blobUrl, err.Error()))
						}
					}
				}
				pbu.jptm.ReportTransferDone()
			}
		}

		if pbu.jptm.WasCanceled() {
			if pbu.jptm.ShouldLog(pipeline.LogInfo) {
				pbu.jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("is cancelled. Hence not picking up page %d", startPage))
			}
			pageDone()
		} else {
			// pageBytes is the byte slice of Page for the given page range
			pageBytes := pbu.srcMmf[startPage : startPage+calculatedPageSize]
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
				if pbu.jptm.ShouldLog(pipeline.LogInfo) {
					pbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is not performing PutPages for Page range from %d to %d since all the bytes are zero", workerId, startPage, startPage+calculatedPageSize))
				}
				pageDone()
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(pageBytes), pbu.pacer)
			pageBlobUrl := pbu.blobUrl.ToPageBlobURL()
			_, err := pageBlobUrl.UploadPages(pbu.jptm.Context(), startPage, body, azblob.BlobAccessConditions{})
			if err != nil {
				if pbu.jptm.WasCanceled() {
					if pbu.jptm.ShouldLog(pipeline.LogInfo) {
						pbu.jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because transfer was cancelled", workerId, startPage, startPage+calculatedPageSize))
					}
				} else {
					if pbu.jptm.ShouldLog(pipeline.LogInfo) {
						pbu.jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("BlobUploadFailed. worker %d failed to Put Page range from %d to %d because of following error %s", workerId, startPage, startPage+calculatedPageSize, err.Error()))
					}
					// cancelling the transfer
					pbu.jptm.Cancel()
					pbu.jptm.SetStatus(common.ETransferStatus.Failed())
				}
				pageDone()
				return
			}
			if pbu.jptm.ShouldLog(pipeline.LogInfo) {
				pbu.jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("has workedId %d which successfully complete PUT page request from range %d to %d", workerId, startPage, startPage+calculatedPageSize))
			}
			pageDone()
		}
	}
}
