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
	"net/url"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"os"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"encoding/base64"
	"bytes"
	"unsafe"
	"strings"
	"io"
	"io/ioutil"
)

func LocalToBlockBlob(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	EndsWith := func(s string, t string) bool{
		return len(s) >= len(t) && strings.EqualFold(s[len(s)-len(t):], t)
	}

	info := jptm.Info()

	u, _ := url.Parse(info.Destination)
	blobUrl := azblob.NewBlobURL(*u, p)

	// step 2: get size info from transfer
	blobSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	// step 3: map in the file to upload before transferring chunks
	srcFile, err := os.Open(info.Source)
	if err != nil{
		if jptm.ShouldLog(pipeline.LogInfo){
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error opening the source file %s", info.SourceSize))
		}
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.AddToBytesTransferred(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}
	srcFileInfo, err := srcFile.Stat()
	if err != nil{
		if jptm.ShouldLog(pipeline.LogInfo){
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error getting the source file Info of file %s", info.SourceSize))
		}
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.AddToBytesTransferred(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	var srcMmf common.MMF
	if srcFileInfo.Size() > 0 {
		// file needs to be memory mapped only when the file size is greater than 0.
		srcMmf, err = common.NewMMF(srcFile, false, 0, srcFileInfo.Size())
		if err != nil{
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("error memory mapping the source file %s. Failed with error %s", srcFile.Name(), err.Error()))
			}
			srcFile.Close()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.AddToBytesTransferred(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	if EndsWith(info.Source, ".vhd") && (blobSize % 512 == 0){
		pageBlobUrl := blobUrl.ToPageBlobURL()
		// todo: remove the hard coded chunk size in case of pageblob
		chunkSize = (4* 1024 * 1024)
		// step 2: Get http headers and meta data of page.
		blobHttpHeaders, metaData := jptm.BlobDstData(srcMmf)

		// step 3: Create Page Blob of the source size
		jptm.AddToBytesOverWire(uint64(blobSize))
		_, err := pageBlobUrl.Create(jptm.Context(), blobSize,
			0, blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("failed since PageCreate failed due to %s", err.Error()))
			}
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			srcMmf.Unmap()
			err = srcFile.Close()
			if err != nil {
				if jptm.ShouldLog(pipeline.LogInfo){
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("got an error while closing file % because of %s", srcFile.Name(), err.Error()))
				}
			}
			return
		}

		//set tier on pageBlob.
		//If set tier fails, then cancelling the job.
		if len(jptm.BlobTier()) > 0 {
			setTierResp, err := pageBlobUrl.SetTier(jptm.Context(), azblob.AccessTierType(jptm.BlobTier()))
				if err != nil{
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("failed since set blob-tier failed due to %s", err.Error()))
					}
					jptm.Cancel()
					jptm.SetStatus(common.ETransferStatus.Failed())
					jptm.ReportTransferDone()
					srcMmf.Unmap()
					err = srcFile.Close()
					if err != nil {
						if jptm.ShouldLog(pipeline.LogInfo){
							jptm.Log(pipeline.LogInfo,
								fmt.Sprintf("got an error while closing file % because of %s", srcFile.Name(), err.Error()))
						}
					}
				return
			}
			if setTierResp != nil{
				io.Copy(ioutil.Discard, setTierResp.Response().Body)
				setTierResp.Response().Body.Close()
			}
		}

		numPages := uint32(0)
		if rem := blobSize % chunkSize; rem == 0 {
			numPages = uint32(blobSize / chunkSize)
		}else{
			numPages = uint32(blobSize / chunkSize) + 1
		}

		jptm.SetNumberOfChunks(numPages)
		// step 4: Scheduling page range update to the Page Blob created in Step 3
		for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
			adjustedPageSize := chunkSize

			// compute actual size of the chunk
			if startIndex+ chunkSize > blobSize {
				adjustedPageSize = blobSize - startIndex
			}

			// schedule the chunk job/msg
			jptm.ScheduleChunks(pageBlobUploadFunc(jptm, srcFile, srcMmf, blobUrl, pacer, startIndex, adjustedPageSize))
		}
	}	else if blobSize == 0 || blobSize <= chunkSize {
			// step 4.a: if blob size is smaller than chunk size, we should do a put blob instead of chunk up the file
			PutBlobUploadFunc(jptm, srcFile, srcMmf, blobUrl, pacer)
			return
	}	else{
			// step 4.b: get the number of blocks and create a slice to hold the blockIDs of each chunk
			numChunks := uint32(0)
			if rem := info.SourceSize % int64(info.BlockSize); rem == 0{
				numChunks = uint32(info.SourceSize / int64(info.BlockSize))
			}else{
				numChunks = uint32(info.SourceSize / int64(info.BlockSize)) + 1
			}
			jptm.SetNumberOfChunks(numChunks)

			blockIds := make([]string, numChunks)
			blockIdCount := int32(0)

			// step 5: go through the file and schedule chunk messages to upload each chunk
			for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
				adjustedChunkSize := chunkSize

				// compute actual size of the chunk
				if startIndex+chunkSize > blobSize {
					adjustedChunkSize = blobSize - startIndex
				}

				// schedule the chunk job/msg
				jptm.ScheduleChunks(blockBlobUploadFunc(jptm, srcFile, srcMmf, blobUrl, pacer, blockIds, blockIdCount, startIndex, adjustedChunkSize))

				blockIdCount += 1
			}
	}
}

// this generates a function which performs the uploading of a single chunk
func blockBlobUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, srcMmf common.MMF, blobURL azblob.BlobURL,
	pacer *pacer, blockIds []string, chunkId int32, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {
		// and the chunkFunc has been changed to the version without param workId
		// transfer done is internal function which marks the transfer done, unmaps the src file and close the  source file.
		transferDone := func() {
			jptm.ReportTransferDone()

			srcMmf.Unmap()
			err := srcFile.Close()
			if err != nil {
				if jptm.ShouldLog(pipeline.LogInfo){
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %v which failed to close the file because of following error %s",
							workerId, err.Error()))
				}
			}
		}
		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up chunkId %d", chunkId))
				jptm.AddToBytesTransferred(adjustedChunkSize)
				transferDone()
			}
		} else {
			// step 1: generate block ID
			blockId := common.NewUUID().String()
			encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

			// step 2: save the block ID into the list of block IDs
			blockIds[chunkId] = encodedBlockId

			// step 3: perform put block
			// adding the adjustedChunkSize to bytesOverWire for throughput.
			jptm.AddToBytesOverWire(uint64(adjustedChunkSize))

			blockBlobUrl := blobURL.ToBlockBlobURL()

			body := newRequestBodyPacer(bytes.NewReader(srcMmf[startIndex:startIndex+adjustedChunkSize]), pacer)
			putBlockResponse, err := blockBlobUrl.StageBlock(jptm.Context(), encodedBlockId, body, azblob.LeaseAccessConditions{})

			if err != nil {
				if jptm.WasCanceled(){
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to upload chunkId %d because transfer was cancelled",
								workerId, chunkId))
					}
				} else {
					// cancel entire transfer because this chunk has failed
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which is canceling transfer because upload of chunkId %d because startIndex of %d has failed",
								workerId, chunkId, startIndex))
					}
					//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				//adding the chunk size to the bytes transferred to report the progress.
				jptm.AddToBytesTransferred(adjustedChunkSize)

				if  lastChunk, _ := jptm.ReportChunkDone(); lastChunk {
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d is finalizing cancellation of transfer", workerId))
					}
					transferDone()
				}
				return
			}

			if putBlockResponse != nil {
				io.Copy(ioutil.Discard, putBlockResponse.Response().Body)
				putBlockResponse.Response().Body.Close()
			}

			//adding the chunk size to the bytes transferred to report the progress.
			jptm.AddToBytesTransferred(adjustedChunkSize)

			// step 4: check if this is the last chunk
			if  lastChunk, _ := jptm.ReportChunkDone(); lastChunk {
				// If the transfer gets cancelled before the putblock list
				if jptm.WasCanceled() {
					transferDone()
					return
				}
				// step 5: this is the last block, perform EPILOGUE
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is concluding download transfer after processing chunkId %d with blocklist %s",
							workerId, chunkId, blockIds))
				}

				// fetching the blob http headers with content-type, content-encoding attributes
				// fetching the metadata passed with the JobPartOrder
				blobHttpHeader, metaData := jptm.BlobDstData(srcMmf)

				jptm.AddToBytesOverWire(uint64(len(blockId) * len(blockIds)))

				putBlockListResponse, err := blockBlobUrl.CommitBlockList(jptm.Context(), blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to conclude Transfer after processing chunkId %d due to error %s",
								workerId, chunkId, string(err.Error())))
					}
					jptm.SetStatus(common.ETransferStatus.Failed())
					transferDone()
					return
				}

				if putBlockListResponse != nil {
					io.Copy(ioutil.Discard, putBlockResponse.Response().Body)
					putBlockListResponse.Response().Body.Close()
				}

				if jptm.ShouldLog(pipeline.LogInfo){
					jptm.Log(pipeline.LogInfo, "completed successfully")
				}
				if len(jptm.BlobTier()) > 0 {
					setTierResp , err := blockBlobUrl.SetTier(jptm.Context(), azblob.AccessTierType(jptm.BlobTier()))
					if err != nil{
						if jptm.ShouldLog(pipeline.LogError){
							jptm.Log(pipeline.LogError,
								fmt.Sprintf("has worker %d which failed to set tier %s on blob and failed with error %s",
									workerId, jptm.BlobTier(), string(err.Error())))
						}
					}
					if setTierResp != nil{
						io.Copy(ioutil.Discard, setTierResp.Response().Body)
						setTierResp.Response().Body.Close()
					}
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				transferDone()
			}
		}
	}
}

func PutBlobUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, srcMmf common.MMF,
	blobURL azblob.BlobURL, pacer *pacer) {

	// transform blobURL and perform put blob operation
	blockBlobUrl := blobURL.ToBlockBlobURL()
	blobHttpHeader, metaData := jptm.BlobDstData(srcMmf)

	var putBlobResp *azblob.BlobsPutResponse
	var err error

	// add blobSize to bytesOverWire.
	jptm.AddToBytesOverWire(uint64(jptm.Info().SourceSize))
	// take care of empty blobs
	if jptm.Info().SourceSize == 0 {
		putBlobResp, err = blockBlobUrl.Upload(jptm.Context(), nil, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	} else {
		body := newRequestBodyPacer(bytes.NewReader(srcMmf), pacer)
		putBlobResp, err = blockBlobUrl.Upload(jptm.Context(), body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	}

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		if jptm.WasCanceled(){
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo, " put blob failed because transfer was cancelled ")
			}
		}else{
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" put blob failed and cancelling the transfer. Failed with error %s", err.Error()))
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
		}
	} else {
		// if the put blob is a success, updating the transfer status to success
		if jptm.ShouldLog(pipeline.LogInfo){
			jptm.Log(pipeline.LogInfo, "put blob successful")
		}

		jptm.SetStatus(common.ETransferStatus.Success())
	}

	if len(jptm.BlobTier()) > 0 {
		setTierResp, err := blockBlobUrl.SetTier(jptm.Context(), azblob.AccessTierType(jptm.BlobTier()))
		if err != nil{
			if jptm.ShouldLog(pipeline.LogError){
				jptm.Log(pipeline.LogError,
					fmt.Sprintf(" failed to set tier %s on blob and failed with error %s", jptm.BlobTier(), string(err.Error())))
			}
		}
		if setTierResp != nil{
			io.Copy(ioutil.Discard, setTierResp.Response().Body)
			setTierResp.Response().Body.Close()
		}
	}

	// adding the bytes transferred to report the progress of transfer.
	jptm.AddToBytesTransferred(jptm.Info().SourceSize)
	// updating number of transfers done for job part order
	jptm.ReportTransferDone()

	// perform clean up for the case where blob size is not 0
	if jptm.Info().SourceSize != 0 {
		//jptm.jobInfo.JobThroughPut.updateCurrentBytes(int64(localToBlockBlob.jptm.SourceSize))

		srcMmf.Unmap()
		err = srcFile.Close()
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("has worker which failed to close the file because of following error %s", err.Error()))
			}
		}
	}
	// closing the put blob response body
	if putBlobResp != nil {
		io.Copy(ioutil.Discard, putBlobResp.Response().Body)
		putBlobResp.Response().Body.Close()
	}
}

func pageBlobUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, srcMmf common.MMF, blobUrl azblob.BlobURL, pacer *pacer, startPage int64, pageSize int64) chunkFunc {
	return func(workerId int) {
		// pageDone is the function called after success / failure of each page.
		// If the calling page is the last page of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func() {
			// adding the page size to the bytes transferred.
			jptm.AddToBytesTransferred(pageSize)
			if lastPage, _ := jptm.ReportChunkDone(); lastPage {
				if jptm.ShouldLog(pipeline.LogInfo){
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is finalizing transfer", workerId))
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				jptm.ReportTransferDone()
				srcMmf.Unmap()
				err := srcFile.Close()
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("got an error while closing file % because of %s", srcFile.Name(), err.Error()))
					}
				}
			}
		}

		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("is cancelled. Hence not picking up page %d", startPage))
			}
			pageDone()
		} else {
			// pageBytes is the byte slice of Page for the given page range
			pageBytes := srcMmf[startPage : startPage+pageSize]
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
				if jptm.ShouldLog(pipeline.LogInfo){
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is not performing PutPages for Page range from %d to %d since all the bytes are zero", workerId, startPage, startPage+pageSize))
				}
				pageDone()
				return
			}

			//adding pageSize to bytesOverWire for throughput.
			jptm.AddToBytesOverWire(uint64(pageSize))
			body := newRequestBodyPacer(bytes.NewReader(pageBytes), pacer)
			pageBlobUrl := blobUrl.ToPageBlobURL()
			resp, err := pageBlobUrl.UploadPages(jptm.Context(), startPage, body, azblob.BlobAccessConditions{})
			if err != nil {
				if jptm.WasCanceled() {
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because transfer was cancelled", workerId, startPage, startPage+pageSize))
					}
				} else {
					if jptm.ShouldLog(pipeline.LogInfo){
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because of following error %s", workerId, startPage, startPage+pageSize, err.Error()))
					}
					// cancelling the transfer
					jptm.Cancel()
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				pageDone()
				return
			}
			if jptm.ShouldLog(pipeline.LogInfo){
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("has workedId %d which successfully complete PUT page request from range %d to %d", workerId, startPage, startPage+pageSize))
			}
			//closing the page upload response.
			if resp != nil{
				io.Copy(ioutil.Discard, resp.Response().Body)
				resp.Response().Body.Close()
			}
			pageDone()
		}
	}
}