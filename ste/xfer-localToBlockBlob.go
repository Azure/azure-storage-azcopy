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
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"unsafe"
)

type blockBlobUpload struct {
	jptm     IJobPartTransferMgr
	srcFile  *os.File
	srcMmf   common.MMF
	blobURL  azblob.BlobURL
	pacer    *pacer
	blockIds *[]string
}

type pageBlobUpload struct {
	jptm    IJobPartTransferMgr
	srcFile *os.File
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

	// step 2a: Open the Source File.
	srcFile, err := os.Open(info.Source)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("error opening the source file %s", info.SourceSize))
		}
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.AddToBytesTransferred(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	// 2b: Memory map the source file. If the file size if not greater than 0, then doesn't memory map the file.
	var srcMmf common.MMF
	if blobSize > 0 {
		// file needs to be memory mapped only when the file size is greater than 0.
		srcMmf, err = common.NewMMF(srcFile, false, 0, blobSize)
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("error memory mapping the source file %s. Failed with error %s", srcFile.Name(), err.Error()))
			}
			srcFile.Close()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.AddToBytesTransferred(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// step 3.a: if blob size is smaller than chunk size and it is not a vhd file
	// we should do a put blob instead of chunk up the file
	if !EndsWith(info.Source, ".vhd") && (blobSize == 0 || blobSize <= chunkSize) {
		PutBlobUploadFunc(jptm, srcFile, srcMmf, blobUrl, pacer)
		return
	} else if EndsWith(info.Source, ".vhd") && (blobSize%512 == 0) {
		// step 3.b: If the Source is vhd file and its size is multiple of 512,
		// then upload the blob as a pageBlob.
		pageBlobUrl := blobUrl.ToPageBlobURL()

		// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
		// then set maximum pageSize will be 4 MB.
		if chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%512 != 0) {
			chunkSize = common.DefaultPageBlobChunkSize
		}

		// Get http headers and meta data of page.
		blobHttpHeaders, metaData := jptm.BlobDstData(srcMmf)

		// Create Page Blob of the source size
		jptm.AddToBytesOverWire(uint64(blobSize))
		_, err := pageBlobUrl.Create(jptm.Context(), blobSize,
			0, blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("failed since PageCreate failed due to %s", err.Error()))
			}
			jptm.Cancel()
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			srcMmf.Unmap()
			err = srcFile.Close()
			if err != nil {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("got an error while closing file % because of %s", srcFile.Name(), err.Error()))
				}
			}
			return
		}

		//set tier on pageBlob.
		//If set tier fails, then cancelling the job.
		_, pageBlobTier := jptm.BlobTiers()
		if pageBlobTier != azblob.AccessTierNone {
			ctxWithValue := context.WithValue(jptm.Context(), overwriteServiceVersionString, "false")
			setTierResp, err := pageBlobUrl.SetTier(ctxWithValue, pageBlobTier)
			if err != nil {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("failed since set blob-tier failed due to %s", err.Error()))
				}
				jptm.Cancel()
				jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				jptm.ReportTransferDone()
				srcMmf.Unmap()
				err = srcFile.Close()
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("got an error while closing file % because of %s", srcFile.Name(), err.Error()))
					}
				}
				return
			}
			if setTierResp != nil {
				io.Copy(ioutil.Discard, setTierResp.Response().Body)
				setTierResp.Response().Body.Close()
			}
		}

		// Calculate the number of Page Ranges for the given PageSize.
		numPages := uint32(0)
		if rem := blobSize % chunkSize; rem == 0 {
			numPages = uint32(blobSize / chunkSize)
		} else {
			numPages = uint32(blobSize/chunkSize) + 1
		}

		jptm.SetNumberOfChunks(numPages)
		pbu := &pageBlobUpload{
			jptm:    jptm,
			srcFile: srcFile,
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
	} else {
		// step 3.c: If the source is not a vhd and size is greater than chunk Size,
		// then uploading the source as block Blob.

		// calculating num of chunks using the source size and chunkSize.
		numChunks := uint32(0)
		if rem := info.SourceSize % int64(info.BlockSize); rem == 0 {
			numChunks = uint32(info.SourceSize / int64(info.BlockSize))
		} else {
			numChunks = uint32(info.SourceSize/int64(info.BlockSize)) + 1
		}

		// Set the number of chunk for the current transfer.
		jptm.SetNumberOfChunks(numChunks)

		// creating a slice to contain the blockIds
		blockIds := make([]string, numChunks)
		blockIdCount := int32(0)

		// creating block Blob struct which holds the srcFile, srcMmf memory map byte slice, pacer instance and blockId list.
		// Each chunk uses these details which uploading the block.
		bbu := &blockBlobUpload{
			jptm:     jptm,
			srcFile:  srcFile,
			srcMmf:   srcMmf,
			blobURL:  blobUrl,
			pacer:    pacer,
			blockIds: &blockIds}

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
	}
}

// This method blockBlobUploadFunc uploads the block of src data from given startIndex till the given chunkSize.
func (bbu *blockBlobUpload) blockBlobUploadFunc(chunkId int32, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {
		// and the chunkFunc has been changed to the version without param workId
		// transfer done is internal function which marks the transfer done, unmaps the src file and close the  source file.
		transferDone := func() {
			bbu.jptm.ReportTransferDone()

			bbu.srcMmf.Unmap()
			err := bbu.srcFile.Close()
			if err != nil {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %v which failed to close the file because of following error %s",
							workerId, err.Error()))
				}
			}
		}
		if bbu.jptm.WasCanceled() {
			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up chunkId %d", chunkId))
				bbu.jptm.AddToBytesTransferred(adjustedChunkSize)
			}
			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d is finalizing cancellation of transfer", workerId))
				}
				transferDone()
			}
			return
		}
		// step 1: generate block ID
		blockId := common.NewUUID().String()
		encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

		// step 2: save the block ID into the list of block IDs
		(*bbu.blockIds)[chunkId] = encodedBlockId

		// adding the adjustedChunkSize to bytesOverWire for throughput.
		bbu.jptm.AddToBytesOverWire(uint64(adjustedChunkSize))

		// step 3: perform put block
		blockBlobUrl := bbu.blobURL.ToBlockBlobURL()
		body := newRequestBodyPacer(bytes.NewReader(bbu.srcMmf[startIndex:startIndex+adjustedChunkSize]), bbu.pacer)
		putBlockResponse, err := blockBlobUrl.StageBlock(bbu.jptm.Context(), encodedBlockId, body, azblob.LeaseAccessConditions{})
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
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is canceling transfer because upload of chunkId %d because startIndex of %d has failed",
							workerId, chunkId, startIndex))
				}
				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
			}
			//adding the chunk size to the bytes transferred to report the progress.
			bbu.jptm.AddToBytesTransferred(adjustedChunkSize)

			if lastChunk, _ := bbu.jptm.ReportChunkDone(); lastChunk {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d is finalizing cancellation of transfer", workerId))
				}
				transferDone()
			}
			return
		}

		// closing the resp body.
		if putBlockResponse != nil &&
			putBlockResponse.Response() != nil {
			io.Copy(ioutil.Discard, putBlockResponse.Response().Body)
			putBlockResponse.Response().Body.Close()
		}

		//adding the chunk size to the bytes transferred to report the progress.
		bbu.jptm.AddToBytesTransferred(adjustedChunkSize)

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
					fmt.Sprintf("has worker %d which is concluding download transfer after processing chunkId %d with blocklist %s",
						workerId, chunkId, (*bbu.blockIds)))
			}

			// fetching the blob http headers with content-type, content-encoding attributes
			// fetching the metadata passed with the JobPartOrder
			blobHttpHeader, metaData := bbu.jptm.BlobDstData(bbu.srcMmf)

			bbu.jptm.AddToBytesOverWire(uint64(len(blockId) * len(*bbu.blockIds)))

			// commit the blocks.
			putBlockListResponse, err := blockBlobUrl.CommitBlockList(bbu.jptm.Context(), *bbu.blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
			if err != nil {
				if bbu.jptm.ShouldLog(pipeline.LogInfo) {
					bbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which failed to conclude Transfer after processing chunkId %d due to error %s",
							workerId, chunkId, string(err.Error())))
				}
				bbu.jptm.SetStatus(common.ETransferStatus.Failed())
				transferDone()
				return
			}

			// close the resp body.
			if putBlockListResponse != nil &&
				putBlockListResponse.Response() != nil {
				io.Copy(ioutil.Discard, putBlockListResponse.Response().Body)
				putBlockListResponse.Response().Body.Close()
			}

			if bbu.jptm.ShouldLog(pipeline.LogInfo) {
				bbu.jptm.Log(pipeline.LogInfo, "completed successfully")
			}
			blockBlobTier, _ := bbu.jptm.BlobTiers()
			if blockBlobTier != azblob.AccessTierNone {
				ctxWithValue := context.WithValue(bbu.jptm.Context(), overwriteServiceVersionString, "false")
				setTierResp, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier)
				if err != nil {
					if bbu.jptm.ShouldLog(pipeline.LogError) {
						bbu.jptm.Log(pipeline.LogError,
							fmt.Sprintf("has worker %d which failed to set tier %s on blob and failed with error %s",
								workerId, blockBlobTier, string(err.Error())))
					}
					bbu.jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
				}
				if setTierResp != nil {
					io.Copy(ioutil.Discard, setTierResp.Response().Body)
					setTierResp.Response().Body.Close()
				}
			}
			bbu.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
		}
	}
}

func PutBlobUploadFunc(jptm IJobPartTransferMgr, srcFile *os.File, srcMmf common.MMF,
	blobURL azblob.BlobURL, pacer *pacer) {

	// transform blobURL and perform put blob operation
	blockBlobUrl := blobURL.ToBlockBlobURL()
	blobHttpHeader, metaData := jptm.BlobDstData(srcMmf)

	var uploadBlobResp *azblob.BlockBlobsUploadResponse
	var err error

	// add blobSize to bytesOverWire.
	jptm.AddToBytesOverWire(uint64(jptm.Info().SourceSize))
	// take care of empty blobs
	if jptm.Info().SourceSize == 0 {
		uploadBlobResp, err = blockBlobUrl.Upload(jptm.Context(), nil, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	} else {
		body := newRequestBodyPacer(bytes.NewReader(srcMmf), pacer)
		uploadBlobResp, err = blockBlobUrl.Upload(jptm.Context(), body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	}

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		if jptm.WasCanceled() {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, " put blob failed because transfer was cancelled ")
			}
		} else {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" put blob failed and cancelling the transfer. Failed with error %s", err.Error()))
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
		}
	} else {
		// if the put blob is a success, updating the transfer status to success
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, "put blob successful")
		}

		blockBlobTier, _ := jptm.BlobTiers()
		if blockBlobTier != azblob.AccessTierNone {
			ctxWithValue := context.WithValue(jptm.Context(), overwriteServiceVersionString, "false")
			setTierResp, err := blockBlobUrl.SetTier(ctxWithValue, blockBlobTier)
			if err != nil {
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError,
						fmt.Sprintf(" failed to set tier %s on blob and failed with error %s", blockBlobTier, string(err.Error())))
				}
				jptm.SetStatus(common.ETransferStatus.BlobTierFailure())
			}
			if setTierResp != nil {
				io.Copy(ioutil.Discard, setTierResp.Response().Body)
				setTierResp.Response().Body.Close()
			}
		}
		jptm.SetStatus(common.ETransferStatus.Success())
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
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo,
					fmt.Sprintf("has worker which failed to close the file because of following error %s", err.Error()))
			}
		}
	}
	// closing the put blob response body
	if uploadBlobResp != nil {
		io.Copy(ioutil.Discard, uploadBlobResp.Response().Body)
		uploadBlobResp.Response().Body.Close()
	}
}

func (pbu *pageBlobUpload) pageBlobUploadFunc(startPage int64, pageSize int64) chunkFunc {
	return func(workerId int) {
		// pageDone is the function called after success / failure of each page.
		// If the calling page is the last page of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func() {
			// adding the page size to the bytes transferred.
			pbu.jptm.AddToBytesTransferred(pageSize)
			if lastPage, _ := pbu.jptm.ReportChunkDone(); lastPage {
				if pbu.jptm.ShouldLog(pipeline.LogInfo) {
					pbu.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf("has worker %d which is finalizing transfer", workerId))
				}
				pbu.jptm.SetStatus(common.ETransferStatus.Success())
				pbu.jptm.ReportTransferDone()
				pbu.srcMmf.Unmap()
				err := pbu.srcFile.Close()
				if err != nil {
					if pbu.jptm.ShouldLog(pipeline.LogInfo) {
						pbu.jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("got an error while closing file % because of %s", pbu.srcFile.Name(), err.Error()))
					}
				}
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
			pageBytes := pbu.srcMmf[startPage : startPage+pageSize]
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
						fmt.Sprintf("has worker %d which is not performing PutPages for Page range from %d to %d since all the bytes are zero", workerId, startPage, startPage+pageSize))
				}
				pageDone()
				return
			}

			//adding pageSize to bytesOverWire for throughput.
			pbu.jptm.AddToBytesOverWire(uint64(pageSize))
			body := newRequestBodyPacer(bytes.NewReader(pageBytes), pbu.pacer)
			pageBlobUrl := pbu.blobUrl.ToPageBlobURL()
			resp, err := pageBlobUrl.UploadPages(pbu.jptm.Context(), startPage, body, azblob.BlobAccessConditions{})
			if err != nil {
				if pbu.jptm.WasCanceled() {
					if pbu.jptm.ShouldLog(pipeline.LogInfo) {
						pbu.jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because transfer was cancelled", workerId, startPage, startPage+pageSize))
					}
				} else {
					if pbu.jptm.ShouldLog(pipeline.LogInfo) {
						pbu.jptm.Log(pipeline.LogInfo,
							fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because of following error %s", workerId, startPage, startPage+pageSize, err.Error()))
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
					fmt.Sprintf("has workedId %d which successfully complete PUT page request from range %d to %d", workerId, startPage, startPage+pageSize))
			}
			//closing the page upload response.
			if resp != nil && resp.Response() != nil {
				io.Copy(ioutil.Discard, resp.Response().Body)
				resp.Response().Body.Close()
			}
			pageDone()
		}
	}
}
