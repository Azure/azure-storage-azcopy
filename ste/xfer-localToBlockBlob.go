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

/*
import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"net/url"
	"os"
)

// this struct is created for each transfer
type localToBlockBlob struct {
	jptm             IJobPartTransferMgr
	blobURL          azblob.BlobURL
	memoryMappedFile common.MMF
	blockIds         []string
	srcFileHandler   *os.File
}

// return a new localToBlockBlob struct targeting a specific transfer
func newLocalToBlockBlob(jptm IJobPartTransferMgr) {
	localToBlockBlob := &localToBlockBlob{jptm: jptm}

	// step 1: create pipeline for the destination blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      UploadMaxTries,
			TryTimeout:    UploadTryTimeout,
			RetryDelay:    UploadRetryDelay,
			MaxRetryDelay: UploadMaxRetryDelay,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {

				localToBlockBlob.jptm.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(localToBlockBlob.jptm.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(localToBlockBlob.jptm.Destination)
	localToBlockBlob.blobURL = azblob.NewBlobURL(*u, p)

	// step 2: get size info from transfer
	blobSize := int64(localToBlockBlob.jptm.SourceSize)
	chunkSize := int64(localToBlockBlob.jptm.BlockSize)

	// step 3: map in the file to upload before transferring chunks
	if blobSize > 0 {
		localToBlockBlob.memoryMappedFile, localToBlockBlob.srcFileHandler = executionEngineHelper{}.openAndMemoryMapFile(localToBlockBlob.transfer.Source)
	}

	// step 4.a: if blob size is smaller than chunk size, we should do a put blob instead of chunk up the file
	if blobSize == 0 || blobSize <= chunkSize {
		localToBlockBlob.putBlob()
		return
	}

	// step 4.b: get the number of blocks and create a slice to hold the blockIDs of each chunk
	localToBlockBlob.blockIds = make([]string, localToBlockBlob.jptm.NumChunks)
	blockIdCount := int32(0)

	// step 5: go through the file and schedule chunk messages to upload each chunk
	for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
		adjustedChunkSize := chunkSize

		// compute actual size of the chunk
		if startIndex+chunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: localToBlockBlob.generateUploadFunc(
				blockIdCount, // this is the index of the chunk
				adjustedChunkSize,
				startIndex),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the uploading of a single chunk
func (localToBlockBlob *localToBlockBlob) generateUploadFunc(chunkId int32, adjustedChunkSize int64, startIndex int64) chunkFunc {
	return func() {
		// TODO: remove the mockNum, workerId is not necessary considering  similar thing as gorountine Id is not necessary for debugging
		// and the chunkFunc has been changed to the version without param workId
		mockNum := 0
		workerId := mockNum

		totalNumOfChunks := uint32(localToBlockBlob.jptm.NumChunks)
		transferDone := func() {
			localToBlockBlob.jptm.TransferDone()
			localToBlockBlob.memoryMappedFile.Unmap()

			err := localToBlockBlob.srcFileHandler.Close()
			if err != nil {
				localToBlockBlob.jptm.Log(common.LogError,
					fmt.Sprintf("has worker %v which failed to close the file because of following error %s",
						workerId, err.Error()))
			}
		}
		if localToBlockBlob.jptm.TransferContext.Err() != nil {
			localToBlockBlob.jptm.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up chunkId %d", chunkId))
			transferDone()
		} else {
			// step 1: generate block ID
			blockId := common.NewUUID().String()
			encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

			// step 2: save the block ID into the list of block IDs
			localToBlockBlob.blockIds[chunkId] = encodedBlockId

			// step 3: perform put block
			blockBlobUrl := localToBlockBlob.blobURL.ToBlockBlobURL()

			body := newRequestBodyPacer(bytes.NewReader(localToBlockBlob.memoryMappedFile[startIndex:startIndex+adjustedChunkSize]), localToBlockBlob.pacer)
			putBlockResponse, err := blockBlobUrl.StageBlock(localToBlockBlob.jptm.TransferContext, encodedBlockId, body, azblob.LeaseAccessConditions{})

			if err != nil {
				if localToBlockBlob.jptm.TransferContext.Err() != nil {
					localToBlockBlob.jptm.Log(common.LogInfo,
						fmt.Sprintf("has worker %d which failed to upload chunkId %d because transfer was cancelled",
							workerId, chunkId))
				} else {
					// cancel entire transfer because this chunk has failed
					localToBlockBlob.jptm.TransferCancelFunc()
					localToBlockBlob.jptm.Log(common.LogInfo,
						fmt.Sprintf("has worker %d which is canceling transfer because upload of chunkId %d because startIndex of %d has failed",
							workerId, chunkId, startIndex))

					//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
					localToBlockBlob.jptm.TransferStatus(common.TransferFailed)
				}
				if localToBlockBlob.jptm.ChunksDone() == totalNumOfChunks {
					localToBlockBlob.jptm.Log(common.LogInfo,
						fmt.Sprintf("has worker %d is finalizing cancellation of transfer", workerId))
					transferDone()
				}
				return
			}

			if putBlockResponse != nil {
				putBlockResponse.Response().Body.Close()
			}

			localToBlockBlob.jptm.jobInfo.JobThroughPut.updateCurrentBytes(adjustedChunkSize)

			// step 4: check if this is the last chunk
			if localToBlockBlob.jptm.ChunksDone() == totalNumOfChunks {
				// If the transfer gets cancelled before the putblock list
				if localToBlockBlob.jptm.TransferContext.Err() != nil {
					transferDone()
					return
				}
				// step 5: this is the last block, perform EPILOGUE
				localToBlockBlob.jptm.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is concluding download transfer after processing chunkId %d with blocklist %s",
						workerId, chunkId, localToBlockBlob.blockIds))

				// fetching the blob http headers with content-type, content-encoding attributes
				// fetching the metadata passed with the JobPartOrder
				blobHttpHeader, metaData := localToBlockBlob.jptm.blobHttpHeaderAndMetadata(localToBlockBlob.memoryMappedFile)

				putBlockListResponse, err := blockBlobUrl.CommitBlockList(localToBlockBlob.transfer.TransferContext, localToBlockBlob.blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
				if err != nil {
					localToBlockBlob.jptm.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to conclude Transfer after processing chunkId %d due to error %s",
							workerId, chunkId, string(err.Error())))
					localToBlockBlob.jptm.TransferStatus(common.TransferFailed)
					transferDone()
					return
				}

				if putBlockListResponse != nil {
					putBlockListResponse.Response().Body.Close()
				}

				localToBlockBlob.jptm.Log(common.LogInfo, "completed successfully")
				localToBlockBlob.jptm.TransferStatus(common.TransferComplete)
				transferDone()
			}
		}
	}
}

func (localToBlockBlob *localToBlockBlob) putBlob() {

	// transform blobURL and perform put blob operation
	blockBlobUrl := localToBlockBlob.blobURL.ToBlockBlobURL()
	blobHttpHeader, metaData := localToBlockBlob.jptm.blobHttpHeaderAndMetadata(localToBlockBlob.memoryMappedFile)

	var putBlobResp *azblob.BlobsPutResponse
	var err error

	// take care of empty blobs
	if localToBlockBlob.jptm.SourceSize == 0 {
		putBlobResp, err = blockBlobUrl.Upload(localToBlockBlob.jptm.TransferContext, nil, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	} else {
		body := newRequestBodyPacer(bytes.NewReader(localToBlockBlob.memoryMappedFile), localToBlockBlob.pacer)
		putBlobResp, err = blockBlobUrl.Upload(localToBlockBlob.jptm.TransferContext, body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
	}

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		localToBlockBlob.jptm.Log(common.LogInfo, " put blob failed and so cancelling the transfer")
		localToBlockBlob.jptm.TransferStatus(common.TransferFailed)
	} else {
		// if the put blob is a success, updating the transfer status to success
		localToBlockBlob.jptm.Log(common.LogInfo,
			fmt.Sprintf("put blob successful"))
		localToBlockBlob.jptm.TransferStatus(common.TransferComplete)
	}

	// updating number of transfers done for job part order
	localToBlockBlob.jptm.TransferDone()

	// perform clean up for the case where blob size is not 0
	if localToBlockBlob.jptm.SourceSize != 0 {
		localToBlockBlob.jptm.jobInfo.JobThroughPut.updateCurrentBytes(int64(localToBlockBlob.jptm.SourceSize))

		localToBlockBlob.memoryMappedFile.Unmap()
		err = localToBlockBlob.srcFileHandler.Close()
		if err != nil {
			localToBlockBlob.jptm.Log(common.LogError,
				fmt.Sprintf("has worker which failed to close the file because of following error %s", err.Error()))
		}
	}

	// closing the put blob response body
	if putBlobResp != nil {
		putBlobResp.Response().Body.Close()
	}
}*/
