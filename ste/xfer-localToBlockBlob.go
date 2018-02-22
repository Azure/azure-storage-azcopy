package ste

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"net/url"
	"os"
	"sync/atomic"
	"time"
)

type localToBlockBlob struct {
	// count the number of chunks that are done
	count uint32
}

// this function performs the setup for each transfer and schedules the corresponding chunkMsgs into the chunkChannel
func (localToBlockBlob localToBlockBlob) prologue(transfer TransferMsgDetail, chunkChannel chan<- ChunkMsg) {

	jobInfo := transfer.JobHandlerMap.LoadJobInfoForJob(transfer.JobId)
	// step 1: create pipeline for the destination blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      5,
			TryTimeout:    time.Minute * 10,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {
				jobInfo.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(jobInfo.minimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(transfer.Destination)
	blobUrl := azblob.NewBlobURL(*u, p)

	// step 2: get the file size
	fi, _ := os.Stat(transfer.Source)
	blobSize := fi.Size()

	// step 3: map in the file to upload before transferring chunks
	memoryMappedFile := openAndMemoryMapFile(transfer.Source)

	// step 4: compute the number of blocks and create a slice to hold the blockIDs of each chunk
	downloadChunkSize := int64(transfer.ChunkSize)

	numOfBlocks := computeNumOfChunks(blobSize, downloadChunkSize)
	blocksIds := make([]string, numOfBlocks)
	blockIdCount := int32(0)

	// step 5: go through the file and schedule chunk messages to upload each chunk
	for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute actual size of the chunk
		if startIndex+downloadChunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: generateUploadFunc(
				transfer.JobId,
				transfer.PartNumber,
				transfer.TransferId,
				blockIdCount, // this is the index of the chunk
				numOfBlocks,
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile,
				transfer.TransferCtx,
				transfer.TransferCancelFunc,
				&localToBlockBlob.count,
				&blocksIds, transfer.JobHandlerMap),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the uploading of a single chunk
func generateUploadFunc(jobId common.JobID, partNum common.PartNumber, transferId uint32, chunkId int32, totalNumOfChunks uint32, chunkSize int64, startIndex int64, blobURL azblob.BlobURL,
	memoryMappedFile mmap.MMap, ctx context.Context, cancelTransfer func(), progressCount *uint32, blockIds *[]string, jobsInfoMap *JobsInfoMap) chunkFunc {
	return func(workerId int) {
		jobInfo := jobsInfoMap.LoadJobInfoForJob(jobId)
		if ctx.Err() != nil {
			jobInfo.Log(common.LogInfo, fmt.Sprintf("transferId %d of jobId %s and partNum %d are cancelled. Hence not picking up chunkId %d", transferId, common.JobID(jobId).String(), partNum, chunkId))
			if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is finalizing cancellation of job %s and part number %d",
						workerId, jobId, partNum))
				//updateTransferStatus(jobId, partNum, transferId, common.TransferStatusFailed, jobsInfoMap)
				updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)
			}
		} else {

			// If there are more than one block for a transfer, then we need to upload each individually
			// and then we need to upload the block list
			if totalNumOfChunks > 1 {
				transferIdentifierStr := fmt.Sprintf("jobId %s and partNum %d and transferId %d", common.JobID(jobId).String(), partNum, transferId)

				// step 1: generate block ID
				blockId := common.NewUUID().String()
				encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

				// step 2: save the block ID into the list of block IDs
				(*blockIds)[chunkId] = encodedBlockId

				// step 3: perform put block
				blockBlobUrl := blobURL.ToBlockBlobURL()

				body := newRequestBodyPacer(bytes.NewReader(memoryMappedFile[startIndex:startIndex+chunkSize]), pc)
				putBlockResponse, err := blockBlobUrl.PutBlock(ctx, encodedBlockId, body, azblob.LeaseAccessConditions{})
				if err != nil {
					// cancel entire transfer because this chunk has failed
					cancelTransfer()
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("worker %d is canceling Chunk job with %s and chunkId %d because startIndex of %d has failed",
							workerId, transferIdentifierStr, chunkId, startIndex))
					//fmt.Println("Worker", workerId, "is canceling CHUNK job with", transferIdentifierStr, "and chunkID", chunkId, "because startIndex of", startIndex, "has failed due to err", err)
					//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
					updateTransferStatus(jobId, partNum, transferId, common.TransferFailed, jobsInfoMap)
					jobInfo.Log(common.LogInfo, fmt.Sprintf("transferId %d of jobId %s and partNum %d are cancelled. Hence not picking up chunkId %d", transferId, common.JobID(jobId).String(), partNum, chunkId))
					if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
						jobInfo.Log(common.LogInfo,
							fmt.Sprintf("worker %d is finalizing cancellation of job %s and part number %d",
								workerId, common.JobID(jobId).String(), partNum))
						updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)

						err := memoryMappedFile.Unmap()
						if err != nil {
							jobInfo.Log(common.LogError,
								fmt.Sprintf("worker %v failed to conclude Transfer job with %v after processing chunkId %v",
									workerId, transferIdentifierStr, chunkId))
						}

					}
					return
				}

				if putBlockResponse != nil {
					putBlockResponse.Response().Body.Close()
				}

				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusComplete, jobsInfoMap)
				realTimeThroughputCounter.updateCurrentBytes(chunkSize)

				// step 4: check if this is the last chunk
				if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
					// If the transfer gets cancelled before the putblock list
					if ctx.Err() != nil {
						updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)
						return
					}
					// step 5: this is the last block, perform EPILOGUE
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("worker %d is concluding download Transfer job with %s after processing chunkId %d with blocklist %s",
							workerId, transferIdentifierStr, chunkId, *blockIds))
					//fmt.Println("Worker", workerId, "is concluding upload TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId, "with blocklist", *blockIds)

					// fetching the blob http headers with content-type, content-encoding attributes
					blobHttpHeader := getBlobHttpHeaders(jobId, partNum, jobsInfoMap, memoryMappedFile)

					// fetching the metadata passed with the JobPartOrder
					metaData := getJobPartMetaData(jobId, partNum, jobsInfoMap)

					putBlockListResponse, err := blockBlobUrl.PutBlockList(ctx, *blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
					if err != nil {
						jobInfo.Log(common.LogError,
							fmt.Sprintf("Worker %d failed to conclude Transfer job with %s after processing chunkId %d due to error %s",
								workerId, transferIdentifierStr, chunkId, string(err.Error())))
						updateTransferStatus(jobId, partNum, transferId, common.TransferFailed, jobsInfoMap)
						updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)
						return
					}

					if putBlockListResponse != nil {
						putBlockListResponse.Response().Body.Close()
					}

					jobInfo.Log(common.LogInfo, fmt.Sprintf("transfer %d of Job %s and part number %d has completed successfully", transferId, common.JobID(jobId).String(), partNum))
					updateTransferStatus(jobId, partNum, transferId, common.TransferComplete, jobsInfoMap)
					updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)

					err = memoryMappedFile.Unmap()
					if err != nil {
						jobInfo.Log(common.LogError,
							fmt.Sprintf("worker %v failed to conclude Transfer job with %v after processing chunkId %v",
								workerId, transferIdentifierStr, chunkId))
					}
				}
			} else {
				// If there is only one block for a transfer, then uploading block as a blob
				blockBlobUrl := blobURL.ToBlockBlobURL()

				blobHttpHeader := getBlobHttpHeaders(jobId, partNum, jobsInfoMap, memoryMappedFile)

				// fetching the metadata passed with the JobPartOrder
				metaData := getJobPartMetaData(jobId, partNum, jobsInfoMap)

				// reading the chunk contents
				body := newRequestBodyPacer(bytes.NewReader(memoryMappedFile[startIndex:startIndex+chunkSize]), pc)

				putblobResp, err := blockBlobUrl.PutBlob(ctx, body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})

				// if the put blob is a failure, updating the transfer status to failed
				if err != nil {
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("put blob failed for transfer %d of Job %s and part number %d failed and so cancelling the transfer",
							transferId, jobId, partNum))
					updateTransferStatus(jobId, partNum, transferId, common.TransferFailed, jobsInfoMap)
				} else {
					// if the put blob is a success, updating the transfer status to success
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("put blob successful for transfer %d of Job %s and part number %d by worked %d",
							transferId, jobId, partNum, workerId))
					updateTransferStatus(jobId, partNum, transferId, common.TransferComplete, jobsInfoMap)
				}

				// updating number of transfers done for job part order
				updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)

				// closing the put blob response body
				if putblobResp != nil {
					putblobResp.Response().Body.Close()
				}

				err = memoryMappedFile.Unmap()
				if err != nil {
					jobInfo.Log(common.LogError,
						fmt.Sprintf("error mapping the memory map file for transfer %d job %s and part number %d",
							transferId, jobId, partNum))
				}
			}
		}
	}
}
