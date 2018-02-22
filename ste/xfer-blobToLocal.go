package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"io"
	"net/url"
	"sync/atomic"
	"time"
)

type blobToLocal struct {
	// count the number of chunks that are done
	count uint32
}

func (blobToLocal blobToLocal) prologue(transfer TransferMsgDetail, chunkChannel chan<- ChunkMsg) {
	// step 1: get blob size
	logger := transfer.JobHandlerMap.LoadJobInfoForJob(transfer.JobId)

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {
				logger.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(logger.minimumLogLevel)
			},
		},
	})
	u, _ := url.Parse(transfer.Source)
	blobUrl := azblob.NewBlobURL(*u, p)
	blobSize := getBlobSize(blobUrl)

	// step 2: prep local file before download starts
	memoryMappedFile := createAndMemoryMapFile(transfer.Destination, blobSize)

	// step 3: go through the blob range and schedule download chunk jobs/msgs
	downloadChunkSize := int64(transfer.ChunkSize)

	blockIdCount := int32(0)
	for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the download chunk job
		chunkChannel <- ChunkMsg{
			doTransfer: generateDownloadFunc(
				transfer.JobId,
				transfer.PartNumber,
				transfer.TransferId,
				blockIdCount, // serves as index of chunk
				computeNumOfChunks(blobSize, downloadChunkSize),
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile,
				transfer.TransferCtx,
				transfer.TransferCancelFunc,
				&blobToLocal.count, transfer.JobHandlerMap),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the downloading of a single chunk
func generateDownloadFunc(jobId common.JobID, partNum common.PartNumber, transferId uint32, chunkId int32, totalNumOfChunks uint32, chunkSize int64, startIndex int64,
	blobURL azblob.BlobURL, memoryMappedFile mmap.MMap, ctx context.Context, cancelTransfer func(), progressCount *uint32, jobsInfoMap *JobsInfoMap) chunkFunc {
	return func(workerId int) {
		jobInfo := jobsInfoMap.LoadJobInfoForJob(jobId)
		if ctx.Err() != nil {
			if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is finalizing cancellation of job %s and part number %d",
						workerId, jobId.String(), partNum))
				updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)
			}
		} else {
			transferIdentifierStr := fmt.Sprintf("jobId %s and partNum %d and transferId %d", jobId.String(), partNum, transferId)

			//fmt.Println("Worker", workerId, "is processing download CHUNK job with", transferIdentifierStr)

			// step 1: perform get
			get, err := blobURL.GetBlob(ctx, azblob.BlobRange{Offset: startIndex, Count: chunkSize}, azblob.BlobAccessConditions{}, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				cancelTransfer()
				jobInfo.Log(common.LogInfo, fmt.Sprintf("worker %d is canceling Chunk job with %s and chunkId %d because startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex))
				updateTransferStatus(jobId, partNum, transferId, common.TransferFailed, jobsInfoMap)
				if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("worker %d is finalizing cancellation of job %s and part number %d",
							workerId, jobId, partNum))
					updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)
				}
				return
			}
			// step2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), memoryMappedFile[startIndex:startIndex+chunkSize])
			get.Body().Close()
			if int64(bytesRead) != chunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				cancelTransfer()
				jobInfo.Log(common.LogInfo, fmt.Sprintf("worker %d is canceling Chunk job with %s and chunkId %d because writing to file for startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex))
				updateTransferStatus(jobId, partNum, transferId, common.TransferFailed, jobsInfoMap)
				if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("worker %d is finalizing cancellation of job %s and part number %d",
							workerId, jobId, partNum))
					updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)
				}
				return
			}

			realTimeThroughputCounter.updateCurrentBytes(chunkSize)

			// step 3: check if this is the last chunk
			if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is concluding download Transfer job with %s after processing chunkId %d",
						workerId, transferIdentifierStr, chunkId))
				//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)

				updateTransferStatus(jobId, partNum, transferId, common.TransferComplete, jobsInfoMap)
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is finalizing cancellation of job %s and part number %d",
						workerId, jobId.String(), partNum))
				updateNumberOfTransferDone(jobId, partNum, jobsInfoMap)

				err := memoryMappedFile.Unmap()
				if err != nil {
					jobInfo.Log(common.LogError,
						fmt.Sprintf("worker %v failed to conclude Transfer job with %v after processing chunkId %v",
							workerId, transferIdentifierStr, chunkId))
				}

				jobPartInfo := jobsInfoMap.LoadJobPartPlanInfoForJobPart(jobId, partNum)
				// if the job order has the flag preserve-last-modified-time set to true,
				// then changing the timestamp of destination to last-modified time received in response
				if jobPartInfo.getJobPartPlanPointer().BlobData.PreserveLastModifiedTime {
					_, dst := jobPartInfo.getTransferSrcDstDetail(transferId)
					lastModifiedTime := get.LastModified()
					setModifiedTime(dst, lastModifiedTime, jobInfo)
				}
			}
		}
	}
}
