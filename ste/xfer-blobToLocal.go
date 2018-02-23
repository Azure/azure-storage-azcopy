package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"io"
	"net/url"
	"time"
)

type blobToLocal struct {
	// count the number of chunks that are done
	count uint32
}

func (blobToLocal blobToLocal) prologue(transfer TransferMsg, chunkChannel chan<- ChunkMsg) {
	// step 1: get blob size
	jobInfo := transfer.getJobInfo()

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
				jobInfo.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(jobInfo.minimumLogLevel)
			},
		},
	})
	source, destination := transfer.SourceDestination()
	u, _ := url.Parse(source)
	blobUrl := azblob.NewBlobURL(*u, p)
	blobSize := getBlobSize(blobUrl)

	// step 2: prep local file before download starts
	memoryMappedFile := createAndMemoryMapFile(destination, blobSize)

	// step 3: go through the blob range and schedule download chunk jobs/msgs
	downloadChunkSize := int64(transfer.getBlockSize())

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
				transfer,
				blockIdCount, // serves as index of chunk
				computeNumOfChunks(blobSize, downloadChunkSize),
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the downloading of a single chunk
func generateDownloadFunc(t TransferMsg,  chunkId int32, totalNumOfChunks uint32, chunkSize int64,
							startIndex int64, blobURL azblob.BlobURL, memoryMappedFile mmap.MMap) chunkFunc {
	return func(workerId int) {
		jobInfo := t.getJobInfo()
		transferIdentifierStr := t.getTransferIdentifierString()
		if t.TransferContext.Err() != nil {
			if t.incrementNumberOfChunksDone() == totalNumOfChunks {
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is finalizing cancellation %s", workerId, transferIdentifierStr))
				t.updateNumberOfTransferDone()
			}
		} else {
			//fmt.Println("Worker", workerId, "is processing download CHUNK job with", transferIdentifierStr)

			// step 1: perform get
			get, err := blobURL.GetBlob(t.TransferContext, azblob.BlobRange{Offset: startIndex, Count: chunkSize}, azblob.BlobAccessConditions{}, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				t.TransferCancelFunc()
				jobInfo.Log(common.LogInfo, fmt.Sprintf("worker %d is canceling Chunk job with %s and chunkId %d because startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex))
				t.updateTransferStatus(common.TransferFailed)
				if t.incrementNumberOfChunksDone() == totalNumOfChunks {
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("worker %d is finalizing cancellation of %s", workerId, transferIdentifierStr))
					t.updateNumberOfTransferDone()
				}
				return
			}
			// step2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), memoryMappedFile[startIndex:startIndex+chunkSize])
			get.Body().Close()
			if int64(bytesRead) != chunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				t.TransferCancelFunc()
				jobInfo.Log(common.LogInfo, fmt.Sprintf("worker %d is canceling Chunk job with %s and chunkId %d because writing to file for startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex))
				t.updateTransferStatus(common.TransferFailed)
				if t.incrementNumberOfChunksDone() == totalNumOfChunks {
					jobInfo.Log(common.LogInfo,
						fmt.Sprintf("worker %d is finalizing cancellation of %s",
							workerId, transferIdentifierStr))
					t.updateNumberOfTransferDone()
				}
				return
			}

			realTimeThroughputCounter.updateCurrentBytes(chunkSize)

			// step 3: check if this is the last chunk
			if t.incrementNumberOfChunksDone() == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is concluding download Transfer job with %s after processing chunkId %d",
						workerId, transferIdentifierStr, chunkId))
				//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)
				t.updateTransferStatus(common.TransferComplete)
				jobInfo.Log(common.LogInfo,
					fmt.Sprintf("worker %d is finalizing cancellation of %s",
						workerId, transferIdentifierStr))
				t.updateNumberOfTransferDone()

				err := memoryMappedFile.Unmap()
				if err != nil {
					jobInfo.Log(common.LogError,
						fmt.Sprintf("worker %v failed to conclude Transfer job with %v after processing chunkId %v",
							workerId, transferIdentifierStr, chunkId))
				}

				t.ifPreserveLastModifiedTime(get)
			}
		}
	}
}
