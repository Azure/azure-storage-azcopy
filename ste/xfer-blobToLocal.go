package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"io"
	"net/url"
	"os"
	"time"
)

type blobToLocal struct {
	// count the number of chunks that are done
	count uint32
}

func (blobToLocal blobToLocal) prologue(transfer TransferMsg, chunkChannel chan<- ChunkMsg) {
	// step 1: get blob size

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
				transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(transfer.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(transfer.Source)
	blobUrl := azblob.NewBlobURL(*u, p)
	blobSize := int64(transfer.SourceSize)

	// step 2: prep local file before download starts
	memoryMappedFile := createAndMemoryMapFile(transfer.Destination, blobSize)

	// step 3: go through the blob range and schedule download chunk jobs/msgs
	downloadChunkSize := int64(transfer.BlockSize)

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
func generateDownloadFunc(t TransferMsg, chunkId int32, totalNumOfChunks uint32, chunkSize int64,
	startIndex int64, blobURL azblob.BlobURL, memoryMappedFile mmap.MMap) chunkFunc {
	return func(workerId int) {
		if t.TransferContext.Err() != nil {
			if t.ChunksDone() == totalNumOfChunks {
				t.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				t.TransferDone()
			}
		} else {
			//fmt.Println("Worker", workerId, "is processing download CHUNK job with", transferIdentifierStr)

			// step 1: perform get
			get, err := blobURL.GetBlob(t.TransferContext, azblob.BlobRange{Offset: startIndex, Count: chunkSize}, azblob.BlobAccessConditions{}, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				t.TransferCancelFunc()
				t.Log(common.LogInfo, fmt.Sprintf(" has worker %d which is canceling job and chunkId %d because startIndex of %d has failed", workerId, chunkId, startIndex))
				t.TransferStatus(common.TransferFailed)
				if t.ChunksDone() == totalNumOfChunks {
					t.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d which finalizing cancellation of Transfer", workerId))
					t.TransferDone()
				}
				return
			}
			// step2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), memoryMappedFile[startIndex:startIndex+chunkSize])
			get.Body().Close()
			if int64(bytesRead) != chunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				t.TransferCancelFunc()
				t.Log(common.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
				t.TransferStatus(common.TransferFailed)
				if t.ChunksDone() == totalNumOfChunks {
					t.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
					t.TransferDone()
				}
				return
			}

			realTimeThroughputCounter.updateCurrentBytes(chunkSize)

			// step 3: check if this is the last chunk
			if t.ChunksDone() == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				t.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
				//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)
				t.TransferStatus(common.TransferComplete)
				t.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
				t.TransferDone()

				err := memoryMappedFile.Unmap()
				if err != nil {
					t.Log(common.LogError,
						fmt.Sprintf(" has worker %v which failed to conclude Transfer after processing chunkId %v", workerId, chunkId))
				}

				lastModifiedTime, preserveLastModifiedTime := t.ifPreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(t.Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						t.Log(common.LogError, fmt.Sprintf(" not able to preserve last modified time for destionation %s", t.Destination))
						return
					}
					t.Log(common.LogInfo, fmt.Sprintf("successfully preserve the last modified time for destinaton %s", t.Destination))
				}
			}
		}
	}
}
