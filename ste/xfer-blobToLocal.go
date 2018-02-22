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
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"io"
	"net/url"
	"os"
)

type blobToLocal struct {}

func (blobToLocal blobToLocal) prologue(transfer TransferMsg, chunkChannel chan<- ChunkMsg) {
	// step 1: create blobUrl for source blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      DownloadMaxTries,
			TryTimeout:    DownloadTryTimeout,
			RetryDelay:    DownloadRetryDelay,
			MaxRetryDelay: DownloadMaxRetryDelay,
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

	// step 2: get size info for the download
	blobSize := int64(transfer.SourceSize)
	downloadChunkSize := int64(transfer.BlockSize)

	// step 3: prep local file before download starts
	memoryMappedFile := executionEngineHelper{}.createAndMemoryMapFile(transfer.Destination, blobSize)

	// step 4: go through the blob range and schedule download chunk jobs
	blockIdCount := int32(0)
	for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the download chunk job
		chunkChannel <- ChunkMsg{
			doTransfer: blobToLocal.generateDownloadFunc(
				transfer,
				blockIdCount, // serves as index of chunk
				uint32(transfer.NumChunks),
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the downloading of a single chunk
func (blobToLocal) generateDownloadFunc(transfer TransferMsg, chunkId int32, totalNumOfChunks uint32, chunkSize int64,
	startIndex int64, blobURL azblob.BlobURL, memoryMappedFile mmap.MMap) chunkFunc {
	return func(workerId int) {

		// TODO consider encapsulating this check operation on transferMsg
		if transfer.TransferContext.Err() != nil {
			if transfer.ChunksDone() == totalNumOfChunks {
				transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				transfer.TransferDone()
			}
		} else {
			// step 1: perform get
			get, err := blobURL.GetBlob(transfer.TransferContext, azblob.BlobRange{Offset: startIndex, Count: chunkSize}, azblob.BlobAccessConditions{}, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				// TODO consider encapsulating cancel operation on transferMsg
				transfer.TransferCancelFunc()
				transfer.Log(common.LogInfo, fmt.Sprintf(" has worker %d which is canceling job and chunkId %d because startIndex of %d has failed", workerId, chunkId, startIndex))
				transfer.TransferStatus(common.TransferFailed)
				if transfer.ChunksDone() == totalNumOfChunks {
					transfer.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d which finalizing cancellation of Transfer", workerId))
					transfer.TransferDone()
				}
				return
			}
			// step 2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), memoryMappedFile[startIndex:startIndex+chunkSize])
			get.Body().Close()
			if int64(bytesRead) != chunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				transfer.TransferCancelFunc()
				transfer.Log(common.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
				transfer.TransferStatus(common.TransferFailed)
				if transfer.ChunksDone() == totalNumOfChunks {
					transfer.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
					transfer.TransferDone()
				}
				return
			}

			// TODO this should be 1 counter per job
			realTimeThroughputCounter.updateCurrentBytes(chunkSize)

			// step 3: check if this is the last chunk
			if transfer.ChunksDone() == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
				//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)
				transfer.TransferStatus(common.TransferComplete)
				transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
				transfer.TransferDone()

				err := memoryMappedFile.Unmap()
				if err != nil {
					transfer.Log(common.LogError,
						fmt.Sprintf(" has worker %v which failed to conclude Transfer after processing chunkId %v", workerId, chunkId))
				}

				lastModifiedTime, preserveLastModifiedTime := transfer.ifPreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(transfer.Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						transfer.Log(common.LogError, fmt.Sprintf(" not able to preserve last modified time for destionation %s", transfer.Destination))
						return
					}
					transfer.Log(common.LogInfo, fmt.Sprintf("successfully preserve the last modified time for destinaton %s", transfer.Destination))
				}
			}
		}
	}
}
