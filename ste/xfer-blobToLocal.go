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

// this struct is created for each transfer
type blobToLocal struct {
	transfer         *TransferMsg
	blobURL          azblob.BlobURL
	memoryMappedFile mmap.MMap
	blockIds         []string
}

// return a new blobToLocal struct targeting a specific transfer
func newBlobToLocal(transfer *TransferMsg, pacer *pacer) xfer {
	// download is not paced
	return &blobToLocal{transfer: transfer}
}

func (blobToLocal *blobToLocal) runPrologue(chunkChannel chan<- ChunkMsg) {
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
				blobToLocal.transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(blobToLocal.transfer.MinimumLogLevel)
			},
		},
	})
	u, _ := url.Parse(blobToLocal.transfer.Source)
	blobToLocal.blobURL = azblob.NewBlobURL(*u, p)

	// step 2: get size info for the download
	blobSize := int64(blobToLocal.transfer.SourceSize)
	downloadChunkSize := int64(blobToLocal.transfer.BlockSize)

	// step 3: prep local file before download starts
	blobToLocal.memoryMappedFile = executionEngineHelper{}.createAndMemoryMapFile(blobToLocal.transfer.Destination, blobSize)

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
				blockIdCount, // serves as index of chunk
				adjustedChunkSize,
				startIndex),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the downloading of a single chunk
func (blobToLocal *blobToLocal) generateDownloadFunc(chunkId int32, adjustedChunkSize int64, startIndex int64) chunkFunc {
	return func(workerId int) {
		totalNumOfChunks := uint32(blobToLocal.transfer.NumChunks)

		// TODO consider encapsulating this check operation on transferMsg
		if blobToLocal.transfer.TransferContext.Err() != nil {
			if blobToLocal.transfer.ChunksDone() == totalNumOfChunks {
				blobToLocal.transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				blobToLocal.transfer.TransferDone()
			}
		} else {
			// step 1: perform get
			get, err := blobToLocal.blobURL.GetBlob(blobToLocal.transfer.TransferContext, azblob.BlobRange{Offset: startIndex, Count: adjustedChunkSize}, azblob.BlobAccessConditions{}, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				// TODO consider encapsulating cancel operation on transferMsg
				blobToLocal.transfer.TransferCancelFunc()
				blobToLocal.transfer.Log(common.LogInfo, fmt.Sprintf(" has worker %d which is canceling job and chunkId %d because startIndex of %d has failed", workerId, chunkId, startIndex))
				blobToLocal.transfer.TransferStatus(common.TransferFailed)
				if blobToLocal.transfer.ChunksDone() == totalNumOfChunks {
					blobToLocal.transfer.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d which finalizing cancellation of Transfer", workerId))
					blobToLocal.transfer.TransferDone()
				}
				return
			}
			// step 2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), blobToLocal.memoryMappedFile[startIndex:startIndex+adjustedChunkSize])
			get.Body().Close()
			if int64(bytesRead) != adjustedChunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				blobToLocal.transfer.TransferCancelFunc()
				blobToLocal.transfer.Log(common.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
				blobToLocal.transfer.TransferStatus(common.TransferFailed)
				if blobToLocal.transfer.ChunksDone() == totalNumOfChunks {
					blobToLocal.transfer.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
					blobToLocal.transfer.TransferDone()
				}
				return
			}

			// TODO this should be 1 counter per job
			realTimeThroughputCounter.updateCurrentBytes(adjustedChunkSize)

			// step 3: check if this is the last chunk
			if blobToLocal.transfer.ChunksDone() == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				blobToLocal.transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
				//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)
				blobToLocal.transfer.TransferStatus(common.TransferComplete)
				blobToLocal.transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
				blobToLocal.transfer.TransferDone()

				err := blobToLocal.memoryMappedFile.Unmap()
				if err != nil {
					blobToLocal.transfer.Log(common.LogError,
						fmt.Sprintf(" has worker %v which failed to conclude Transfer after processing chunkId %v", workerId, chunkId))
				}

				lastModifiedTime, preserveLastModifiedTime := blobToLocal.transfer.PreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(blobToLocal.transfer.Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						blobToLocal.transfer.Log(common.LogError, fmt.Sprintf(" not able to preserve last modified time for destionation %s", blobToLocal.transfer.Destination))
						return
					}
					blobToLocal.transfer.Log(common.LogInfo, fmt.Sprintf("successfully preserve the last modified time for destinaton %s", blobToLocal.transfer.Destination))
				}
			}
		}
	}
}
