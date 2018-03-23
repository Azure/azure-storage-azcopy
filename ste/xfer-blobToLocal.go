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
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"io"
	"net/url"
	"os"
)

// this struct is created for each transfer
type blobToLocal struct {
	jptm       IJobPartTransferMgr
	srcBlobURL azblob.BlobURL
	dstFile    *os.File   // MUST be closed in the epilog
	dstMMF     common.MMF // MUST be unmapped in the epilog
	blockIds   []string   // Base64 block IDs
}

// return a new blobToLocal struct targeting a specific transfer
func newBlobToLocal(jptm IJobPartTransferMgr) {
	blobToLocal := &blobToLocal{jptm: jptm}

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
			Log: blobToLocal.jptm.Log,
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(blobToLocal.jptm.MinimumLogLevel)
			},
		},
	})
	info := blobToLocal.jptm.Info()
	u, _ := url.Parse(info.Source)
	blobToLocal.srcBlobURL = azblob.NewBlobURL(*u, p)

	// step 2: get size info for the download
	blobSize := int64(info.SourceSize)
	downloadChunkSize := int64(info.BlockSize)

	// step 3: prep local file before download starts
	var err error
	blobToLocal.dstFile, err = os.Open(info.Destination)
	if err != nil {
		// TODO
	}
	blobToLocal.dstMMF, err = common.NewMMF(blobToLocal.dstFile, true, 0, info.SourceSize)
	if err != nil {
		blobToLocal.dstFile.Close()
	}

	// step 4: go through the blob range and schedule download chunk jobs
	blockIdCount := int32(0)
	for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the download chunk job
		jptm.ScheduleChunk(func() {
			blobToLocal.generateDownloadFunc(blockIdCount */
/* chunk index*//*
 , adjustedChunkSize, startIndex)
		})
		blockIdCount += 1
	}
}

// this generates a function which performs the downloading of a single chunk
func (blobToLocal *blobToLocal) generateDownloadFunc(chunkId int32, adjustedChunkSize int64, startIndex int64) chunkFunc {
	return func() {
		totalNumOfChunks := uint32(blobToLocal.jptm.NumChunks)

		// TODO consider encapsulating this check operation on transferMsg
		if blobToLocal.jptm.TransferContext.Err() != nil {
			if blobToLocal.jptm.ChunksDone() == totalNumOfChunks {
				blobToLocal.jptm.Log(pipeline.LogInfo,
					fmt.Print(" has worker which is finalizing cancellation of the Transfer"))
				blobToLocal.jptm.TransferDone()
			}
		} else {
			// step 1: perform get
			get, err := blobToLocal.blobURL.Download(blobToLocal.transfer.TransferContext, azblob.BlobRange{Offset: startIndex, Count: adjustedChunkSize}, azblob.BlobAccessConditions{}, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				// TODO consider encapsulating cancel operation on transferMsg
				blobToLocal.jptm.TransferCancelFunc()
				blobToLocal.jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which is canceling job and chunkId %d because startIndex of %d has failed", workerId, chunkId, startIndex))
				blobToLocal.jptm.TransferStatus(common.TransferFailed)
				if blobToLocal.jptm.ChunksDone() == totalNumOfChunks {
					blobToLocal.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf(" has worker %d which finalizing cancellation of Transfer", workerId))
					blobToLocal.jptm.TransferDone()
				}
				return
			}
			// step 2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), blobToLocal.memoryMappedFile[startIndex:startIndex+adjustedChunkSize])
			get.Body().Close()
			if int64(bytesRead) != adjustedChunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				blobToLocal.jptm.TransferCancelFunc()
				blobToLocal.jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
				blobToLocal.jptm.TransferStatus(common.TransferFailed)
				if blobToLocal.jptm.ChunksDone() == totalNumOfChunks {
					blobToLocal.jptm.Log(pipeline.LogInfo,
						fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
					blobToLocal.jptm.TransferDone()
				}
				return
			}

			blobToLocal.jptm.jobInfo.JobThroughPut.updateCurrentBytes(adjustedChunkSize)

			// step 3: check if this is the last chunk
			if blobToLocal.jptm.ChunksDone() == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				blobToLocal.jptm.Log(pipeline.LogInfo,
					fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
				//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)
				blobToLocal.jptm.TransferStatus(common.TransferComplete)
				blobToLocal.jptm.Log(pipeline.LogInfo,
					fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
				blobToLocal.jptm.TransferDone()

				blobToLocal.memoryMappedFile.Unmap()
				err := blobToLocal.srcFileHandler.Close()
				if err != nil {
					blobToLocal.jptm.Log(pipeline.LogError,
						fmt.Sprintf(" has worker %v which failed to close the file %s and failed with error %s", workerId, blobToLocal.srcFileHandler.Name(), err.Error()))
				}

				lastModifiedTime, preserveLastModifiedTime := blobToLocal.jptm.PreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(blobToLocal.jptm.Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						blobToLocal.jptm.Log(pipeline.LogError, fmt.Sprintf(" not able to preserve last modified time for destionation %s", blobToLocal.transfer.Destination))
						return
					}
					blobToLocal.jptm.Log(pipeline.LogInfo, fmt.Sprintf("successfully preserve the last modified time for destinaton %s", blobToLocal.transfer.Destination))
				}
			}
		}
	}
}
*/
