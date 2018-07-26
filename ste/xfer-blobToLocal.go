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
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

func BlobToLocal(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	// step 1: get the source, destination info for the transfer.
	info := jptm.Info()
	u, _ := url.Parse(info.Source)

	srcBlobURL := azblob.NewBlobURL(*u, p)
	// step 2: get size info for the download
	blobSize := int64(info.SourceSize)
	downloadChunkSize := int64(info.BlockSize)
	numChunks := uint32(0)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the blob exists locally or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := os.Stat(info.Destination)
		if err == nil {
			// If the error is nil, then blob exists locally and it doesn't needs to be downloaded.
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("skipping the transfer since blob already exists"))
			}
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.AddToBytesDone(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// step 3: prep local file before download starts
	if blobSize == 0 {
		err := createEmptyFile(info.Destination)
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "BlobDownloadFailed. transfer failed because dst file could not be created locally. Failed with error "+err.Error())
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		lMTime, plmt := jptm.PreserveLastModifiedTime()
		if plmt {
			err := os.Chtimes(jptm.Info().Destination, lMTime, lMTime)
			if err != nil {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobDownloadFailed. Failed while preserving last modified time for destionation %s", info.Destination))
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				// Since the transfer failed, the file created above should be deleted
				err = deleteFile(info.Destination)
				if err != nil {
					// If there was an error deleting the file, log the error
					if jptm.ShouldLog(pipeline.LogError) {
						jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the file %s. Failed with error %s", info.Destination, err.Error()))
					}
				}
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" successfully preserved the last modified time for destinaton %s", info.Destination))
			}
		}

		// executing the epilogue.
		jptm.Log(pipeline.LogInfo, "BlobDownloadSuccessful. concluding the download Transfer of job after creating an empty file")
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()

	} else { // 3b: source has content
		dstFile, err := createFileOfSize(info.Destination, blobSize)
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "BlobDownloadFailed. transfer failed because dst file could not be created locally. Failed with error "+err.Error())
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}

		defer dstFile.Close()

		dstMMF, err := common.NewMMF(dstFile, true, 0, info.SourceSize)
		if err != nil {
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "BlobDownloadFailed. transfer failed because dst file did not memory mapped successfully")
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
			// Since the transfer failed, the file created above should be deleted
			err = deleteFile(info.Destination)
			if err != nil {
				// If there was an error deleting the file, log the error
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the file %s. Failed with error %s", info.Destination, err.Error()))
				}
			}
			jptm.ReportTransferDone()
			return
		}
		if rem := blobSize % downloadChunkSize; rem == 0 {
			numChunks = uint32(blobSize / downloadChunkSize)
		} else {
			numChunks = uint32(blobSize/downloadChunkSize + 1)
		}
		jptm.SetNumberOfChunks(numChunks)
		blockIdCount := int32(0)
		// step 4: go through the blob range and schedule download chunk jobs
		for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
			adjustedChunkSize := downloadChunkSize

			// compute exact size of the chunk
			if startIndex+downloadChunkSize > blobSize {
				adjustedChunkSize = blobSize - startIndex
			}

			// schedule the download chunk job
			jptm.ScheduleChunks(generateDownloadBlobFunc(jptm, srcBlobURL, blockIdCount, dstMMF, info.Destination, startIndex, adjustedChunkSize, pacer))
			blockIdCount++
		}
	}
}

func generateDownloadBlobFunc(jptm IJobPartTransferMgr, transferBlobURL azblob.BlobURL, chunkId int32, destinationMMF *common.MMF, destinationPath string, startIndex int64, adjustedChunkSize int64, p *pacer) chunkFunc {
	return func(workerId int) {
		// TODO: added the two operations for debugging purpose. remove later
		// Increment a number of goroutine performing the transfer / acting on chunks msg by 1
		jptm.OccupyAConnection()
		// defer the decrement in the number of goroutine performing the transfer / acting on chunks msg by 1
		defer jptm.ReleaseAConnection()

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		//defer func (jptm IJobPartTransferMgr) {
		//	r := recover()
		//	if r != nil {
		//		info := jptm.Info()
		//		if jptm.ShouldLog(pipeline.LogError) {
		//			jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startIndex %v chunkSize %v sourceMMF size %v",
		//					r, info.Source, info.Destination, info.SourceSize, startIndex, adjustedChunkSize, len(destinationMMF.Slice())))
		//		}
		//		jptm.SetStatus(common.ETransferStatus.Failed())
		//		jptm.ReportTransferDone()
		//	}
		//}(jptm)

		chunkDone := func() {
			// adding the bytes transferred or skipped of a transfer to determine the progress of transfer.
			jptm.AddToBytesDone(adjustedChunkSize)
			lastChunk, _ := jptm.ReportChunkDone()
			if lastChunk {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				}
				destinationMMF.Unmap()
				// If the current transfer status value is less than or equal to 0
				// then transfer either failed or was cancelled
				// the file created locally should be deleted
				if jptm.TransferStatus() <= 0 {
					err := deleteFile(destinationPath)
					if err != nil {
						// If there was an error deleting the file, log the error
						if jptm.ShouldLog(pipeline.LogError) {
							jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the file %s. Failed with error %s", destinationPath, err.Error()))
						}
					}
				}
				jptm.ReportTransferDone()
			}
		}
		if jptm.WasCanceled() {
			chunkDone()
		} else {
			// Step 1: Download blob from start Index till startIndex + adjustedChunkSize
			get, err := transferBlobURL.Download(jptm.Context(), startIndex, adjustedChunkSize, azblob.BlobAccessConditions{}, false)
			if err != nil {
				if !jptm.WasCanceled() {
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobDownloadFailed. worker %d is canceling job because writing to file for startIndex of %d has failed", workerId, startIndex))
					}
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}
			// step 2: write the body into the memory mapped file directly
			body := get.Body(azblob.RetryReaderOptions{MaxRetryRequests: MaxRetryPerDownloadBody})
			body = newResponseBodyPacer(body, p, destinationMMF)
			_, err = io.ReadFull(body, destinationMMF.Slice()[startIndex:startIndex+adjustedChunkSize])
			if err != nil {
				// cancel entire transfer because this chunk has failed
				if !jptm.WasCanceled() {
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobDownloadFailed. worker %d is canceling job because reading the downloaded chunk failed. Failed with error %s", workerId, err.Error()))
					}
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}

			jptm.AddToBytesDone(adjustedChunkSize)

			lastChunk, _ := jptm.ReportChunkDone()
			// step 3: check if this is the last chunk
			if lastChunk {
				// step 4: this is the last block, perform EPILOGUE
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobDownloadSuccessful. worker %d is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is finalizing Transfer", workerId))
				}
				jptm.ReportTransferDone()

				destinationMMF.Unmap()

				lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						if jptm.ShouldLog(pipeline.LogInfo) {
							jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which failed while preserving last modified time for destionation %s", workerId, jptm.Info().Destination))
						}
						return
					}
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which successfully preserve the last modified time for destinaton %s", workerId, jptm.Info().Destination))
					}
				}
			}
		}
	}
}

func createParentDirectoryIfNotExist(destinationPath string) error {
	// check if parent directory exists
	parentDirectory := destinationPath[:strings.LastIndex(destinationPath, string(os.PathSeparator))]
	_, err := os.Stat(parentDirectory)
	// if the parent directory does not exist, create it and all its parents
	if os.IsNotExist(err) {
		err = os.MkdirAll(parentDirectory, os.ModePerm)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// create an empty file and its parent directories, without any content
func createEmptyFile(destinationPath string) error {
	createParentDirectoryIfNotExist(destinationPath)
	f, err := os.OpenFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

// deletes the file
func deleteFile(destinationPath string) error {
	return os.Remove(destinationPath)
}

// create a file, given its path and length

func createFileOfSize(destinationPath string, fileSize int64) (*os.File, error) {
	createParentDirectoryIfNotExist(destinationPath)

	f, err := os.OpenFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		return nil, err
	}
	return f, nil
}
