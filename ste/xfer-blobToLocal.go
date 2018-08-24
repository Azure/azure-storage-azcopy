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
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the blob exists locally or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := os.Stat(info.Destination)
		if err == nil {
			// TODO: Confirm if this is an error condition or not
			// If the error is nil, then blob exists locally and it doesn't need to be downloaded.
			jptm.LogDownloadError(info.Source, info.Destination, "Blob already exists", 0)
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.BlobAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	// step 3: prep local file before download starts
	if blobSize == 0 {
		err := createEmptyFile(info.Destination)
		if err != nil {
			jptm.LogDownloadError(info.Source, info.Destination, "Empty File Creation error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		lMTime, plmt := jptm.PreserveLastModifiedTime()
		if plmt {
			err := os.Chtimes(jptm.Info().Destination, lMTime, lMTime)
			if err != nil {
				jptm.LogDownloadError(info.Source, info.Destination, "Preserving Modified Time Error "+err.Error(), 0)
				jptm.SetStatus(common.ETransferStatus.Failed())
				// Since the transfer failed, the file created above should be deleted
				err = deleteFile(info.Destination)
				if err != nil {
					// If there was an error deleting the file, log the error
					jptm.LogError(info.Destination, "Delete File Error ", err)
				}
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" Preserved last modified time for %s", info.Destination))
			}
		}

		// executing the epilogue.
		jptm.Log(pipeline.LogInfo, "DOWNLOAD SUCCESSFUL")
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()

	} else { // 3b: source has content
		dstFile, err := common.CreateFileOfSize(info.Destination, blobSize)
		if err != nil {
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// Since the transfer failed, the file created above should be deleted
			// If there was an error while opening / creating the file, delete will fail.
			// But delete is required when error occurred while truncating the file and
			// in this case file should be deleted.
			err = deleteFile(info.Destination)
			if err != nil {
				// If there was an error deleting the file, log the error
				jptm.LogError(info.Destination, "Delete File Error ", err)
			}
			jptm.ReportTransferDone()
			return
		}
		dstFileInfo, err := dstFile.Stat()
		if err != nil || (dstFileInfo.Size() != blobSize) {
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// Since the transfer failed, the file created above should be deleted
			// If there was an error while opening / creating the file, delete will fail.
			// But delete is required when error occurred while truncating the file and
			// in this case file should be deleted.
			err = deleteFile(info.Destination)
			if err != nil {
				// If there was an error deleting the file, log the error
				jptm.LogError(info.Destination, "Delete File Error ", err)
			}
			jptm.ReportTransferDone()
			return
		}

		defer dstFile.Close()

		dstMMF, err := common.NewMMF(dstFile, true, 0, info.SourceSize)
		if err != nil {
			jptm.LogDownloadError(info.Source, info.Destination, "Memory Map Error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// Since the transfer failed, the file created above should be deleted
			err = deleteFile(info.Destination)
			if err != nil {
				// If there was an error deleting the file, log the error
				jptm.LogError(info.Destination, "Delete File Error ", err)
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
			jptm.ScheduleChunks(generateDownloadBlobFunc(jptm, info.Source, info.Destination, srcBlobURL, blockIdCount, dstMMF, startIndex, adjustedChunkSize, pacer))
			blockIdCount++
		}
	}
}

func generateDownloadBlobFunc(jptm IJobPartTransferMgr, source, destination string, transferBlobURL azblob.BlobURL, chunkId int32, destinationMMF *common.MMF, startIndex int64, adjustedChunkSize int64, p *pacer) chunkFunc {
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
			lastChunk, _ := jptm.ReportChunkDone()
			if lastChunk {
				if jptm.ShouldLog(pipeline.LogDebug) {
					jptm.Log(pipeline.LogDebug, " Finalizing Transfer Cancellation")
				}
				destinationMMF.Unmap()
				// If the current transfer status value is less than or equal to 0
				// then transfer either failed or was cancelled
				// the file created locally should be deleted
				if jptm.TransferStatus() <= 0 {
					err := deleteFile(destination)
					if err != nil {
						// If there was an error deleting the file, log the error
						jptm.LogError(destination, "Delete File Error ", err)
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
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					jptm.LogDownloadError(source, destination, msg, status)
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
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					jptm.LogDownloadError(source, destination, msg, status)
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}

			lastChunk, _ := jptm.ReportChunkDone()
			// step 3: check if this is the last chunk
			if lastChunk {
				// step 4: this is the last block, perform EPILOGUE
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "DOWNLOAD SUCCESSFUL")
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				if jptm.ShouldLog(pipeline.LogDebug) {
					jptm.Log(pipeline.LogDebug, "Finalizing Transfer")
				}
				jptm.ReportTransferDone()

				destinationMMF.Unmap()

				lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						jptm.LogError(destination, "Changing Modified Time ", err)
						return
					}
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" Preserved Modified Time for %s", destination))
					}
				}
			}
		}
	}
}

// create an empty file and its parent directories, without any content
func createEmptyFile(destinationPath string) error {
	common.CreateParentDirectoryIfNotExist(destinationPath)
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
