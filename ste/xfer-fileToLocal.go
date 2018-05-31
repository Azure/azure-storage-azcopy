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
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

// TODO: Unify blobToLocal and fileToLocal after code review and logic is finalized.
func FileToLocal(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	info := jptm.Info()
	u, _ := url.Parse(info.Source)
	srcFileURL := azfile.NewFileURL(*u, p)
	// step 2: get size info for the download
	fileSize := int64(info.SourceSize)
	downloadChunkSize := int64(info.BlockSize)
	numChunks := uint32(0)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled() {
		jptm.AddToBytesDone(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	// If the force Write flags is set to false
	// then check the file exists locally or not.
	// If it does, mark transfer as failed.
	if !jptm.IsForceWriteTrue() {
		_, err := os.Stat(info.Destination)
		if err == nil{
			// If the error is nil, then blob exists locally and it doesn't needs to be downloaded.
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("skipping the transfer since blob already exists"))
			}
			// Mark the transfer as failed with FileAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure())
			jptm.AddToBytesDone(info.SourceSize)
			jptm.ReportTransferDone()
			return
		}
	}

	// step 3: prep local file before download starts
	if fileSize == 0 {
		err := createEmptyFile(info.Destination)
		if err != nil {
			if strings.Contains(err.Error(), "too many open files") {
				// dst file could not be created because azcopy process
				// reached the open file descriptor limit set for each process.
				// Rescheduling the transfer.
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, " rescheduled since process reached open file descriptor limit.")
				}
				jptm.RescheduleTransfer()
			} else {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "transfer failed because dst file could not be created locally. Failed with error "+err.Error())
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
			}
			return
		}
		lMTime, plmt := jptm.PreserveLastModifiedTime()
		if plmt {
			err := os.Chtimes(jptm.Info().Destination, lMTime, lMTime)
			if err != nil {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" failed while preserving last modified time for destionation %s", info.Destination))
				}
				return
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf(" successfully preserved the last modified time for destinaton %s", info.Destination))
			}
		}

		// executing the epilogue.
		jptm.Log(pipeline.LogInfo, " concluding the download Transfer of job after creating an empty file")
		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()

	} else { // 3b: source has content
		dstFile, err := createFileOfSize(info.Destination, fileSize)
		if err != nil {
			if strings.Contains(err.Error(), "too many open files") {
				// dst file could not be created because azcopy process
				// reached the open file descriptor limit set for each process.
				// Rescheduling the transfer.
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, " rescheduled since process reached open file descriptor limit.")
				}
				jptm.RescheduleTransfer()
			} else {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "transfer failed because dst file could not be created locally. Failed with error "+err.Error())
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
			}
			return
		}

		dstMMF, err := common.NewMMF(dstFile, true, 0, info.SourceSize)
		if err != nil {
			dstFile.Close()
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "transfer failed because dst file did not memory mapped successfully")
			}
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		if rem := fileSize % downloadChunkSize; rem == 0 {
			numChunks = uint32(fileSize / downloadChunkSize)
		} else {
			numChunks = uint32(fileSize/downloadChunkSize + 1)
		}
		jptm.SetNumberOfChunks(numChunks)
		chunkIDCount := int32(0)
		// step 4: go through the file range and schedule download chunk jobs
		for startIndex := int64(0); startIndex < fileSize; startIndex += downloadChunkSize {
			adjustedChunkSize := downloadChunkSize

			// compute exact size of the chunk
			if startIndex+downloadChunkSize > fileSize {
				adjustedChunkSize = fileSize - startIndex
			}

			// schedule the download chunk job
			jptm.ScheduleChunks(generateDownloadFileFunc(jptm, srcFileURL, chunkIDCount, dstFile, dstMMF, startIndex, adjustedChunkSize))
			chunkIDCount++
		}
	}
}

func generateDownloadFileFunc(jptm IJobPartTransferMgr, transferFileURL azfile.FileURL, chunkID int32, destinationFile *os.File, destinationMMF common.MMF, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {
		chunkDone := func() {
			// adding the bytes transferred or skipped of a transfer to determine the progress of transfer.
			jptm.AddToBytesDone(adjustedChunkSize)
			lastChunk, _ := jptm.ReportChunkDone()
			if lastChunk {
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				}
				jptm.ReportTransferDone()
				destinationMMF.Unmap()
				err := destinationFile.Close()
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which failed closing the file %s", workerId, destinationFile.Name()))
					}
				}
			}
		}
		if jptm.WasCanceled() {
			chunkDone()
		} else {
			// step 1: Downloading the file from range startIndex till (startIndex + adjustedChunkSize)
			get, err := transferFileURL.Download(jptm.Context(), startIndex, adjustedChunkSize, false)
			if err != nil {
				if !jptm.WasCanceled() {
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkID %d because writing to file for startIndex of %d has failed", workerId, chunkID, startIndex))
					}
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}

			// step 2: write the body into the memory mapped file directly
			retryReader := get.Body(azfile.RetryReaderOptions{MaxRetryRequests: DownloadMaxTries})
			_, err = io.ReadFull(retryReader, destinationMMF[startIndex:startIndex+adjustedChunkSize])
			io.Copy(ioutil.Discard, retryReader)
			retryReader.Close()
			if err != nil {
				// cancel entire transfer because this chunk has failed
				if !jptm.WasCanceled() {
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkID %d because reading the downloaded chunk failed. Failed with error %s", workerId, chunkID, err.Error()))
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
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkID %d", workerId, chunkID))
				}
				jptm.SetStatus(common.ETransferStatus.Success())
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is finalizing Transfer", workerId))
				}
				jptm.ReportTransferDone()

				destinationMMF.Unmap()
				err := destinationFile.Close()
				if err != nil {
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which failed closing the file %s", workerId, destinationFile.Name()))
					}
				}

				lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						if jptm.ShouldLog(pipeline.LogInfo) {
							jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which failed while preserving last modified time for destionation %s", workerId, destinationFile.Name()))
						}
						return
					}
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which successfully preserve the last modified time for destinaton %s", workerId, destinationFile.Name()))
					}
				}
			}
		}
	}
}
