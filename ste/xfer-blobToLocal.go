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
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"io"
	"net/url"
	"os"
	"strings"
)

func BlobToLocalPrologue(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {

	info := jptm.Info()
	u, _ := url.Parse(info.Source)
	srcBlobURL := azblob.NewBlobURL(*u, p)
	// step 2: get size info for the download
	blobSize := int64(info.SourceSize)
	downloadChunkSize := int64(info.BlockSize)
	numChunks := uint32(0)

	// If the transfer was cancelled, then reporting transfer as done and increasing the bytestransferred by the size of the source.
	if jptm.WasCanceled(){
		jptm.AddToBytesTransferred(info.SourceSize)
		jptm.ReportTransferDone()
		return
	}

	// step 3: prep local file before download starts
	if blobSize == 0 {
		err := createEmptyFile(info.Destination)
		if err != nil{
			if strings.Contains(err.Error(), "too many open files"){
				// dst file could not be created because azcopy process
				// reached the open file descriptor limit set for each process.
				// Rescheduling the transfer.
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, " rescheduled since process reached open file descriptor limit.")
				}
				jptm.RescheduleTransfer()
			}else{
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "transfer failed because dst file could not be created locally. Failed with error " + err.Error())
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
		dstFile, err := createFileOfSize(info.Destination, blobSize)
		if err != nil {
			if strings.Contains(err.Error(), "too many open files"){
				// dst file could not be created because azcopy process
				// reached the open file descriptor limit set for each process.
				// Rescheduling the transfer.
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, " rescheduled since process reached open file descriptor limit.")
				}
				jptm.RescheduleTransfer()
			}else{
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, "transfer failed because dst file could not be created locally. Failed with error " + err.Error())
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
		if rem := blobSize % downloadChunkSize; rem == 0 {
			numChunks = uint32(blobSize / downloadChunkSize)
		} else {
			numChunks = uint32(blobSize / downloadChunkSize + 1)
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
			jptm.ScheduleChunks(generateDownloadFunc(jptm, srcBlobURL, blockIdCount, dstFile, dstMMF, startIndex, adjustedChunkSize))
			blockIdCount++
		}
	}
}

func generateDownloadFunc(jptm IJobPartTransferMgr, transferBlobURL azblob.BlobURL, chunkId int32, destinationFile *os.File, destinationMMF common.MMF, startIndex int64, adjustedChunkSize int64) chunkFunc {
	return func(workerId int) {
		chunkDone := func() {
			// adding the bytes transferred or skipped of a transfer to determine the progress of transfer.
			jptm.AddToBytesTransferred(adjustedChunkSize)
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
			// step 1: adding the chunks size to bytesOverWire and perform get
			jptm.AddToBytesOverWire(uint64(adjustedChunkSize))
			get, err := transferBlobURL.Download(jptm.Context(), startIndex, adjustedChunkSize, azblob.BlobAccessConditions{}, false)
			if err != nil {
				if !jptm.WasCanceled() {
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
					}
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}
			// step 2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(get.Body(), destinationMMF[startIndex:startIndex+adjustedChunkSize])
			get.Body().Close()
			if int64(bytesRead) != adjustedChunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				if !jptm.WasCanceled() {
					jptm.Cancel()
					if jptm.ShouldLog(pipeline.LogInfo) {
						jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
					}
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}


			lastChunk, nc := jptm.ReportChunkDone()
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("is last chunk %s and no of chunk %d", lastChunk, nc))
			// step 3: check if this is the last chunk
			if lastChunk {
				// step 4: this is the last block, perform EPILOGUE
				if jptm.ShouldLog(pipeline.LogInfo) {
					jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
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
					fmt.Println("last modified time ", lastModifiedTime)
					fmt.Println("destination ", jptm.Info().Destination)
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
