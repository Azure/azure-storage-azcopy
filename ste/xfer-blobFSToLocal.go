package ste

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

type BlobFSFileDownload struct {
	jptm        IJobPartTransferMgr
	srcFileURL  azbfs.FileURL
	source      string
	destination string
	destMMF     *common.MMF
	pacer       *pacer
}

func BlobFSToLocal(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {
	// step 1: get the source, destination info for the transfer.
	info := jptm.Info()
	u, _ := url.Parse(info.Source)
	srcBlobURL := azbfs.NewDirectoryURL(*u, p)

	// step 2: get size info for the download
	sourceSize := int64(info.SourceSize)
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
			// If the error is nil, then blob exists locally and it doesn't needs to be downloaded.
			jptm.LogError(info.Destination, "File Already Exists", err)
			// Mark the transfer as failed with BlobAlreadyExistsFailure
			jptm.SetStatus(common.ETransferStatus.FileAlreadyExistsFailure())
			jptm.ReportTransferDone()
			return
		}
	}

	// step 3: prep local file before download starts
	// If the size of the file to be downloaded is 0
	// We don't need to schedule the download in the chunk Channel.
	if sourceSize == 0 {
		// create the empty file
		// preserve the modified time
		err := createEmptyFile(info.Destination)
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+msg, status)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		lMTime, plmt := jptm.PreserveLastModifiedTime()
		if plmt {
			err := os.Chtimes(jptm.Info().Destination, lMTime, lMTime)
			if err != nil {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				jptm.LogDownloadError(info.Source, info.Destination, "Preserve Modified Time Error "+msg, status)
				jptm.SetStatus(common.ETransferStatus.Failed())
				// Since the transfer failed, the file created above should be deleted
				err = deleteFile(info.Destination)
				if err != nil {
					// If there was an error deleting the file, log the error
					jptm.LogError(info.Destination, "Delete File Error ", err)
				}
			}
			if jptm.ShouldLog(pipeline.LogInfo) {
				jptm.Log(pipeline.LogInfo, "DOWNLOAD SUCCESSFUL")
			}
		}

		jptm.SetStatus(common.ETransferStatus.Success())
		jptm.ReportTransferDone()

	} else { // 3b: source has content
		dstFile, err := common.CreateFileOfSize(info.Destination, sourceSize)
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+msg, status)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}

		defer dstFile.Close()

		dstMMF, err := common.NewMMF(dstFile, true, 0, info.SourceSize)
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogDownloadError(info.Source, info.Destination, "Memory Map Error "+msg, status)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// Since the transfer failed, the file created above should be deleted
			err = deleteFile(info.Destination)
			if err != nil {
				// If there was an error deleting the file, log the error
				// If there was an error deleting the file, log the error
				jptm.LogError(info.Destination, "Delete File Error ", err)
			}
			jptm.ReportTransferDone()
			return
		}
		if rem := sourceSize % downloadChunkSize; rem == 0 {
			numChunks = uint32(sourceSize / downloadChunkSize)
		} else {
			numChunks = uint32(sourceSize/downloadChunkSize + 1)
		}
		jptm.SetNumberOfChunks(numChunks)
		blockIdCount := int32(0)
		bffd := &BlobFSFileDownload{jptm: jptm,
			srcFileURL:  srcBlobURL.NewFileUrl(),
			source:      info.Source,
			destination: info.Destination,
			destMMF:     dstMMF,
			pacer:       pacer}
		// step 4: go through the blob range and schedule download chunk jobs
		for startIndex := int64(0); startIndex < sourceSize; startIndex += downloadChunkSize {
			adjustedChunkSize := downloadChunkSize

			// compute exact size of the chunk
			if startIndex+downloadChunkSize > sourceSize {
				adjustedChunkSize = sourceSize - startIndex
			}
			// schedule the download chunk job
			jptm.ScheduleChunks(bffd.generateDownloadFileFunc(blockIdCount, startIndex, adjustedChunkSize))
			blockIdCount++
		}
	}
}

func (bffd *BlobFSFileDownload) generateDownloadFileFunc(blockIdCount int32, startIndex int64, adjustedRangeSize int64) chunkFunc {
	return func(workerId int) {

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		//defer func(jptm IJobPartTransferMgr) {
		//	r := recover()
		//	if r != nil {
		//		// Get the transfer Info and log the details
		//		info := jptm.Info()
		//		if jptm.ShouldLog(pipeline.LogError) {
		//			jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startIndex %v adjustedRangeSize %v destinationMMF size %v",
		//				r, info.Source, info.Destination, info.SourceSize, startIndex, adjustedRangeSize, len(bffd.destMMF.Slice())))
		//		}
		//		jptm.SetStatus(common.ETransferStatus.Failed())
		//		jptm.ReportTransferDone()
		//	}
		//}(bffd.jptm)

		info := bffd.jptm.Info()
		// chunkDone is an internal function which marks a chunkDone
		// Check if the current chunk is the last Chunk
		// unmaps the source
		// Report transfer done
		// If the transfer status is less than 0, it means transfer either got failed or cancelled
		// Perform the clean up
		chunkDone := func() {
			lastChunk, _ := bffd.jptm.ReportChunkDone()
			if lastChunk {
				if bffd.jptm.ShouldLog(pipeline.LogInfo) {
					bffd.jptm.Log(pipeline.LogInfo, fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				}
				bffd.destMMF.Unmap()
				bffd.jptm.ReportTransferDone()
				// If the status of transfer is less than or equal to 0
				// then transfer failed or cancelled
				// the downloaded file needs to be deleted
				if bffd.jptm.TransferStatus() <= 0 {
					err := os.Remove(info.Destination)
					if err != nil {
						if bffd.jptm.ShouldLog(pipeline.LogError) {
							bffd.jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the file %s. Failed with error %s", bffd.jptm.Info().Destination, err.Error()))
						}
					}
				}
			}
		}
		if bffd.jptm.WasCanceled() {
			chunkDone()
		} else {
			// step 1: Downloading the file from range startIndex till (startIndex + adjustedRangeSize)
			get, err := bffd.srcFileURL.Download(bffd.jptm.Context(), startIndex, adjustedRangeSize)
			if err != nil {
				if !bffd.jptm.WasCanceled() {
					bffd.jptm.Cancel()
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					bffd.jptm.LogDownloadError(bffd.source, bffd.destination, msg, status)
					bffd.jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}

			// step 2: write the body into the memory mapped file directly
			resp := get.Body(azbfs.RetryReaderOptions{MaxRetryRequests: MaxRetryPerDownloadBody})
			body := newResponseBodyPacer(resp, bffd.pacer, bffd.destMMF)
			_, err = io.ReadFull(body, bffd.destMMF.Slice()[startIndex:startIndex+adjustedRangeSize])
			// reading the response and closing the resp body
			if resp != nil {
				io.Copy(ioutil.Discard, resp)
				resp.Close()
			}
			if err != nil {
				// cancel entire transfer because this chunk has failed
				if !bffd.jptm.WasCanceled() {
					bffd.jptm.Cancel()
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					bffd.jptm.LogDownloadError(bffd.source, bffd.destination, msg, status)
					bffd.jptm.SetStatus(common.ETransferStatus.Failed())
				}
				chunkDone()
				return
			}

			lastChunk, _ := bffd.jptm.ReportChunkDone()
			// step 3: check if this is the last chunk
			if lastChunk {
				// step 4: this is the last block, perform EPILOGUE
				if bffd.jptm.ShouldLog(pipeline.LogDebug) {
					bffd.jptm.Log(pipeline.LogDebug, fmt.Sprintf("Concluding Transfer after processing chunk %d", blockIdCount))
				}
				bffd.jptm.SetStatus(common.ETransferStatus.Success())
				if bffd.jptm.ShouldLog(pipeline.LogInfo) {
					bffd.jptm.Log(pipeline.LogInfo, "DOWNLOAD SUCCESSFUL")
				}
				bffd.jptm.ReportTransferDone()

				bffd.destMMF.Unmap()

				lastModifiedTime, preserveLastModifiedTime := bffd.jptm.PreserveLastModifiedTime()
				if preserveLastModifiedTime {
					err := os.Chtimes(bffd.jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
					if err != nil {
						bffd.jptm.LogError(bffd.destination, "Change Modified Time ", err)
						return
					}
					if bffd.jptm.ShouldLog(pipeline.LogInfo) {
						bffd.jptm.Log(pipeline.LogInfo, fmt.Sprintf("Preserved last modified time %s", info.Destination))
					}
				}
			}
		}
	}
}
