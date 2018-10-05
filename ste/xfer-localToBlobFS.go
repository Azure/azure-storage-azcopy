package ste

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

type fileRangeAppend struct {
	jptm    IJobPartTransferMgr
	srcFile *os.File
	fileUrl azbfs.FileURL
	pacer   *pacer
}

func LocalToBlobFS(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer *pacer) {
	// transferDone is the internal api that sets the transfer status
	// and report transfer done
	transferDone := func(status common.TransferStatus) {
		jptm.SetStatus(status)
		jptm.ReportTransferDone()
	}
	// step 1. Get the transfer Information which include source, destination string, source size and other information.
	info := jptm.Info()
	sourceSize := int64(info.SourceSize)
	chunkSize := int64(info.BlockSize)

	// Get the file/dir Info to determine whether source is a file or directory
	// since url to upload files and directories is different
	fInfo, err := os.Stat(info.Source)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, err.Error(), 0)
		transferDone(common.ETransferStatus.Failed())
		return
	}
	// parse the destination Url
	dUrl, err := url.Parse(info.Destination)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "Url Parsing Error "+err.Error(), 0)
		transferDone(common.ETransferStatus.Failed())
		return
	}

	// If the source is a directory
	if fInfo.IsDir() {
		dirUrl := azbfs.NewDirectoryURL(*dUrl, p)
		_, err := dirUrl.Create(jptm.Context())
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogUploadError(info.Source, info.Destination, "Directory creation error "+msg, status)
			if jptm.WasCanceled() {
				transferDone(jptm.TransferStatus())
			} else {
				transferDone(common.ETransferStatus.Failed())
			}
			return
		}
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
		}
		transferDone(common.ETransferStatus.Success())
		return
	}

	// If the file Size is 0, there is no need to open the file and memory map it
	if fInfo.Size() == 0 {
		fileUrl := azbfs.NewFileURL(*dUrl, p)
		_, err = fileUrl.Create(jptm.Context())
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogUploadError(info.Source, info.Destination, "File creation Eror "+msg, status)
			transferDone(common.ETransferStatus.Failed())
			return
		}
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, "UPLOAD SUCCESSFUL")
		}
		transferDone(common.ETransferStatus.Success())
		return
	}

	// Open the source file and memory map it
	srcfile, err := os.Open(info.Source)
	if err != nil {
		jptm.LogUploadError(info.Source, info.Destination, "File Open Error "+err.Error(), 0)
		transferDone(common.ETransferStatus.Failed())
		return
	}

	// Since the source is a file, it can be uploaded by appending the ranges to file concurrently
	// before the ranges are appended, file has to be created first and the ranges are scheduled to append
	//Create the fileUrl and then create the file on FileSystem
	fileUrl := azbfs.NewFileURL(*dUrl, p)
	_, err = fileUrl.Create(jptm.Context())
	if err != nil {
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(info.Source, info.Destination, "File creation Eror "+msg, status)
		transferDone(common.ETransferStatus.Failed())
		return
	}
	// Calculate the number of file Ranges for the given fileSize.
	numRanges := common.Iffuint32(sourceSize%chunkSize == 0,
		uint32(sourceSize/chunkSize),
		uint32(sourceSize/chunkSize)+1)

	// set the number of ranges for the given file
	jptm.SetNumberOfChunks(numRanges)

	fru := &fileRangeAppend{
		jptm:    jptm,
		srcFile: srcfile,
		fileUrl: fileUrl,
		pacer:   pacer}

	// Scheduling page range update to the Page Blob created above.
	for startIndex := int64(0); startIndex < sourceSize; startIndex += chunkSize {
		adjustedRangeInterval := chunkSize
		// compute actual size of the chunk
		if startIndex+chunkSize > sourceSize {
			adjustedRangeInterval = sourceSize - startIndex
		}
		// schedule the chunk job/msg
		jptm.ScheduleChunks(fru.fileRangeAppend(startIndex, adjustedRangeInterval))
	}
}

// fileRangeAppend is the api that is used to append the range to a file from range startRange to (startRange + calculatedRangeInterval)
func (fru *fileRangeAppend) fileRangeAppend(startRange int64, calculatedRangeInterval int64) chunkFunc {
	return func(workerId int) {
		info := fru.jptm.Info()

		// transferDone is the internal function which called by the last range append
		// it unmaps the source file and delete the file in case transfer failed
		transferDone := func() {

			fru.srcFile.Close()
			// if the transfer status is less than or equal to 0, it means that transfer was cancelled or failed
			// in this case, we need to delete the file which was created before any range was appended
			if fru.jptm.TransferStatus() <= 0 {
				_, err := fru.fileUrl.Delete(context.Background())
				if err != nil {
					fru.jptm.LogError(fru.fileUrl.String(), "Delete Remote File Error ", err)
				}
			}
			// report transfer done
			fru.jptm.ReportTransferDone()
		}
		if fru.jptm.WasCanceled() {
			if fru.jptm.ShouldLog(pipeline.LogDebug) {
				fru.jptm.Log(pipeline.LogDebug, fmt.Sprintf("Chunk of cancelled transfer not picked "))
			}
			// report the chunk done
			// if it is the last range that was scheduled to be appended to the file
			// report transfer done
			lastRangeDone, _ := fru.jptm.ReportChunkDone()
			if lastRangeDone {
				transferDone()
			}
			return
		}

		srcMMF := &common.MMF{}
		srcMMF, err := common.NewMMF(fru.srcFile, false, startRange, calculatedRangeInterval)
		if err != nil {
			// If the file append range failed, it could be that transfer was cancelled
			// status of transfer does not change when it is cancelled
			if fru.jptm.WasCanceled() {
				if fru.jptm.ShouldLog(pipeline.LogDebug) {
					fru.jptm.Log(pipeline.LogDebug, "Append Range of cancelled transfer not processed")
				}
			} else {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				// If the transfer was not cancelled, then append range failed due to some other reason
				fru.jptm.LogUploadError(info.Source, info.Destination, msg, status)
				// cancel the transfer
				fru.jptm.Cancel()
				fru.jptm.SetStatus(common.ETransferStatus.Failed())
				fru.jptm.SetErrorCode(int32(status))
			}
			// report the number of range done
			lastRangeDone, _ := fru.jptm.ReportChunkDone()
			// if the current range is the last range to be appended for the transfer
			// report transfer done
			if lastRangeDone {
				transferDone()
			}
			return
		}
		defer srcMMF.Unmap()

		body := newRequestBodyPacer(bytes.NewReader(srcMMF.Slice()), fru.pacer, srcMMF)
		_, err = fru.fileUrl.AppendData(fru.jptm.Context(), startRange, body)
		if err != nil {
			// If the file append range failed, it could be that transfer was cancelled
			// status of transfer does not change when it is cancelled
			if fru.jptm.WasCanceled() {
				if fru.jptm.ShouldLog(pipeline.LogDebug) {
					fru.jptm.Log(pipeline.LogDebug, "Append Range of cancelled transfer not processed")
				}
			} else {
				status, msg := ErrorEx{err}.ErrorCodeAndString()
				// If the transfer was not cancelled, then append range failed due to some other reason
				fru.jptm.LogUploadError(info.Source, info.Destination, msg, status)
				// cancel the transfer
				fru.jptm.Cancel()
				fru.jptm.SetStatus(common.ETransferStatus.Failed())
				fru.jptm.SetErrorCode(int32(status))
			}
			// report the number of range done
			lastRangeDone, _ := fru.jptm.ReportChunkDone()
			// if the current range is the last range to be appended for the transfer
			// report transfer done
			if lastRangeDone {
				transferDone()
			}
			return
		}
		// successfully appended the range to the file
		if fru.jptm.ShouldLog(pipeline.LogDebug) {
			fru.jptm.Log(pipeline.LogDebug, fmt.Sprintf("Append Range Successful for startrange %d "+
				"and rangeInterval %d", startRange, calculatedRangeInterval))
		}

		//report the chunkDone
		lastRangeDone, _ := fru.jptm.ReportChunkDone()
		// if this the last range, then transfer needs to be concluded
		if lastRangeDone {
			// If the transfer was cancelled before the ranges could be flushed
			if fru.jptm.WasCanceled() {
				transferDone()
				return
			}
			_, err = fru.fileUrl.FlushData(fru.jptm.Context(), fru.jptm.Info().SourceSize)
			if err != nil {
				if fru.jptm.WasCanceled() {
					// Flush Range failed because the transfer was cancelled
					if fru.jptm.ShouldLog(pipeline.LogDebug) {
						fru.jptm.Log(pipeline.LogDebug, fmt.Sprintf("Cancelled transfer range %d %d not flushed ", startRange, startRange+calculatedRangeInterval))
					}
				} else {
					status, msg := ErrorEx{err}.ErrorCodeAndString()
					fru.jptm.LogUploadError(info.Source, info.Destination, msg, status)
					fru.jptm.Cancel()
					fru.jptm.SetStatus(common.ETransferStatus.Failed())
					fru.jptm.SetErrorCode(int32(status))
				}
				transferDone()
				return
			}
			if fru.jptm.ShouldLog(pipeline.LogError) {
				fru.jptm.Log(pipeline.LogError, "UPLOAD SUCCESSFUL")
			}
			fru.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
			return
		}
	}
}
