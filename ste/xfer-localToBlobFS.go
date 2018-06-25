package ste

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
)

type fileRangeAppend struct {
	jptm    IJobPartTransferMgr
	srcMmf  *common.MMF
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
		if jptm.ShouldLog(pipeline.LogError) {
			jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. error getting the source info %s", info.Source))
		}
		transferDone(common.ETransferStatus.Failed())
		return
	}
	// parse the destination Url
	dUrl, err := url.Parse(info.Destination)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogError) {
			jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. error parsing the destination Url %s", info.Destination))
		}
		transferDone(common.ETransferStatus.Failed())
		return
	}

	// If the source is a directory
	if fInfo.IsDir() {
		dirUrl := azbfs.NewDirectoryURL(*dUrl, p)
		_, err := dirUrl.Create(jptm.Context())
		if err != nil {
			if jptm.WasCanceled() {
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, fmt.Sprintf("creating directory %s failed since transfer was cancelled ", info.Destination))
				}
				transferDone(jptm.TransferStatus())
			} else {
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. Creating directory %s failed with error ", err.Error()))
				}
				transferDone(common.ETransferStatus.Failed())
			}
			return
		}
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobFSUploadSuccessful. created directory for the given destination url %s", info.Destination))
		}
		transferDone(common.ETransferStatus.Success())
		return
	}

	// If the file Size is 0, there is no need to open the file and memory map it
	if fInfo.Size() == 0 {
		fileUrl := azbfs.NewFileURL(*dUrl, p)
		_, err = fileUrl.Create(jptm.Context())
		if err != nil {
			if jptm.ShouldLog(pipeline.LogError) {
				jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. Error creating the file for destination url %s. failed with error %s", info.Destination, err.Error()))
			}
			transferDone(common.ETransferStatus.Failed())
			return
		}
		if jptm.ShouldLog(pipeline.LogInfo) {
			jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobFSUploadSuccessful. Created the empty file for destination url %s", info.Destination))
		}
		transferDone(common.ETransferStatus.Success())
		return
	}

	// Open the source file and memory map it
	srcfile, err := os.Open(info.Source)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogError) {
			jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. Error opening the source file %s. Failed with error %s", info.Source, err.Error()))
		}
		transferDone(common.ETransferStatus.Failed())
		return
	}
	defer srcfile.Close()

	// memory map the source file
	srcMmf, err := common.NewMMF(srcfile, false, 0, sourceSize)
	if err != nil {
		if jptm.ShouldLog(pipeline.LogError) {
			jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. Error mapping the source file %s. failed with error %s", info.Source, err.Error()))
		}
		transferDone(common.ETransferStatus.Failed())
		return
	}

	// Since the source is a file, it can be uploaded by appending the ranges to file concurrently
	// before the ranges are appended, file has to be created first and the ranges are scheduled to append
	//Create the fileUrl and then create the file on FileSystem
	fileUrl := azbfs.NewFileURL(*dUrl, p)
	_, err = fileUrl.Create(jptm.Context())
	if err != nil {
		if jptm.ShouldLog(pipeline.LogError) {
			jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed. Error creating the file for destination url %s. failed with error %s", info.Destination, err.Error()))
		}
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
		srcMmf:  srcMmf,
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

		// This function allows routine to manage behavior of unexpected panics.
		// The panic error along with transfer details are logged.
		// The transfer is marked as failed and is reported as done.
		defer func(jptm IJobPartTransferMgr) {
			r := recover()
			if r != nil {
				// Get the transfer Info and log the details
				info := jptm.Info()
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, fmt.Sprintf(" recovered from unexpected crash %s. Transfer Src %s Dst %s SrcSize %v startRange %v calculatedRangeInterval %v sourceMMF size %v",
						r, info.Source, info.Destination, info.SourceSize, startRange, calculatedRangeInterval, len(fru.srcMmf.Slice())))
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
			}
		}(fru.jptm)

		// transferDone is the internal function which called by the last range append
		// it unmaps the source file and delete the file in case transfer failed
		transferDone := func() {
			// unmap the source file
			fru.srcMmf.Unmap()
			// if the transfer status is less than or equal to 0, it means that transfer was cancelled or failed
			// in this case, we need to delete the file which was created before any range was appended
			if fru.jptm.TransferStatus() <= 0 {
				_, err := fru.fileUrl.Delete(context.Background())
				if err != nil {
					if fru.jptm.ShouldLog(pipeline.LogInfo) {
						fru.jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the file %s. failed with error %s", fru.fileUrl, err.Error()))
					}
				}
			}
			// report transfer done
			fru.jptm.ReportTransferDone()
		}
		if fru.jptm.WasCanceled() {
			if fru.jptm.ShouldLog(pipeline.LogInfo) {
				fru.jptm.Log(pipeline.LogInfo, fmt.Sprintf("not picking up the chunk since transfer was cancelled"))
			}
			// add range updated to bytes done for progress
			fru.jptm.AddToBytesDone(calculatedRangeInterval)
			// report the chunk done
			// if it is the last range that was scheduled to be appended to the file
			// report transfer done
			lastRangeDone, _ := fru.jptm.ReportChunkDone()
			if lastRangeDone {
				transferDone()
			}
			return
		}

		body := newRequestBodyPacer(bytes.NewReader(fru.srcMmf.Slice()[startRange:startRange+calculatedRangeInterval]), fru.pacer, fru.srcMmf)
		_, err := fru.fileUrl.AppendData(fru.jptm.Context(), startRange, body)
		if err != nil {
			// If the file append range failed, it could be that transfer was cancelled
			// status of transfer does not change when it is cancelled
			if fru.jptm.WasCanceled() {
				if fru.jptm.ShouldLog(pipeline.LogInfo) {
					fru.jptm.Log(pipeline.LogInfo, fmt.Sprintf("error appending the range to the file %s since transfer was cancelled", fru.fileUrl))
				}
			} else {
				// If the transfer was not cancelled, then append range failed due to some other reason
				if fru.jptm.ShouldLog(pipeline.LogInfo) {
					fru.jptm.Log(pipeline.LogInfo, fmt.Sprintf("BlobFSUploadFailed while appending the range to the file %s for startIndex %s and range interval %s. "+
						"Failed with error %s", fru.fileUrl, startRange, calculatedRangeInterval, err.Error()))
				}
				// cancel the transfer
				fru.jptm.Cancel()
				fru.jptm.SetStatus(common.ETransferStatus.Failed())
			}
			// add range updated to bytes done for progress
			fru.jptm.AddToBytesDone(calculatedRangeInterval)
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
		if fru.jptm.ShouldLog(pipeline.LogInfo) {
			fru.jptm.Log(pipeline.LogInfo, fmt.Sprintf("successfully appended the range for file %s for startrange %d "+
				"and rangeInterval %s", fru.fileUrl, startRange, calculatedRangeInterval))
		}

		// add range updated to bytes done for progress
		fru.jptm.AddToBytesDone(calculatedRangeInterval)

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
					if fru.jptm.ShouldLog(pipeline.LogInfo) {
						fru.jptm.Log(pipeline.LogInfo, fmt.Sprintf("error flushing the ranges for file %s since transfer was cancelled ", fru.fileUrl))
					}
				} else {
					if fru.jptm.ShouldLog(pipeline.LogError) {
						fru.jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadFailed while flushing the ranges for file %s failed with error %s", fru.fileUrl, err.Error()))
					}
					fru.jptm.Cancel()
					fru.jptm.SetStatus(common.ETransferStatus.Failed())
				}
				transferDone()
				return
			}
			if fru.jptm.ShouldLog(pipeline.LogError) {
				fru.jptm.Log(pipeline.LogError, fmt.Sprintf("BlobFSUploadSuccessful. successfully flushed the ranges for file %s", fru.fileUrl))
			}
			fru.jptm.SetStatus(common.ETransferStatus.Success())
			transferDone()
			return
		}
	}
}
