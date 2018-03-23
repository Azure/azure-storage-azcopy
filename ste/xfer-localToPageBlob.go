package ste
/*

import (
	"bytes"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"net/url"
	"os"
	"unsafe"
)

type localToPageBlob struct {
	transfer         *TransferMsg
	pacer            *pacer
	pageBlobUrl      azblob.PageBlobURL
	memoryMappedFile common.MMF
	srcFileHandler   *os.File
}

// return a new localToPageBlob struct targeting a specific transfer
func newLocalToPageBlob(transfer *TransferMsg, pacer *pacer) xfer {
	return &localToPageBlob{transfer: transfer, pacer: pacer}
}

// this function performs the setup for each transfer and schedules the corresponding page requests into the chunkChannel
func (localToPageBlob *localToPageBlob) runPrologue(chunkChannel chan<- ChunkMsg) {

	// step 1: create pipeline for the destination blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      UploadMaxTries,
			TryTimeout:    UploadTryTimeout,
			RetryDelay:    UploadRetryDelay,
			MaxRetryDelay: UploadMaxRetryDelay,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {
				localToPageBlob.transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(localToPageBlob.transfer.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(localToPageBlob.transfer.Destination)
	localToPageBlob.pageBlobUrl = azblob.NewPageBlobURL(*u, p)

	// step 2: map in the file to upload before appending blobs
	localToPageBlob.memoryMappedFile, localToPageBlob.srcFileHandler = executionEngineHelper{}.openAndMemoryMapFile(localToPageBlob.transfer.Source)
	blobHttpHeaders, metaData := localToPageBlob.transfer.blobHttpHeaderAndMetadata(localToPageBlob.memoryMappedFile)

	// step 3: Create Page Blob of the source size
	_, err := localToPageBlob.pageBlobUrl.Create(localToPageBlob.transfer.TransferContext, int64(localToPageBlob.transfer.SourceSize),
		0, blobHttpHeaders, metaData, azblob.BlobAccessConditions{})
	if err != nil {
		localToPageBlob.transfer.Log(common.LogError, fmt.Sprintf("failed since PageCreate failed due to %s", err.Error()))
		localToPageBlob.transfer.TransferCancelFunc()
		localToPageBlob.transfer.TransferDone()
		localToPageBlob.transfer.TransferStatus(common.TransferFailed)
		localToPageBlob.memoryMappedFile.Unmap()
		err = localToPageBlob.srcFileHandler.Close()
		if err != nil {
			localToPageBlob.transfer.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", localToPageBlob.srcFileHandler.Name(), err.Error()))
		}
		return
	}

	blobSize := int64(localToPageBlob.transfer.SourceSize)

	pageSize := int64(localToPageBlob.transfer.BlockSize)

	// step 4: Scheduling page range update to the Page Blob created in Step 3
	for startIndex := int64(0); startIndex < blobSize; startIndex += pageSize {
		adjustedPageSize := pageSize

		// compute actual size of the chunk
		if startIndex+pageSize > blobSize {
			adjustedPageSize = blobSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: localToPageBlob.generateUploadFunc(
				uint32(localToPageBlob.transfer.NumChunks),
				startIndex,
				adjustedPageSize),
		}
	}
}

func (localToPageBlob *localToPageBlob) generateUploadFunc(numberOfPages uint32, startPage int64, pageSize int64) chunkFunc {
	return func(workerId int) {
		t := localToPageBlob.transfer
		file := localToPageBlob.srcFileHandler
		// chunk done is the function called after success / failure of each chunk.
		// If the calling chunk is the last chunk of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		pageDone := func(status common.TransferStatus) {
			if t.ChunksDone() == numberOfPages {
				// Transfer status
				if status != common.TransferInProgress {
					t.TransferStatus(status)
				}
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is finalizing transfer", workerId))
				t.TransferDone()
				localToPageBlob.memoryMappedFile.Unmap()
				err := file.Close()
				if err != nil {
					t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
				}
			}
		}

		if t.TransferContext.Err() != nil {
			t.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up page %d", startPage))
			pageDone(common.TransferInProgress)
		} else {
			// pageBytes is the byte slice of Page for the given page range
			pageBytes := localToPageBlob.memoryMappedFile[startPage : startPage+pageSize]
			// converted the bytes slice to int64 array.
			// converting each of 8 bytes of byteSlice to an integer.
			int64Slice := (*(*[]int64)(unsafe.Pointer(&pageBytes)))[:len(pageBytes)/8]

			allBytesZero := true
			// Iterating though each integer of in64 array to check if any of the number is greater than 0 or not.
			// If any no is greater than 0, it means that the 8 bytes slice represented by that integer has atleast one byte greater than 0
			// If all integers are 0, it means that the 8 bytes slice represented by each integer has no byte greater than 0
			for index := 0; index < len(int64Slice); index++ {
				if int64Slice[index] > 0 {
					// If one number is greater than 0, then we need to perform the PutPage update.
					allBytesZero = false
					break
				}
			}

			// If all the bytes in the pageBytes is 0, then we do not need to perform the PutPage
			// Updating number of chunks done.
			if allBytesZero {
				t.Log(common.LogInfo, fmt.Sprintf("has worker %d which is not performing PutPages for Page range from %d to %d since all the bytes are zero", workerId, startPage, startPage+pageSize))
				pageDone(common.TransferComplete)
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(pageBytes), localToPageBlob.pacer)

			_, err := localToPageBlob.pageBlobUrl.UploadPages(t.TransferContext, azblob.PageRange{Start: int64(startPage), End: int64(startPage + pageSize - 1)},
				body, azblob.BlobAccessConditions{})
			if err != nil {
				status := common.TransferInProgress
				if t.TransferContext.Err() != nil {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because transfer was cancelled", workerId, startPage, startPage+pageSize))
				} else {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because of following error %s", workerId, startPage, startPage+pageSize, err.Error()))
					// cancelling the transfer
					t.TransferCancelFunc()
					status = common.TransferFailed
				}
				pageDone(status)
				return
			}
			t.Log(common.LogInfo, fmt.Sprintf("has workedId %d which successfully complete PUT page request from range %d to %d", workerId, startPage, startPage+pageSize))

			//updating the through put counter of the Job
			t.jobInfo.JobThroughPut.updateCurrentBytes(int64(pageSize))
			// this check is to cover the scenario when the last page is successfully updated, but transfer was cancelled.
			if t.TransferContext.Err() != nil {
				pageDone(common.TransferInProgress)
			} else {
				pageDone(common.TransferComplete)
			}
		}
	}
}*/
