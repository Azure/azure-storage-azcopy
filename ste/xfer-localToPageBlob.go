package ste

import (
	"bytes"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"net/url"
	"os"
)

type localToPageBlob struct {
	transfer         *TransferMsg
	pacer            *pacer
	pageBlobUrl      azblob.PageBlobURL
	memoryMappedFile mmap.MMap
}

// return a new localToPageBlob struct targeting a specific transfer
func newLocalToPageBlob(transfer *TransferMsg, pacer *pacer) xfer {
	return &localToPageBlob{transfer: transfer, pacer: pacer}
}

// this function performs the setup for each transfer and schedules the corresponding page requests into the chunkChannel
func (localToPageBlob *localToPageBlob) runPrologue(chunkChannel chan<- ChunkMsg) {

	var file *os.File
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
	localToPageBlob.memoryMappedFile, file = executionEngineHelper{}.openAndMemoryMapFile(localToPageBlob.transfer.Source)
	//blobHttpHeaders, metaData := transfer.blobHttpHeaderAndMetadata(memoryMappedFile)

	// step 3: Create Page Blob of the source size
	_, err := localToPageBlob.pageBlobUrl.Create(localToPageBlob.transfer.TransferContext, int64(localToPageBlob.transfer.SourceSize),
		0, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		localToPageBlob.transfer.Log(common.LogError, fmt.Sprintf("failed since PageCreate failed due to %s", err.Error()))
		localToPageBlob.transfer.TransferCancelFunc()
		localToPageBlob.transfer.TransferDone()
		localToPageBlob.transfer.TransferStatus(common.TransferFailed)
		err := localToPageBlob.memoryMappedFile.Unmap()
		if err != nil {
			localToPageBlob.transfer.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
		}
		err = file.Close()
		if err != nil {
			localToPageBlob.transfer.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
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
				adjustedPageSize,
				file),
		}
	}
}

func (localToPageBlob *localToPageBlob) generateUploadFunc(numberOfPages uint32, startPage int64, pageSize int64, file *os.File) chunkFunc {
	return func(workerId int) {
		t := localToPageBlob.transfer
		if t.TransferContext.Err() != nil {
			t.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up page %d", startPage))
			if t.ChunksDone() == numberOfPages {
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is finalizing cancellation of transfer", workerId))
				t.TransferDone()
				err := localToPageBlob.memoryMappedFile.Unmap()
				if err != nil {
					t.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
				}
				err = file.Close()
				if err != nil {
					t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
				}
			}
		} else {
			body := newRequestBodyPacer(bytes.NewReader(localToPageBlob.memoryMappedFile[startPage:startPage+pageSize]), localToPageBlob.pacer)

			_, err := localToPageBlob.pageBlobUrl.PutPages(t.TransferContext, azblob.PageRange{Start: int32(startPage), End: int32(startPage + pageSize - 1)},
				body, azblob.BlobAccessConditions{})
			if err != nil {
				if t.TransferContext.Err() != nil {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because transfer was cancelled", workerId, startPage, startPage+pageSize))
				} else {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because of following error %s", workerId, startPage, startPage+pageSize, err.Error()))
					t.TransferStatus(common.TransferFailed)
				}
				if t.ChunksDone() == numberOfPages {
					t.TransferDone()
					err := localToPageBlob.memoryMappedFile.Unmap()
					if err != nil {
						t.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
					}
					err = file.Close()
					if err != nil {
						t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
					}
					return
				}
			}
			t.Log(common.LogInfo, fmt.Sprintf("has workedId %d which successfully complete PUT page request from range %d to %d", workerId, startPage, startPage+pageSize))

			//updating the through put counter of the Job
			t.jobInfo.JobThroughPut.updateCurrentBytes(int64(t.SourceSize))
			if t.ChunksDone() == numberOfPages {
				t.TransferDone()
				t.TransferStatus(common.TransferComplete)
				t.Log(common.LogInfo, "successfully completed")
				err := localToPageBlob.memoryMappedFile.Unmap()
				if err != nil {
					t.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
				}
				err = file.Close()
				if err != nil {
					t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
				}
			}
		}
	}
}
