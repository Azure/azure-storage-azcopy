package ste

import (
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"bytes"
	"os"
)

type localToPageBlob struct {}

// this function performs the setup for each transfer and schedules the corresponding chunkMsgs into the chunkChannel
func (localToPageBlob localToPageBlob) prologue(transfer TransferMsg, chunkChannel chan<- ChunkMsg) {

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
				transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(transfer.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(transfer.Destination)
	pageBlobURL := azblob.NewPageBlobURL(*u, p)



	// step 2: map in the file to upload before appending blobs
	memoryMappedFile, file := executionEngineHelper{}.openAndMemoryMapFile(transfer.Source)
	//blobHttpHeaders, metaData := transfer.blobHttpHeaderAndMetadata(memoryMappedFile)

	fmt.Println("transfer source size ", transfer.SourceSize)
	_, err := pageBlobURL.Create(transfer.TransferContext, int64(transfer.SourceSize),
												0, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		transfer.Log(common.LogError, fmt.Sprintf("failed since PageCreate failed due to %s", err.Error()))
		transfer.TransferCancelFunc()
		transfer.TransferDone()
		transfer.TransferStatus(common.TransferFailed)
		err := memoryMappedFile.Unmap()
		if err != nil{
			transfer.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
		}
		err = file.Close()
		if err != nil{
			transfer.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
		}
		return
	}

	blobSize := int64(transfer.SourceSize)

	pageSize := int64(transfer.BlockSize)

	// step 3: Scheduling page blob in Chunk channel to perform Put pages
	for startIndex := int64(0); startIndex < blobSize; startIndex += pageSize {
		adjustedPageSize := pageSize

		// compute actual size of the chunk
		if startIndex + pageSize > blobSize {
			adjustedPageSize = blobSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: localToPageBlob.generateUploadFunc(
				transfer,
				uint32(transfer.NumChunks),
				pageBlobURL,
				startIndex,
				adjustedPageSize,
				memoryMappedFile,
				file),
		}
	}
}

func (localToPageBlob localToPageBlob) generateUploadFunc(t TransferMsg, numberOfPages uint32, pageBlobUrl azblob.PageBlobURL, startPage int64,
	pageSize int64, memoryMap mmap.MMap, file *os.File) (chunkFunc){
	return func(workerId int) {
		if t.TransferContext.Err() != nil {
			t.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up page %d", startPage))
			if t.ChunksDone() == numberOfPages {
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is finalizing cancellation of transfer", workerId))
				t.TransferDone()
				err := memoryMap.Unmap()
				if err != nil{
					t.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
				}
				err = file.Close()
				if err != nil{
					t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
				}
			}
		}else{
			body := newRequestBodyPacer(bytes.NewReader(memoryMap[startPage:startPage + pageSize]), pc)

			_, err := pageBlobUrl.PutPages(t.TransferContext, azblob.PageRange{Start: int32(startPage), End: int32(startPage + pageSize-1)},
				body, azblob.BlobAccessConditions{})
			if err != nil {
				if t.TransferContext.Err() != nil{
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because transfer was cancelled", workerId, startPage, startPage + pageSize))
				}else{
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to Put Page range from %d to %d because of following error %s", workerId, startPage, startPage + pageSize, err.Error()))
					t.TransferStatus(common.TransferFailed)
				}
				if t.ChunksDone() == numberOfPages{
					t.TransferDone()
					err := memoryMap.Unmap()
					if err != nil{
						t.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
					}
					err = file.Close()
					if err != nil{
						t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
					}
					return
				}
			}
			t.Log(common.LogInfo, fmt.Sprintf("has workedId %d which successfully complete PUT page request from range %d to %d", workerId, startPage, startPage + pageSize))
			if t.ChunksDone() == numberOfPages{
				t.TransferDone()
				t.TransferStatus(common.TransferComplete)
				t.Log(common.LogInfo, "successfully completed")
				err := memoryMap.Unmap()
				if err != nil{
					t.Log(common.LogError, fmt.Sprintf("got an error while unmapping the memory mapped file % because of %s", file.Name(), err.Error()))
				}
				err = file.Close()
				if err != nil{
					t.Log(common.LogError, fmt.Sprintf("got an error while closing file % because of %s", file.Name(), err.Error()))
				}
			}
		}
	}
}