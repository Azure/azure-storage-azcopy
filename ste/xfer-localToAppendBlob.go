package ste

import (
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/edsrzf/mmap-go"
	"fmt"
	"bytes"
)

type localToAppendBlob struct {

	transfer         *TransferMsg
	pacer            *pacer
	blobURL          azblob.BlobURL
	memoryMappedFile mmap.MMap
	blockIds         []string
}

// return a new localToBlockBlob struct targeting a specific transfer
func newLocalToAppendBlob(transfer *TransferMsg, pacer *pacer) xfer {
	return &localToAppendBlob{transfer: transfer, pacer: pacer}
}

// this function performs the setup for each transfer and schedules the corresponding chunkMsgs into the chunkChannel
func (localToAppendBlob *localToAppendBlob) runPrologue(chunkChannel chan<- ChunkMsg) {

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
				localToAppendBlob.transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(localToAppendBlob.transfer.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(localToAppendBlob.transfer.Destination)
	appendBlobUrl := azblob.NewAppendBlobURL(*u, p)

	// step 2: map in the file to upload before appending blobs
	localToAppendBlob.memoryMappedFile,_ = executionEngineHelper{}.openAndMemoryMapFile(localToAppendBlob.transfer.Source)


	// step 3: Scheduling append blob in Chunk channel to perform append blob
	chunkChannel <- ChunkMsg{doTransfer:localToAppendBlob.generateUploadFunc(
		localToAppendBlob.transfer,
		appendBlobUrl,
		localToAppendBlob.memoryMappedFile),
	}
}

func (localToAppendBlob localToAppendBlob) generateUploadFunc(t *TransferMsg, appendBlobURL azblob.AppendBlobURL, memoryMappedFile mmap.MMap) (chunkFunc){
	return func(workerId int) {
		if t.TransferContext.Err() != nil{
			t.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up transfer by worked %d", workerId))
			t.TransferDone()
		}else{
			blobHttpHeader, metaData := t.blobHttpHeaderAndMetadata(memoryMappedFile)
			_, err := appendBlobURL.Create(t.TransferContext, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
			if err != nil {
				if t.TransferContext.Err() != nil{
					t.Log(common.LogError, fmt.Sprintf("failed by worker %d while creating blob because transfer was cancelled",workerId))
				}else{
					t.Log(common.LogError, fmt.Sprintf("upload failed while creating blob because of following reason %s by worker %d", err.Error(), workerId))
					t.TransferStatus(common.TransferFailed)
				}
				t.TransferDone()
				err = memoryMappedFile.Unmap()
				if err != nil{
					t.Log(common.LogError, " has error mapping the memory map file")
				}
				return
			}

			// Iterating through source size and append blocks.
			// If the source size greater than 4 MB, then source is split into
			// 4MB blocks each and appended to the blob.
			for startIndex := int64(0); startIndex < int64(t.SourceSize); startIndex += int64(t.BlockSize) {
				adjustedChunkSize := int64(t.BlockSize)

				// compute actual size of the chunk
				if startIndex + int64(t.BlockSize) > int64(t.SourceSize) {
					adjustedChunkSize = int64(t.SourceSize) - startIndex
				}

				// requesting (startIndex + adjustedChunkSize) bytes from pacer to be send to service.
				body := newRequestBodyPacer(bytes.NewReader(memoryMappedFile[startIndex:startIndex + adjustedChunkSize]), localToAppendBlob.pacer)

				_, err := appendBlobURL.AppendBlock(t.TransferContext, body, azblob.BlobAccessConditions{})
				if err != nil {
					// If the append block failed because transfer was cancelled.
					if t.TransferContext.Err() != nil{ // append block failed because transfer was cancelled.
						t.Log(common.LogError, fmt.Sprintf("has worker %d appending block from %d to %d failed because the transfer was cancelled", workerId, startIndex, startIndex + adjustedChunkSize))
					} else{
						// If the append block failed because of some other reason.
						t.Log(common.LogError, fmt.Sprintf("had append block from %d to %d failed because of following reason %s", startIndex, startIndex + adjustedChunkSize, err.Error()))

						// cancelling the transfer because one of the append block failed.
						t.TransferCancelFunc()

						// updating the transfer status
						t.TransferStatus(common.TransferFailed)
					}
					// updating number of the transfers done.
					t.TransferDone()
					err = memoryMappedFile.Unmap()
					if err != nil{
						t.Log(common.LogError, " has error mapping the memory map file")
					}
					return
				}
			}
			t.Log(common.LogInfo, "successfully uploaded ")
			t.TransferDone()
			t.TransferStatus(common.TransferComplete)
			err = memoryMappedFile.Unmap()
			if err != nil{
				t.Log(common.LogError, " has error mapping the memory map file")
			}
		}
	}
}