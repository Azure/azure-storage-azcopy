package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"io"
	"net/url"
	"sync/atomic"
	"time"
)

type blobToLocal struct {
	// count the number of chunks that are done
	count uint32
}

func (blobToLocal blobToLocal) prologue(transfer TransferMsgDetail, chunkChannel chan<- ChunkMsg) {
	// step 1: get blob size
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	})
	u, _ := url.Parse(transfer.Source)
	blobUrl := azblob.NewBlobURL(*u, p)
	blobSize := getBlobSize(blobUrl)

	// step 2: prep local file before download starts
	memoryMappedFile := createAndMemoryMapFile(transfer.Destination, blobSize)

	// step 3: go through the blob range and schedule download chunk jobs/msgs
	downloadChunkSize := int64(transfer.ChunkSize)

	blockIdCount := int32(0)
	for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the download chunk job
		chunkChannel <- ChunkMsg{
			doTransfer: generateDownloadFunc(
				transfer.JobId,
				transfer.PartNumber,
				transfer.TransferId,
				blockIdCount, // serves as index of chunk
				computeNumOfChunks(blobSize, downloadChunkSize),
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile,
				transfer.TransferCtx,
				transfer.TransferCancelFunc,
				&blobToLocal.count, transfer.JobHandlerMap),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the downloading of a single chunk
func generateDownloadFunc(jobId common.JobID, partNum common.PartNumber, transferId uint32, chunkId int32, totalNumOfChunks uint32, chunkSize int64, startIndex int64,
	blobURL azblob.BlobURL, memoryMappedFile mmap.MMap, ctx context.Context, cancelTransfer func(), progressCount *uint32, jobInfoMap *JobsInfoMap) chunkFunc {
	return func(workerId int) {
		logger := getLoggerForJobId(jobId, jobInfoMap)
		transferIdentifierStr := fmt.Sprintf("jobId %s and partNum %d and transferId %d", jobId, partNum, transferId)

		//fmt.Println("Worker", workerId, "is processing download CHUNK job with", transferIdentifierStr)

		// step 1: perform get
		get, err := blobURL.GetBlob(ctx, azblob.BlobRange{Offset: startIndex, Count: chunkSize}, azblob.BlobAccessConditions{}, false)
		if err != nil {
			// cancel entire transfer because this chunk has failed
			cancelTransfer()
			logger.Debug("worker %d is canceling Chunk job with %s and chunkId %d because startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex)
			updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobInfoMap)
			updateTransferStatus(jobId, partNum, transferId, common.TransferStatusFailed, jobInfoMap)
			return
		}

		// step2: write the body into the memory mapped file directly
		bytesRead, err := io.ReadFull(get.Body(), memoryMappedFile[startIndex:startIndex+chunkSize])
		get.Body().Close()
		if int64(bytesRead) != chunkSize || err != nil {
			// cancel entire transfer because this chunk has failed
			cancelTransfer()
			logger.Debug("worker %d is canceling Chunk job with %s and chunkId %d because writing to file for startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex)
			//fmt.Println("Worker", workerId, "is canceling CHUNK job with", transferIdentifierStr, "and chunkID", chunkId, "because writing to file for startIndex of", startIndex, "has failed")
			updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobInfoMap)
			updateTransferStatus(jobId, partNum, transferId, common.TransferStatusFailed, jobInfoMap)
			return
		}

		updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusComplete, jobInfoMap)
		updateThroughputCounter(chunkSize)

		// step 3: check if this is the last chunk
		if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
			// step 4: this is the last block, perform EPILOGUE
			logger.Debug("worker %d is concluding download Transfer job with %s after processing chunkId %d", workerId, transferIdentifierStr, chunkId)
			//fmt.Println("Worker", workerId, "is concluding download TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)

			updateTransferStatus(jobId, partNum, transferId, common.TransferStatusComplete, jobInfoMap)

			err := memoryMappedFile.Unmap()
			if err != nil {
				logger.Error("worker %v failed to conclude Transfer job with %v after processing chunkId %v", workerId, transferIdentifierStr, chunkId)
				//fmt.Println("Worker", workerId, "failed to conclude TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId)
			}
		}
	}
}
