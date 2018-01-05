package main

import (
	"fmt"
	"time"
	"sync/atomic"
	"github.com/Azure/azure-storage-azcopy/common"
	"path/filepath"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"github.com/edsrzf/mmap-go"
	"io"
	"context"
	"os"
	"runtime"
	"crypto/rand"
	"encoding/base64"
	"bytes"
)

type transferJob common.TransferJob
type chunkJob struct {
	transferId int32
	doTransfer func(int)
}

type suicideJob byte

func main() {
	fmt.Println("ENGINE STARTING!")
	runtime.GOMAXPROCS(4) //scale?

	highChunk := make(chan chunkJob, 500)
	highTransfer := make(chan transferJob, 500)
	suicideLine := make(chan suicideJob, 100)

	for i := 1; i <= 5; i++ {
		go engineWorker(i, highChunk, highTransfer, suicideLine)
	}

	//highTransfer <- transferJob{
	//	id: 1,
	//	chunkSize: 4 * 1024 * 1024,
	//	count: 0,
	//
	//	Source: "https://azcopynextgendev2.blob.core.windows.net/testcontainer/Testy_PPT1.pptx?st=2017-12-07T00%3A27%3A00Z&se=2018-12-08T00%3A27%3A00Z&sp=rwdl&sv=2016-05-31&sr=c&sig=D9xT4VAKVAHQYosYzKDY%2FaMhBrTIvlcxLORsPst6%2BuM%3D",
	//	SourceType: common.Blob,
	//
	//	Destination: filepath.Join("/Users/Zed/Documents/test-download", "result1.pptx"),
	//	DestinationType: common.Local,
	//}
	//
	//highTransfer <- transferJob{
	//	id: 2,
	//	chunkSize: 4 * 1024 * 1024,
	//	count: 0,
	//
	//	Source: "https://azcopynextgendev2.blob.core.windows.net/testcontainer/Testy_PPT1.pptx?st=2017-12-07T00%3A27%3A00Z&se=2018-12-08T00%3A27%3A00Z&sp=rwdl&sv=2016-05-31&sr=c&sig=D9xT4VAKVAHQYosYzKDY%2FaMhBrTIvlcxLORsPst6%2BuM%3D",
	//	SourceType: common.Blob,
	//
	//	Destination: filepath.Join("/Users/Zed/Documents/test-download", "result2.pptx"),
	//	DestinationType: common.Local,
	//}

	highTransfer <- transferJob{
		Id: 1,
		ChunkSize: 4 * 1024 * 1024,
		Count: 0,

		Destination: "http://azcopynextgendev2.blob.core.windows.net/testcontainer/aaa4.pdf?st=2017-12-07T00%3A27%3A00Z&se=2018-12-08T00%3A27%3A00Z&sp=rwdl&sv=2016-05-31&sr=c&sig=D9xT4VAKVAHQYosYzKDY%2FaMhBrTIvlcxLORsPst6%2BuM%3D",
		DestinationType: common.Blob,

		Source: filepath.Join("/Users/Zed/Documents/test-upload", "test.pdf"),
		SourceType: common.Local,
	}


	//// wait a bit and kill one worker
	//time.Sleep(10 * time.Second)
	//suicideLine <- 0
	//
	//// wait a bit and add one worker
	//time.Sleep(10 * time.Second)
	//fmt.Println("NEW WORKER IN TOWN!")
	//go engineWorker(3, highChunk, highTransfer, suicideLine)

	// let the execution engine run
	time.Sleep(1000 * time.Second)
}

// general purpose worker that reads in transfer jobs, schedules chunk jobs, and executes chunk jobs
func engineWorker(workerId int, highPriorityChunkQueue chan chunkJob, highPriorityTransferQueue chan transferJob, suicideLine chan suicideJob) {
	for {
		// priority 0: whether to commit suicide
		select {
		case <-suicideLine:
			fmt.Println("Worker", workerId, "is committing SUICIDE.")
			return
		default:
			// priority 1: high priority chunk queue, do actual upload/download
			select {
			case chunkJobItem := <-highPriorityChunkQueue:
				chunkJobItem.doTransfer(workerId)

			default:
				// priority 2: high priority transfer queue, schedule chunk jobs
				select {
				case transferJobItem := <-highPriorityTransferQueue:
					scheduleChunkJobs(workerId, transferJobItem, highPriorityChunkQueue)
				default:
					// lower priorities should go here in the future
					//fmt.Println("Worker", workerId, "is IDLE, sleeping for 0.01 sec zzzzzz")
					time.Sleep(10 * time.Millisecond)
					//fmt.Println("Worker", workerId,)
				}
			}
		}
	}
}

// determine the type of transfer and perform the PROLOGUE to set it up
func scheduleChunkJobs(workerId int, transferJobItem transferJob, chunkJobQueue chan chunkJob) {
	fmt.Println("Worker", workerId, "is processing TRANSFER job with id", transferJobItem.id)

	switch {
	case transferJobItem.SourceType == common.Blob && transferJobItem.DestinationType == common.Local: // download from Azure to local
		blobUrl, blobSize, memoryMappedFile := performDownloadPrologue(transferJobItem.Source, transferJobItem.Destination)
		downloadChunkSize := int64(transferJobItem.chunkSize)
		blockIdCount := int32(0)
		ctx, cancelTransfer := context.WithCancel(context.Background())

		for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
			adjustedChunkSize :=  downloadChunkSize

			// compute range
			if startIndex + downloadChunkSize > blobSize {
				adjustedChunkSize = blobSize - startIndex
			}

			// schedule the chunk job
			chunkJobQueue <- chunkJob{
				transferId: transferJobItem.id,
				doTransfer: generateDownloadFunc(
					transferJobItem.id,
					blockIdCount,
					computeNumOfChunks(blobSize, downloadChunkSize),
					adjustedChunkSize,
					startIndex,
					blobUrl,
					memoryMappedFile[startIndex: startIndex + adjustedChunkSize],
					ctx,
					cancelTransfer,
					&transferJobItem.count),
			}
			blockIdCount += 1
		}
	case transferJobItem.SourceType == common.Local && transferJobItem.DestinationType == common.Blob: // upload from local to Azure
		blobUrl, blobSize, memoryMappedFile := performUploadPrologue(transferJobItem.Destination, transferJobItem.Source)
		downloadChunkSize := int64(transferJobItem.chunkSize)
		blockIdCount := int32(0)
		ctx, cancelTransfer := context.WithCancel(context.Background())
		numOfBlocks := computeNumOfChunks(blobSize, downloadChunkSize)
		blocksIds := make([]string, numOfBlocks)

		for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
			adjustedChunkSize :=  downloadChunkSize

			// compute range
			if startIndex + downloadChunkSize > blobSize {
				adjustedChunkSize = blobSize - startIndex
			}

			// schedule the chunk job
			chunkJobQueue <- chunkJob{
				transferId: transferJobItem.id,
				doTransfer: generateUploadFunc(
					transferJobItem.id,
					blockIdCount,
					numOfBlocks,
					adjustedChunkSize,
					startIndex,
					blobUrl,
					memoryMappedFile[startIndex: startIndex + adjustedChunkSize],
					ctx,
					cancelTransfer,
					&transferJobItem.count,
					&blocksIds),
			}
			blockIdCount += 1
		}
	default:
		fmt.Println("Worker", workerId, "is rejecting TRANSFER job with id", transferJobItem.id)
		return
	}

	fmt.Println("Worker", workerId, "is DONE processing TRANSFER job with id", transferJobItem.id)
}

func performUploadPrologue(blobUrlString string, sourcePath string) (blobUrl azblob.BlobURL, blobSize int64, memoryMappedFile mmap.MMap) {
	// create pipeline for the destination blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	})
	u, _ := url.Parse(blobUrlString)
	blobUrl = azblob.NewBlobURL(*u, p)
	
	// get the file size
	fi, _ := os.Stat(sourcePath)
	blobSize = fi.Size()

	// map in the file to upload before transferring chunks
	memoryMappedFile = openAndMemoryMapFile(sourcePath, blobSize)
	return
}

func performDownloadPrologue(blobUrlString string, destinationPath string) (blobUrl azblob.BlobURL, blobSize int64, memoryMappedFile mmap.MMap) {
	// get blob size
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	})
	u, _ := url.Parse(blobUrlString)
	blobUrl = azblob.NewBlobURL(*u, p)
	blobSize = getBlobSize(blobUrl)

	// prep local file before download starts
	memoryMappedFile = createAndMemoryMapFile(destinationPath, blobSize)
	return
}

func generateUploadFunc(transferId int32, chunkId int32, totalNumOfChunks uint32, chunkSize int64,
	startIndex int64, blobURL azblob.BlobURL, memoryMappedFile mmap.MMap, ctx context.Context, cancelTransfer func(), progressCount *uint32, blockIds *[]string, ) func(int) {
	return func(workerId int) {
		// step 1: generate block ID
		blockId, _ := newUUID()
		encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))
		(*blockIds)[chunkId] = encodedBlockId
		fmt.Println("Worker", workerId, "is processing CHUNK job with transferId", transferId, "and chunkID", chunkId, "and blockID", encodedBlockId)

		// step 2: perform put block
		blockBlobUrl := blobURL.ToBlockBlobURL()
		chunkReader := bytes.NewReader(memoryMappedFile)

		_, err := blockBlobUrl.PutBlock(ctx, encodedBlockId, chunkReader, azblob.LeaseAccessConditions{})
		if err != nil {
			// cancel entire transfer because this chunk has failed
			cancelTransfer()
			fmt.Println("Worker", workerId, "is canceling CHUNK job with transferId", transferId, "and chunkID", chunkId, "because startIndex of", startIndex, "has failed due to err", err)
			return
		}

		// step 3 check if this is the last chunk
		if atomic.AddUint32(progressCount, 1) == totalNumOfChunks { // this is the last block, perform EPILOGUE

			fmt.Println("Worker", workerId, "is concluding TRANSFER job with transferId", transferId, "after processing chunkId", chunkId, "with blocklist", *blockIds)

			_, err = blockBlobUrl.PutBlockList(ctx, *blockIds, azblob.Metadata{}, azblob.BlobHTTPHeaders{}, azblob.BlobAccessConditions{})
			if err != nil {
				fmt.Println("Worker", workerId, "failed to conclude TRANSFER job with transferId", transferId, "after processing chunkId", chunkId, "due to err", err)
			}

			err := memoryMappedFile.Unmap()
			if err != nil {
				fmt.Println("Worker", workerId, "failed to conclude TRANSFER job with transferId", transferId, "after processing chunkId", chunkId, "due to err", err)
			}

		}
	}
}

func generateDownloadFunc(transferId int32, chunkId int32, totalNumOfChunks uint32, chunkSize int64,
	startIndex int64, blobURL azblob.BlobURL, memoryMappedFile mmap.MMap, ctx context.Context, cancelTransfer func(), progressCount *uint32, ) func(int) {
	return func(workerId int) {
		fmt.Println("Worker", workerId, "is processing CHUNK job with transferId", transferId, "and chunkID", chunkId)

		// step 1: perform get
		get, err := blobURL.GetBlob(ctx, azblob.BlobRange{Offset: startIndex, Count: chunkSize}, azblob.BlobAccessConditions{}, false)
		if err != nil {
			// cancel entire transfer because this chunk has failed
			cancelTransfer()
			fmt.Println("Worker", workerId, "is canceling CHUNK job with transferId", transferId, "and chunkID", chunkId, "because startIndex of", startIndex, "has failed")
			return
		}

		// step2: write the body into memory mapped file directly
		bytesRead, err := io.ReadFull(get.Body(), memoryMappedFile)
		get.Body().Close()


		if int64(bytesRead) != chunkSize || err != nil {
			// cancel entire transfer because this chunk has failed
			cancelTransfer()
			fmt.Println("Worker", workerId, "is canceling CHUNK job with transferId", transferId, "and chunkID", chunkId, "because writing to file for startIndex of", startIndex, "has failed")
			return
		}

		// step 3 check if this is the last chunk
		if atomic.AddUint32(progressCount, 1) == totalNumOfChunks { // this is the last block, perform EPILOGUE
			err := memoryMappedFile.Unmap()
			if err != nil {
				fmt.Println("Worker", workerId, "is failed to conclude TRANSFER job with transferId", transferId, "after processing chunkId", chunkId)
			}
			fmt.Println("Worker", workerId, "is concluding TRANSFER job with transferId", transferId, "after processing chunkId", chunkId)
		}
	}
}

// for a given total size, compute how many chunks there are
func computeNumOfChunks(totalSize int64, chunkSize int64) uint32 {
	if totalSize % chunkSize == 0 {
		return uint32(totalSize / chunkSize)
	} else {
		return uint32(totalSize / chunkSize + 1)
	}
}

// opens file with desired flags and return File
func openFile(destinationPath string, flags int) *os.File {
	f, err := os.OpenFile(destinationPath, flags, 0644)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func mapFile(f *os.File) mmap.MMap{
	memoryMappedFile, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		panic(fmt.Sprintf("Error mapping: %s", err))
	}
	return memoryMappedFile
}

// create and memory map a file, given its path and length
func createAndMemoryMapFile(destinationPath string, fileSize int64) mmap.MMap {
	f := openFile(destinationPath, os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		panic(truncateError)
	}

	return mapFile(f)
}

// open and memory map a file, given its path
func openAndMemoryMapFile(destinationPath string, fileSize int64) mmap.MMap {
	f := openFile(destinationPath, os.O_RDWR)
	return mapFile(f)
}

// make a HEAD request to get the blob size
func getBlobSize(blobUrl azblob.BlobURL) int64{
	blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
	if err != nil {
		panic("Cannot get blob size")
	}
	return blobProperties.ContentLength()
}

// newUUID generates a random UUID according to RFC 4122
func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}
