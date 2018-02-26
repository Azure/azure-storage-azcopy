package ste

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"os"
	"time"
)

func InitializeExecutionEngine(execEngineChannels *EEChannels, numOfEngineWorker int) {
	highChunk := execEngineChannels.HighChunkTransaction
	highTransfer := execEngineChannels.HighTransfer
	// TODO add the other channels
	suicideLine := execEngineChannels.SuicideChannel

	for i := 1; i <= numOfEngineWorker; i++ {
		// TODO take struct instead
		go engineWorker(i, highChunk, highTransfer, suicideLine)
	}
}

// general purpose worker that reads in transfer jobs, schedules chunk jobs, and executes chunk jobs
func engineWorker(workerId int, highPriorityChunkChannel chan ChunkMsg, highPriorityTransferChannel <-chan TransferMsg, suicideLine <-chan SuicideJob) {
	for {
		// priority 0: whether to commit suicide, this is used to scale back
		select {
		case <-suicideLine:
			fmt.Println("Worker", workerId, "is committing SUICIDE.")
			return
		default:
			// priority 1: high priority chunk channel, do actual upload/download
			select {
			case chunkJobItem := <-highPriorityChunkChannel:
				chunkJobItem.doTransfer(workerId)
			default:
				// priority 2: high priority transfer channel, schedule chunkMsgs
				select {
				case transferMsg := <-highPriorityTransferChannel:
					// If the transfer Msg has been cancelled,
					if transferMsg.TransferContext.Err() != nil {
						transferMsg.Log(common.LogInfo, fmt.Sprintf(" is not picked up worked %d", workerId))
						//TransferStatus(transferMsg.jobId, transferMsg.partNumber, transferMsg.transferIndex, common.TransferStatusFailed, transferMsg.infoMap)
						transferMsg.TransferDone()
					} else {
						transferMsg.Log(common.LogInfo,
							fmt.Sprintf("has worker %d which is processing TRANSFER", workerId))
						prologueFunction := computePrologueFunc(transferMsg.SourceType, transferMsg.DestinationType)
						if prologueFunction == nil {

							transferMsg.Log(common.LogError,
								fmt.Sprintf(" has unrecognizable type of transfer with sourceLocationType as %d and destinationLocationType as %d",
									transferMsg.SourceType, transferMsg.DestinationType))
							panic(errors.New(fmt.Sprintf("Unrecognizable type of transfer with sourceLocationType as %d and destinationLocationType as %d",
								transferMsg.SourceType, transferMsg.DestinationType)))
						}
						prologueFunction(transferMsg, highPriorityChunkChannel)
					}
				default:
					// lower priorities should go here in the future
					//fmt.Println("Worker", workerId, "is IDLE, sleeping for 0.01 sec zzzzzz")
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	}
}

// the prologue function is generated based on the type of source and destination
func computePrologueFunc(sourceLocationType, destinationLocationType common.LocationType) prologueFunc {
	switch {
	case sourceLocationType == common.Blob && destinationLocationType == common.Local: // download from Azure to local
		return blobToLocal{}.prologue
	case sourceLocationType == common.Local && destinationLocationType == common.Blob: // upload from local to Azure
		return localToBlockBlob{}.prologue
	default:
		return nil
	}
}

// for a given total size, compute how many chunks there are
func computeNumOfChunks(totalSize int64, chunkSize int64) uint32 {
	if totalSize%chunkSize == 0 {
		return uint32(totalSize / chunkSize)
	} else {
		return uint32(totalSize/chunkSize + 1)
	}
}

// opens file with desired flags and return *os.File
func openFile(filePath string, flags int) *os.File {
	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		panic(fmt.Sprintf("Error opening file: %s", err))
	}
	return f
}

// maps a *os.File into memory and return a byte slice (mmap.MMap)
func mapFile(file *os.File) mmap.MMap {
	memoryMappedFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(fmt.Sprintf("Error mapping: %s", err))
	}
	return memoryMappedFile
}

// create and memory map a file, given its path and length
func createAndMemoryMapFile(destinationPath string, fileSize int64) mmap.MMap {
	f := openFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		panic(truncateError)
	}

	return mapFile(f)
}

// open and memory map a file, given its path
func openAndMemoryMapFile(destinationPath string) mmap.MMap {
	f := openFile(destinationPath, os.O_RDWR)
	return mapFile(f)
}

// make a HEAD request to get the blob size
func getBlobSize(blobUrl azblob.BlobURL) int64 {
	blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
	if err != nil {
		panic("Cannot get blob size")
	}
	return blobProperties.ContentLength()
}
