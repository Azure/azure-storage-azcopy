// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/edsrzf/mmap-go"
	"os"
	"time"
)

// TODO move execution engine as internal package
type executionEngine struct {
	executionEngineChannels *EEChannels
	pacer                   *pacer
	numOfEngineWorker       int
}

func newExecutionEngine(executionEngineChannels *EEChannels, numOfEngineWorker int, targetRateInMBps int) *executionEngine {
	return &executionEngine{
		executionEngineChannels: executionEngineChannels,
		pacer:             newPacer(int64(targetRateInMBps) * 1024 * 1024),
		numOfEngineWorker: numOfEngineWorker,
	}
}

func (executionEngine *executionEngine) start() {
	// spin up the desired number of executionEngine workers to process transfers
	for i := 1; i <= executionEngine.numOfEngineWorker; i++ {
		go executionEngine.engineWorker(i, executionEngine.executionEngineChannels)
	}
}

// general purpose worker that reads in transfer jobs, schedules chunk jobs, and executes chunk jobs
func (executionEngine *executionEngine) engineWorker(workerId int, executionEngineChannels *EEChannels) {
	for {
		// priority 0: whether to commit suicide, this is used to scale back
		select {
		case <-executionEngineChannels.SuicideChannel:
			return
		default:
			// priority 1: high priority chunk channel, do actual upload/download
			select {
			case chunkJobItem := <-executionEngineChannels.HighChunk:
				chunkJobItem.doTransfer(workerId)
			default:
				// priority 2: high priority transfer channel, schedule chunkMsgs
				select {
				case transferMsg := <-executionEngineChannels.HighTransfer:
					// if the transfer Msg has been cancelled
					if transferMsg.TransferContext.Err() != nil {
						transferMsg.Log(common.LogInfo, fmt.Sprintf(" is not picked up worked %d because transfer was cancelled", workerId))
						transferMsg.TransferDone()
					} else {
						// TODO fix preceding space
						transferMsg.Log(common.LogInfo,
							fmt.Sprintf("has worker %d which is processing TRANSFER", workerId))

						// the xferFactory is generated based on the type of transfer (source and destination pair)
						xferFactory := executionEngine.computeTransferFactory(transferMsg.SourceType, transferMsg.DestinationType)
						if xferFactory == nil {
							// TODO can these two calls be combined?
							transferMsg.Log(common.LogError,
								fmt.Sprintf(" has unrecognizable type of transfer with sourceLocationType as %d and destinationLocationType as %d",
									transferMsg.SourceType, transferMsg.DestinationType))
							panic(fmt.Errorf("Unrecognizable type of transfer with sourceLocationType as %d and destinationLocationType as %d",
								transferMsg.SourceType, transferMsg.DestinationType))
						}
						xfer := xferFactory(&transferMsg, executionEngine.pacer)
						xfer.runPrologue(executionEngineChannels.HighChunk)
					}
				default:
					// lower priorities should go here in the future
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	}
}

// the xfer factory is generated based on the type of source and destination
func (*executionEngine) computeTransferFactory(sourceLocationType, destinationLocationType common.LocationType) xferFactory {
	switch {
	case sourceLocationType == common.Blob && destinationLocationType == common.Local: // download from Azure to local
		return newBlobToLocal
	case sourceLocationType == common.Local && destinationLocationType == common.Blob: // upload from local to Azure
		return newLocalToBlockBlob
	default:
		return nil
	}
}

// TODO give these to the plugin packages
type executionEngineHelper struct{}

// opens file with desired flags and return *os.File
func (executionEngineHelper executionEngineHelper) openFile(filePath string, flags int) *os.File {
	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		panic(fmt.Sprintf("Error opening file: %s", err))
	}
	return f
}

// maps a *os.File into memory and return a byte slice (mmap.MMap)
func (executionEngineHelper executionEngineHelper) mapFile(file *os.File) mmap.MMap {
	memoryMappedFile, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(fmt.Sprintf("Error mapping: %s", err))
	}
	return memoryMappedFile
}

// create and memory map a file, given its path and length
func (executionEngineHelper executionEngineHelper) createAndMemoryMapFile(destinationPath string, fileSize int64) mmap.MMap {
	f := executionEngineHelper.openFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		panic(truncateError)
	}

	return executionEngineHelper.mapFile(f)
}

// open and memory map a file, given its path
func (executionEngineHelper executionEngineHelper) openAndMemoryMapFile(destinationPath string) mmap.MMap {
	f := executionEngineHelper.openFile(destinationPath, os.O_RDWR)
	return executionEngineHelper.mapFile(f)
}
