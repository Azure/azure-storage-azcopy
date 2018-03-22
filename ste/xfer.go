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
	"github.com/Azure/azure-storage-azcopy/common"
	"time"
)

// upload related
const UploadMaxTries = 5
const UploadTryTimeout = time.Minute * 10
const UploadRetryDelay = time.Second * 1
const UploadMaxRetryDelay = time.Second * 3

// download related
const DownloadMaxTries = 5
const DownloadTryTimeout = time.Minute * 10
const DownloadRetryDelay = time.Second * 1
const DownloadMaxRetryDelay = time.Second * 3

// pacer related
const PacerTimeToWaitInMs = 50

type ChunkMsg struct {
	doTransfer chunkFunc
}

type chunkFunc func(int)

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// These types are define the STE Coordinator
type NewJobXfer func( /*Job-specific parameters*/ logger common.ILogger) IJobXfer

type IJobXfer interface {
	NewJobPartXfer( /*Job Part-specific parameters*/ chunkCh chan struct{}) IJobPartXfer
	Cleanup()
}

type IJobPartXfer interface {
	StartTransfer( /*Job Part Transfer-specific parameters*/ t IJobPartTransferMgr)
	Cleanup()
}

// the xfer factory is generated based on the type of source and destination
func computeTransferFactory(srcLocation, dstLocation common.Location, blobType common.BlobType) /*NewJobXfer */ xferFactory {
	switch {
	case srcLocation == (common.Location{}).Blob() && dstLocation == (common.Location{}).Local(): // download from Azure Blob to local file system
		return newBlobToLocal
	case srcLocation == (common.Location{}).Local() && dstLocation == (common.Location{}).Blob(): // upload from local file system to Azure blob
		switch blobType {
		case (common.BlobType{}).Block():
			return newLocalToBlockBlob
		case (common.BlobType{}).Append():
			return newLocalToAppendBlob
		case (common.BlobType{}).Page():
			return newLocalToPageBlob
		default:
			panic("invalid blob type")
		}
	case srcLocation == (common.Location{}).File() && dstLocation == (common.Location{}).Local(): // download from Azure File to local file system
		return nil // TODO
	case srcLocation == (common.Location{}).Local() && dstLocation == (common.Location{}).File(): // upload from local file system to Azure File
		return nil // TODO
	default:
		return nil // unknown src/dst location transfer
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
func JobLoad() {
	logger := Logger{}
	jobXfer := Compute()
	if jobXfer == nil { // invalid src/dst location combination
}

jx := jobXfer(logger)   // Called once per Job BEFORE any of the Job's Transfers are queued to the channel
jpx := jx.NewJobPartXfer(nil)   // Called once per Job Part BEFORE any of the Job Part's Transfers are queued to the channel
// Store the jpx in side the TransferMsg before puttin it in the channel

// When the EE pull the TM from the channel, call
tm.StartTransfer()  // Internally, this will call jpx.StartTransfer and pass it the TM
}

// NewLocalToBlockBlobJobInfo creates an object that manages all transfers related
// to all of a Job's Job Parts. This method is called by STE's Coordinator before
// it queues any Job Part transfers.
func NewLocalToBlockBlobJobXfer(logger common.ILogger) IJobXfer {
	return &localToBlockBlobJobXfer{pipeline: NewPipeline(logger)}
}

// localToBlockBlobJobInfo is a private struct implementing the XferJobInfo interface.
// An instance of this struct stores any Job-specific state needed across all this job's
// job part's transfers (Ex: The Pipeline used for all of this Job's transfers)
type localToBlockBlobJobXfer struct {
	pipeline Pipeline
}

// NewJobPartXfer creates an object that manages all transfers related
// to a Job's single Job Parts. This method is called by STE's Coordinator before
// it queues any of the Job Part's transfers.
func (ji *localToBlockBlobJobXfer) NewJobPartXfer(chunkCh chan struct{}) IJobPartXfer {
	return &localToBlockBlobJobPartXfer{ji: ji, chunkCh: chunkCh}
}

func (ji *localToBlockBlobJobXfer) Cleanup() {
	// Do any Job cleanup here...
}

// localToBlockBlobJobPartInfo is a private struct implementing the XferJobPartInfo interface.
// An instance of this struct stores any Job Part-specific state needed across all this job
// part's transfers (Ex: the channel used for queuing all transfer chunks)
type localToBlockBlobJobPartXfer struct {
	jx      *localToBlockBlobJobXfer
	chunkCh chan struct{}
}

// StartTransfer is called when a transfer is pulled from a transfer channel
func (jpx *localToBlockBlobJobPartXfer) StartTransfer(t TransferMsg) {
	// This is the transfer Prologue method
	tx := &localToBlockBlobJobPartTransferXfer{jpx: jpi, t: t}
	_ = tx

	// Here's how to get the 1 pipeline object to be used by ALL transfers of this Job
	pl := jpx.jx.pipeline
	_ = pl

	// Here's hw to get the 1 channel object to be used by ALL transfers of this Job Part
	ch := jpx.chunkCh
	_ = ch
}

func (jpx *localToBlockBlobJobPartXfer) Cleanup() {
	// Do any Job Part cleanup here...
}

// Store any Job Part Transfer-specific state need for this transfer (Ex: log file for this Job)
// localToBlockBlobJobPartTransferInfo is a private struct. An instance of this struct
// stores any Job Part Transfer-specific state needed across all of this transfer's operations
// (Ex: the source & destination paths)
type localToBlockBlobJobPartTransferXfer struct {
	jpx *localToBlockBlobJobPartXfer
	t   TransferMsg
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

type Pipeline struct{}

func NewPipeline(logger Logger) Pipeline {
	return Pipeline{}
}
*/
//////////////////////////////////////////////////////////////////////////////////////////////////////

type xferFactory func(jptm IJobPartTransferMgr, pacer *pacer) xfer
type xfer interface {
	runPrologue(chunkChannel chan<- ChunkMsg)
}

/*
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
func (executionEngineHelper executionEngineHelper) mapFile(file *os.File) common.MMF {
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	memoryMappedFile, err := common.NewMMF(file, true, 0, int(fileInfo.Size()))
	if err != nil {
		panic(fmt.Sprintf("Error mapping: %s", err))
	}
	return memoryMappedFile
}

// create and memory map a file, given its path and length
func (executionEngineHelper executionEngineHelper) createAndMemoryMapFile(destinationPath string, fileSize int64) (common.MMF, *os.File) {
	f := executionEngineHelper.openFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		panic(truncateError)
	}

	return executionEngineHelper.mapFile(f), f
}

// open and memory map a file, given its path
func (executionEngineHelper executionEngineHelper) openAndMemoryMapFile(destinationPath string) (common.MMF, *os.File) {
	f := executionEngineHelper.openFile(destinationPath, os.O_RDWR)
	return executionEngineHelper.mapFile(f), f
}
*/
