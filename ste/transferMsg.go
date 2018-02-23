package ste

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
)

// TransferMsg represents the transfer message for scheduling the transfers
// These messages are exchanged over transfer channel of Coordinator Channels
type TransferMsg struct {
	// jobId - JobId of job to which the transfer belongs to
	jobId common.JobID
	// partNumber is the part of the Job to which transfer belongs to
	partNumber common.PartNumber
	// transferIndex is the index of transfer in JobPartOrder
	transferIndex uint32
	// infoMap is the pointer to in memory JobsInfo
	infoMap *JobsInfo

	// TransferContext is the context of transfer to be scheduled
	TransferContext context.Context
}

// JobInfo returns the JobInfo for given JobId
func (t TransferMsg) JobInfo() *JobInfo {
	return t.infoMap.JobInfo(t.jobId)
}

// SourceDestination returns the source and destination of Transfer which TransferMsg represents.
func (t TransferMsg) SourceDestination() (source, destination string) {
	jHandler := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber)
	source, destination = jHandler.getTransferSrcDstDetail(t.transferIndex)
	return source, destination
}

// SourceDestinationType returns the souce type and destination type of source and destination of a transfer.
func (t TransferMsg) SourceDestinationType() (sourceType, destinationType common.LocationType) {
	jPartPlanInfo := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber).getJobPartPlanPointer()
	sourceType, destinationType = jPartPlanInfo.SrcLocationType, jPartPlanInfo.DstLocationType
	return sourceType, destinationType
}

// Returns the size of source of transfer on disk
func (t TransferMsg) SourceSize() uint64 {
	return t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber).Transfer(t.transferIndex).SourceSize
}

// TransferCancelFunc returns the cancel func to cancel an in-flight transfer
func (t TransferMsg) TransferCancelFunc() func() {
	return t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber).TransfersInfo[t.transferIndex].cancel
}

// getNumChunks api returns the number of chunks depending on source Type and destination type
func (t TransferMsg) NumberOfChunks() uint16 {
	// jHandler is the JobPartPlanInfo Pointer for given JobId and part number
	jHandler := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber)

	// jPartPlanPointer is the memory map JobPartPlan for given JobId and part number
	jPartPlanPointer := jHandler.getJobPartPlanPointer()

	transfer := jHandler.Transfer(t.transferIndex)

	blockSize := jPartPlanPointer.BlobData.BlockSize

	if uint64(transfer.SourceSize)%blockSize == 0 {
		return uint16(uint64(transfer.SourceSize) / blockSize)
	} else {
		return uint16(uint64(transfer.SourceSize)/blockSize) + 1
	}
}

// TransferIdentifierString returns the string format consisting of JobId, part number and transfer Index
// TransferIdentifierString is used to log the details of specific transfer
func (t TransferMsg) TransferIdentifierString() string {
	return fmt.Sprintf("transfer Id %d of Job with JobId %s and part number %d", t.transferIndex, t.jobId.String(), t.partNumber)
}

// Returns the block size of JobPartPlanOrder of a transfer
func (t TransferMsg) BlockSize() uint64 {
	return t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber).
		getJobPartPlanPointer().BlobData.BlockSize
}

// UpdateNumTransferDone api increments the var numberOfTransfersDone_doNotUse by 1 atomically
// If this numberOfTransfersDone_doNotUse equals the number of transfer in a job part,
// all transfers of Job Part have either paused, cancelled or completed
func (t TransferMsg) updateNumberOfTransferDone() {
	jobInfo := t.infoMap.JobInfo(t.jobId)
	jHandler := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber)
	jPartPlanInfo := jHandler.getJobPartPlanPointer()
	totalNumberofTransfersCompleted := jHandler.numberOfTransfersDone()
	jobInfo.Log(common.LogInfo, fmt.Sprintf("total number of transfers paused, cancelled or completed for Job %s and part number %d is %d", t.jobId, t.partNumber, totalNumberofTransfersCompleted))
	if jHandler.incrementNumberOfTransfersDone() == jPartPlanInfo.NumTransfers {
		updateNumberOfPartsDone(t.jobId, t.infoMap)
	}
}

// incrementNumberOfChunksDone increments numberOfChunksDone counter by 1
// numberOfChunksDone is used to monitor the number of chunks completed, failed or cancelled for a transfer
// numberOfChunksDone is also used to finalize the cancellation or completion of a transfer.
func (t TransferMsg) incrementNumberOfChunksDone() uint32 {
	jPartPlanInfo := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber)

	return atomic.AddUint32(&(jPartPlanInfo.TransfersInfo[t.transferIndex].NumberOfChunksDone), 1)
}

// updateTransferStatus updates the status of given transfer for given jobId and partNumber
func (t TransferMsg) updateTransferStatus(transferStatus common.TransferStatus) {
	jHandler := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber)
	transferHeader := jHandler.Transfer(t.transferIndex)
	transferHeader.setTransferStatus(transferStatus)
}

// getBlobHttpHeaders returns the azblob.BlobHTTPHeaders with blobData attributes of JobPart Order
func (t TransferMsg) getBlobHttpHeaders(sourceBytes []byte) azblob.BlobHTTPHeaders {

	// jPartPlanHeader is the JobPartPlan header for memory mapped JobPartOrder File
	jPartPlanHeader := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber).getJobPartPlanPointer()
	contentTpe := ""
	contentEncoding := ""
	// If NoGuessMimeType is set to true, then detecting the content type
	if jPartPlanHeader.BlobData.NoGuessMimeType {
		contentTpe = http.DetectContentType(sourceBytes)
	} else {
		// If the NoGuessMimeType is set to false, then using the user given content-type
		if jPartPlanHeader.BlobData.ContentEncodingLength > 0 {
			contentTpe = string(jPartPlanHeader.BlobData.ContentType[:])
		}
	}

	if jPartPlanHeader.BlobData.ContentEncodingLength > 0 {
		contentEncoding = string(jPartPlanHeader.BlobData.ContentEncoding[:])
	}
	httpHeaderProperties := azblob.BlobHTTPHeaders{ContentType: contentTpe, ContentEncoding: contentEncoding}
	return httpHeaderProperties
}

// getJobPartMetaData returns the meta data of JobPart Order store in following format
// "key1=val1;key2=val2;key3=val3"
func (t TransferMsg) getJobPartMetaData() azblob.Metadata {
	// jPartPlanHeader is the JobPartPlan header for memory mapped JobPartOrder File
	jPartPlanHeader := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber).getJobPartPlanPointer()
	if jPartPlanHeader.BlobData.MetaDataLength == 0 {
		return azblob.Metadata{}
	}
	var mData azblob.Metadata
	// metaDataString is meta data stored as string in JobPartOrder file
	metaDataString := string(jPartPlanHeader.BlobData.MetaData[:])
	// Split the meta data string using ';' to get key=value pairs
	metaDataKeyValues := strings.Split(metaDataString, ";")
	for index := 0; index < len(metaDataKeyValues); index++ {
		// Splitting each key=value pair to get key and values
		keyValue := strings.Split(metaDataKeyValues[index], "=")
		mData[keyValue[0]] = keyValue[1]
	}
	return mData
}

// ifPreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it takes the last modified time of source from resp
// and sets the mtime , atime of destination to the former last modified time.
func (t TransferMsg) ifPreserveLastModifiedTime(resp *azblob.GetResponse) {
	jPartPlanInfo := t.infoMap.JobPartPlanInfo(t.jobId, t.partNumber)
	if jPartPlanInfo.getJobPartPlanPointer().BlobData.PreserveLastModifiedTime {
		_, dst := jPartPlanInfo.getTransferSrcDstDetail(t.transferIndex)
		lastModifiedTime := resp.LastModified()
		err := os.Chtimes(dst, lastModifiedTime, lastModifiedTime)
		if err != nil {
			t.infoMap.JobInfo(t.jobId).Panic(errors.New(fmt.Sprintf("error changing the modified time of file %s to the time %s", dst, lastModifiedTime.String())))
			return
		}
		t.infoMap.JobInfo(t.jobId).Log(common.LogInfo, fmt.Sprintf("successfully changed the modified time of file %s to the time %s", dst, lastModifiedTime.String()))
	}
}
