package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/http"
	"strings"
	"time"
)

// TransferMsg represents the transfer message for scheduling the transfers
// These messages are exchanged over transfer channel of Coordinator Channels
type TransferMsg struct {
	// partNumber is the part of the Job to which transfer belongs to
	partNumber common.PartNumber
	// transferIndex is the index of transfer in JobPartOrder
	transferIndex uint32
	// infoMap is the pointer to in memory JobsInfo
	jobInfo *JobInfo

	// TransferContext is the context of transfer to be scheduled
	TransferContext    context.Context
	TransferCancelFunc context.CancelFunc

	MinimumLogLevel common.LogLevel
	SourceType      common.LocationType
	DestinationType common.LocationType
	Source          string
	SourceSize      uint64
	Destination     string
	NumChunks       uint16
	BlockSize       uint32
}

func (t TransferMsg) Log(level common.LogLevel, msg string) {
	jobId := t.jobInfo.JobPartPlanInfo(t.partNumber).getJobPartPlanPointer().Id
	t.jobInfo.Log(level, fmt.Sprintf("transfer %d of job with jobId %s and part number %d %s", t.transferIndex, jobId.String(), t.partNumber, msg))
}

// ChunksDone increments numberOfChunksDone counter by 1
// numberOfChunksDone in TransferInfo for each Transfer is used to monitor the number of chunks completed, failed or cancelled for a transfer
// numberOfChunksDone is also used to finalize the cancellation or completion of a transfer.
func (t TransferMsg) ChunksDone() uint32 {
	return t.jobInfo.JobPartPlanInfo(t.partNumber).TransfersInfo[t.transferIndex].ChunksDone()
}

// UpdateNumTransferDone api increments the var numberOfTransfersDone_doNotUse by 1 atomically
// If this numberOfTransfersDone_doNotUse equals the number of transfer in a job part,
// all transfers of Job Part have either paused, cancelled or completed
func (t TransferMsg) TransferDone() {
	jPartPlanInfo := t.jobInfo.JobPartPlanInfo(t.partNumber)
	totalNumberofTransfersCompleted := jPartPlanInfo.numberOfTransfersDone()
	t.Log(common.LogInfo, fmt.Sprintf("has total number %d of transfers paused, cancelled or completed", totalNumberofTransfersCompleted))
	if jPartPlanInfo.TransfersDone() == jPartPlanInfo.getJobPartPlanPointer().NumTransfers {
		t.jobInfo.PartsDone()
	}
}

// TransferStatus updates the status of given transfer for given jobId and partNumber
func (t TransferMsg) TransferStatus(transferStatus common.TransferStatus) {
	transfer := t.jobInfo.JobPartPlanInfo(t.partNumber).Transfer(t.transferIndex)
	if transfer.transferStatus() == common.TransferFailed {
		return
	}
	transfer.setTransferStatus(transferStatus)
}

// getBlobHttpHeaders returns the azblob.BlobHTTPHeaders with blobData attributes of JobPart Order
func (t TransferMsg) blobHttpHeaderAndMetadata(sourceBytes []byte) (httpHeaderProperties azblob.BlobHTTPHeaders, metadata azblob.Metadata) {

	// jPartPlanHeader is the JobPartPlan header for memory mapped JobPartOrder File
	jPartPlanHeader := t.jobInfo.JobPartPlanInfo(t.partNumber).getJobPartPlanPointer()
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
	httpHeaderProperties = azblob.BlobHTTPHeaders{ContentType: contentTpe, ContentEncoding: contentEncoding}

	if jPartPlanHeader.BlobData.MetaDataLength == 0 {
		return
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
	metadata = mData
	return
}

// PreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it takes the last modified time of source from resp
// and sets the mtime , atime of destination to the former last modified time.
func (t TransferMsg) PreserveLastModifiedTime() (time.Time, bool) {
	jPartPlanInfo := t.jobInfo.JobPartPlanInfo(t.partNumber)
	if jPartPlanInfo.getJobPartPlanPointer().BlobData.PreserveLastModifiedTime {
		lastModifiedTime := jPartPlanInfo.Transfer(t.transferIndex).ModifiedTime
		return time.Unix(0, int64(lastModifiedTime)), true
	}
	return time.Time{}, false
}
