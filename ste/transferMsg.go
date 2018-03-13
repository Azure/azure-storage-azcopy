package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/http"
	"time"
	"bytes"
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

	// TransferCancelFunc is the cancel func that is used to cancel the Transfer Context
	TransferCancelFunc context.CancelFunc

	// MinimumLogLevel is the log level below which all the message with lower log level will be logged to JobLog file.
	MinimumLogLevel common.LogLevel
	BlobType        common.BlobType
	SourceType      common.LocationType
	DestinationType common.LocationType
	Source          string
	SourceSize      uint64
	Destination     string
	// NumChunks is the number of chunks in which transfer will be split into while uploading the transfer.
	// NumChunks is not used in case of AppendBlob transfer.
	NumChunks       uint16
	BlockSize       uint32
}

// Log method logs the given log message to JobLog file.
// If the given log level is greater than Minimum Log level
// of the transfer, then messages will not be logged.
func (t *TransferMsg) Log(level common.LogLevel, msg string) {
	var buffer bytes.Buffer
	jobId := t.jobInfo.JobPartPlanInfo(t.partNumber).getJobPartPlanPointer().Id
	transferIdentifierString := fmt.Sprintf("transfer %d of job with jobId %s and part number %d ", t.transferIndex, jobId.String(), t.partNumber)
	buffer.WriteString(transferIdentifierString)
	buffer.WriteString(msg)
	t.jobInfo.Log(level, buffer.String())
}

// ChunksDone increments numberOfChunksDone counter by 1
// numberOfChunksDone in TransferInfo for each Transfer is used to monitor the number of chunks completed, failed or cancelled for a transfer
// numberOfChunksDone is also used to finalize the cancellation or completion of a transfer.
func (t *TransferMsg) ChunksDone() uint32 {
	return t.jobInfo.JobPartPlanInfo(t.partNumber).TransfersInfo[t.transferIndex].ChunksDone()
}

// UpdateNumTransferDone api increments the var numberOfTransfersDone_doNotUse by 1 atomically
// If this numberOfTransfersDone_doNotUse equals the number of transfer in a job part,
// all transfers of Job Part have either paused, cancelled or completed
func (t *TransferMsg) TransferDone() {
	jPartPlanInfo := t.jobInfo.JobPartPlanInfo(t.partNumber)
	totalNumberofTransfersCompleted := jPartPlanInfo.numberOfTransfersDone()
	t.Log(common.LogInfo, fmt.Sprintf("has total number %d of transfers paused, cancelled or completed", totalNumberofTransfersCompleted))
	if jPartPlanInfo.TransfersDone() == jPartPlanInfo.getJobPartPlanPointer().NumTransfers {
		t.jobInfo.PartsDone()
	}
}

// TransferStatus updates the status of given transfer for given jobId and partNumber
func (t *TransferMsg) TransferStatus(transferStatus common.TransferStatus) {
	transfer := t.jobInfo.JobPartPlanInfo(t.partNumber).Transfer(t.transferIndex)
	if transfer.transferStatus() == common.TransferFailed {
		return
	}
	transfer.setTransferStatus(transferStatus)
}

// getBlobHttpHeaders returns the azblob.BlobHTTPHeaders with blobData attributes of JobPart Order
func (t *TransferMsg) blobHttpHeaderAndMetadata(sourceBytes []byte) (httpHeaderProperties azblob.BlobHTTPHeaders, metadata azblob.Metadata) {

	// jPartPlanHeader is the JobPartPlan header for memory mapped JobPartOrder File

	jPartPlanInfo := t.jobInfo.JobPartPlanInfo(t.partNumber)
	jPartPlanHeader := jPartPlanInfo.getJobPartPlanPointer()
	contentType := ""
	contentEncoding := ""
	// If NoGuessMimeType is set to false, then detecting the content type
	if !jPartPlanHeader.BlobData.NoGuessMimeType {
		contentType = http.DetectContentType(sourceBytes)
	} else {
		// If the NoGuessMimeType is set to false, then using the user given content-type
		if jPartPlanHeader.BlobData.ContentTypeLength > 0 {
			contentType = string(jPartPlanHeader.BlobData.ContentType[:jPartPlanHeader.BlobData.ContentTypeLength])
		}
	}
	if jPartPlanHeader.BlobData.ContentEncodingLength > 0 {
		contentEncoding = string(jPartPlanHeader.BlobData.ContentEncoding[:jPartPlanHeader.BlobData.ContentEncodingLength])
	}
	httpHeaderProperties = azblob.BlobHTTPHeaders{ContentType: contentType, ContentEncoding: contentEncoding}

	metadata = jPartPlanInfo.metaData
	
	return
}

// PreserveLastModifiedTime checks for the PreserveLastModifiedTime flag in JobPartPlan of a transfer.
// If PreserveLastModifiedTime is set to true, it returns the lastModifiedTime of the source.
func (t *TransferMsg) PreserveLastModifiedTime() (time.Time, bool) {
	jPartPlanInfo := t.jobInfo.JobPartPlanInfo(t.partNumber)
	if jPartPlanInfo.getJobPartPlanPointer().BlobData.PreserveLastModifiedTime {
		lastModifiedTime := jPartPlanInfo.Transfer(t.transferIndex).ModifiedTime
		return time.Unix(0, int64(lastModifiedTime)), true
	}
	return time.Time{}, false
}
