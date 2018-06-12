package common

import (
	"reflect"
	"time"
)

var ERpcCmd = RpcCmd("")

// JobStatus indicates the status of a Job; the default is InProgress.
type RpcCmd string

func (RpcCmd) None() RpcCmd             { return RpcCmd("--none--") }
func (RpcCmd) CopyJobPartOrder() RpcCmd { return RpcCmd("CopyJobPartOrder") }
func (RpcCmd) ListJobs() RpcCmd         { return RpcCmd("ListJobs") }
func (RpcCmd) ListJobSummary() RpcCmd   { return RpcCmd("ListJobSummary") }
func (RpcCmd) ListJobTransfers() RpcCmd { return RpcCmd("ListJobTransfers") }
func (RpcCmd) CancelJob() RpcCmd        { return RpcCmd("CancelJob") }
func (RpcCmd) PauseJob() RpcCmd         { return RpcCmd("PauseJob") }
func (RpcCmd) ResumeJob() RpcCmd        { return RpcCmd("ResumeJob") }

func (c RpcCmd) String() string {
	return EnumHelper{}.String(c, reflect.TypeOf(c))
}
func (c RpcCmd) Pattern() string { return "/" + c.String() }

func (c *RpcCmd) Parse(s string) error {
	val, err := EnumHelper{}.Parse(reflect.TypeOf(c), s, false)
	if err == nil {
		*c = val.(RpcCmd)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrderRequest struct {
	Version             Version     // version of the azcopy
	JobID               JobID       // Guid - job identifier
	PartNum             PartNumber  // part number of the job
	IsFinalPart         bool        // to determine the final part for a specific job
	ForceWrite          bool        // to determine if the existing needs to be overwritten or not. If set to true, existing blobs are overwritten
	Priority            JobPriority // priority of the task
	FromTo              FromTo
	Include             map[string]int
	Exclude             map[string]int
	Transfers           []CopyTransfer
	LogLevel            LogLevel
	BlobAttributes      BlobTransferAttributes
	CredentialType      CredentialType
	JSONFormatTokenInfo string
}

type SyncJobPartOrderRequest struct {
	JobID            JobID
	FromTo           FromTo
	PartNumber       PartNumber
	LogLevel         LogLevel
	BlockSizeInBytes uint32
	CopyJobRequest   CopyJobPartOrderRequest
	DeleteJobRequest CopyJobPartOrderRequest
	// FilesDeletedLocally is used to keep track of the file that are deleted locally
	// Since local files to delete are not sent as transfer to STE
	// the count of the local files deletion is tracked using it.
	FilesDeletedLocally int
}

type CopyJobPartOrderResponse struct {
	ErrorMsg   string
	JobStarted bool
}

// represents the raw list command input from the user when requested the list of transfer with given status for given JobId
type ListRequest struct {
	JobID    JobID
	OfStatus string // TODO: OfStatus with string type sounds not good, change it to enum
}

// This struct represents the optional attribute for blob request header
type BlobTransferAttributes struct {
	//BlobType                 BlobType // The type of a blob - BlockBlob, PageBlob, AppendBlob
	ContentType              string        //The content type specified for the blob.
	ContentEncoding          string        //Specifies which content encodings have been applied to the blob.
	BlockBlobTier            BlockBlobTier // Specifies the tier to set on the block blobs.
	PageBlobTier             PageBlobTier  // Specifies the tier to set on the page blobs.
	Metadata                 string        //User-defined name-value pairs associated with the blob
	NoGuessMimeType          bool          // represents user decision to interpret the content-encoding from source file
	PreserveLastModifiedTime bool          // when downloading, tell engine to set file's timestamp to timestamp of blob
	BlockSizeInBytes         uint32
}

// ListJobsResponse represent the Job with JobId and
type ListJobsResponse struct {
	ErrorMessage string
	JobIDs       []JobID
}

// ListContainerResponse represents the list of blobs within the container.
type ListContainerResponse struct {
	Blobs []string
}

// represents the JobProgressPercentage Summary response for list command when requested the Job Progress Summary for given JobId
type ListJobSummaryResponse struct {
	ErrorMsg  string
	Timestamp time.Time
	JobID     JobID
	// CompleteJobOrdered determines whether the Job has been completely ordered or not
	CompleteJobOrdered          bool
	JobStatus                   JobStatus
	TotalTransfers              uint32
	TransfersCompleted          uint32
	TransfersFailed             uint32
	JobProgressPercentage       float64
	BytesOverWire               uint64
	FailedTransfers             []TransferDetail
	ThroughputInBytesPerSeconds float64
}

type ListJobTransfersRequest struct {
	JobID    JobID
	OfStatus TransferStatus
}

type ResumeJob struct {
	JobID           JobID
	IncludeTransfer map[string]int
	ExcludeTransfer map[string]int
}

// represents the Details and details of a single transfer
type TransferDetail struct {
	Src            string
	Dst            string
	TransferStatus TransferStatus
}

type CancelPauseResumeResponse struct {
	ErrorMsg              string
	CancelledPauseResumed bool
}

// represents the list of Details and details of number of transfers
type ListJobTransfersResponse struct {
	ErrorMsg string
	JobID    JobID
	Details  []TransferDetail
}
