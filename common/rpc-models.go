package common

import (
	"reflect"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/JeffreyRichter/enum/enum"
)

var ERpcCmd = RpcCmd("")

// JobStatus indicates the status of a Job; the default is InProgress.
type RpcCmd string

func (RpcCmd) None() RpcCmd               { return RpcCmd("--none--") }
func (RpcCmd) CopyJobPartOrder() RpcCmd   { return RpcCmd("CopyJobPartOrder") }
func (RpcCmd) ListJobs() RpcCmd           { return RpcCmd("ListJobs") }
func (RpcCmd) ListJobSummary() RpcCmd     { return RpcCmd("ListJobSummary") }
func (RpcCmd) ListSyncJobSummary() RpcCmd { return RpcCmd("ListSyncJobSummary") }
func (RpcCmd) ListJobTransfers() RpcCmd   { return RpcCmd("ListJobTransfers") }
func (RpcCmd) CancelJob() RpcCmd          { return RpcCmd("Cancel") }
func (RpcCmd) PauseJob() RpcCmd           { return RpcCmd("PauseJob") }
func (RpcCmd) ResumeJob() RpcCmd          { return RpcCmd("ResumeJob") }
func (RpcCmd) GetJobFromTo() RpcCmd       { return RpcCmd("GetJobFromTo") }

func (c RpcCmd) String() string {
	return enum.String(c, reflect.TypeOf(c))
}
func (c RpcCmd) Pattern() string { return "/" + c.String() }

func (c *RpcCmd) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(c), s, false)
	if err == nil {
		*c = val.(RpcCmd)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrderRequest struct {
	Version     Version     // version of the azcopy
	JobID       JobID       // Guid - job identifier
	PartNum     PartNumber  // part number of the job
	IsFinalPart bool        // to determine the final part for a specific job
	ForceWrite  bool        // to determine if the existing needs to be overwritten or not. If set to true, existing blobs are overwritten
	Priority    JobPriority // priority of the task
	FromTo      FromTo
	Include     map[string]int
	Exclude     map[string]int
	// list of blobTypes to exclude.
	ExcludeBlobType []azblob.BlobType
	SourceRoot      string
	DestinationRoot string
	Transfers       []CopyTransfer
	LogLevel        LogLevel
	BlobAttributes  BlobTransferAttributes
	SourceSAS       string
	DestinationSAS  string
	// commandString hold the user given command which is logged to the Job log file
	CommandString  string
	CredentialInfo CredentialInfo

	S2SGetPropertiesInBackend      bool
	S2SSourceChangeValidation      bool
	S2SInvalidMetadataHandleOption InvalidMetadataHandleOption
}

// CredentialInfo contains essential credential info which need be transited between modules,
// and used during creating Azure storage client Credential.
type CredentialInfo struct {
	CredentialType   CredentialType
	OAuthTokenInfo   OAuthTokenInfo
	S3CredentialInfo S3CredentialInfo
}

// S3CredentialInfo contains essential credential info which need to build up S3 client.
type S3CredentialInfo struct {
	Endpoint string
	Region   string
}

type CopyJobPartOrderResponse struct {
	ErrorMsg   string
	JobStarted bool
}

// represents the raw list command input from the user when requested the list of transfer with given status for given JobId
type ListRequest struct {
	JobID    JobID
	OfStatus string // TODO: OfStatus with string type sounds not good, change it to enum
	Output   OutputFormat
}

// This struct represents the optional attribute for blob request header
type BlobTransferAttributes struct {
	BlobType                 BlobType             // The type of a blob - BlockBlob, PageBlob, AppendBlob
	ContentType              string               //The content type specified for the blob.
	ContentEncoding          string               //Specifies which content encodings have been applied to the blob.
	BlockBlobTier            BlockBlobTier        // Specifies the tier to set on the block blobs.
	PageBlobTier             PageBlobTier         // Specifies the tier to set on the page blobs.
	Metadata                 string               //User-defined Name-value pairs associated with the blob
	NoGuessMimeType          bool                 // represents user decision to interpret the content-encoding from source file
	PreserveLastModifiedTime bool                 // when downloading, tell engine to set file's timestamp to timestamp of blob
	PutMd5                   bool                 // when uploading, should we create and PUT Content-MD5 hashes
	MD5ValidationOption      HashValidationOption // when downloading, how strictly should we validate MD5 hashes?
	BlockSizeInBytes         uint32
}

type JobIDDetails struct {
	JobId         JobID
	CommandString string
	StartTime     int64
}

// ListJobsResponse represent the Job with JobId and
type ListJobsResponse struct {
	ErrorMessage string
	JobIDDetails []JobIDDetails
}

// ListContainerResponse represents the list of blobs within the container.
type ListContainerResponse struct {
	Blobs []string
}

// represents the JobProgressPercentage Summary response for list command when requested the Job Progress Summary for given JobId
type ListJobSummaryResponse struct {
	ErrorMsg  string
	Timestamp time.Time `json:"-"`
	JobID     JobID     `json:"-"`
	// TODO: added for debugging purpose. remove later
	ActiveConnections int64
	// CompleteJobOrdered determines whether the Job has been completely ordered or not
	CompleteJobOrdered bool
	JobStatus          JobStatus
	TotalTransfers     uint32
	TransfersCompleted uint32
	TransfersFailed    uint32
	TransfersSkipped   uint32
	BytesOverWire      uint64
	// sum of the size of transfer completed successfully so far.
	TotalBytesTransferred uint64
	// sum of the total transfer enumerated so far.
	TotalBytesEnumerated uint64
	FailedTransfers      []TransferDetail
	SkippedTransfers     []TransferDetail
	PerfConstraint       PerfConstraint
	PerfStrings          []string `json:"-"`
}

// represents the JobProgressPercentage Summary response for list command when requested the Job Progress Summary for given JobId
type ListSyncJobSummaryResponse struct {
	ErrorMsg  string
	Timestamp time.Time `json:"-"`
	JobID     JobID     `json:"-"`
	// TODO: added for debugging purpose. remove later
	ActiveConnections int64
	// CompleteJobOrdered determines whether the Job has been completely ordered or not
	CompleteJobOrdered       bool
	JobStatus                JobStatus
	CopyTotalTransfers       uint32
	CopyTransfersCompleted   uint32
	CopyTransfersFailed      uint32
	BytesOverWire            uint64
	DeleteTotalTransfers     uint32
	DeleteTransfersCompleted uint32
	DeleteTransfersFailed    uint32
	FailedTransfers          []TransferDetail
	PerfConstraint           PerfConstraint
	PerfStrings              []string `json:"-"`
	// sum of the size of transfer completed successfully so far.
	TotalBytesTransferred uint64
	// sum of the total transfer enumerated so far.
	TotalBytesEnumerated uint64
}

type ListJobTransfersRequest struct {
	JobID    JobID
	OfStatus TransferStatus
}

type ResumeJobRequest struct {
	JobID           JobID
	SourceSAS       string
	DestinationSAS  string
	IncludeTransfer map[string]int
	ExcludeTransfer map[string]int
	CredentialInfo  CredentialInfo
}

// represents the Details and details of a single transfer
type TransferDetail struct {
	Src            string
	Dst            string
	TransferStatus TransferStatus
	ErrorCode      int32
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

// GetJobFromToRequest indicates request to get job's FromTo info from job part plan header
type GetJobFromToRequest struct {
	JobID JobID
}

// GetJobFromToResponse indicates response to get job's FromTo info.
type GetJobFromToResponse struct {
	ErrorMsg    string
	FromTo      FromTo
	Source      string
	Destination string
}
