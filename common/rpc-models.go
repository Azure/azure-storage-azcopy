package common

import (
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/JeffreyRichter/enum/enum"
)

var ERpcCmd = RpcCmd("")

// JobStatus indicates the status of a Job; the default is InProgress.
type RpcCmd string

func (RpcCmd) None() RpcCmd               { return RpcCmd("--none--") }
func (RpcCmd) CopyJobPartOrder() RpcCmd   { return RpcCmd("CopyJobPartOrder") }
func (RpcCmd) GetJobLCMWrapper() RpcCmd   { return RpcCmd("GetJobLCMWrapper") }
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

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ResourceString represents a source or dest string, that can have
// three parts: the main part, a sas, and extra query parameters that are not part of the sas.
type ResourceString struct {
	Value      string
	SAS        string // SAS should NOT be persisted in the plan files (both for security reasons, and because, at the time of any resume, it may be stale anyway. Resume requests fresh SAS on command line)
	ExtraQuery string
}

func (r ResourceString) Clone() ResourceString {
	return r // not pointer, so copied by value
}

func (r ResourceString) CloneWithValue(newValue string) ResourceString {
	c := r.Clone()
	c.Value = newValue // keep the other properties intact
	return c
}

func (r ResourceString) CloneWithConsolidatedSeparators() ResourceString {
	c := r.Clone()
	c.Value = ConsolidatePathSeparators(c.Value)
	return c
}

func (r ResourceString) FullURL() (*url.URL, error) {
	u, err := url.Parse(r.Value)
	if err == nil {
		r.addParamsToUrl(u, r.SAS, r.ExtraQuery)
	}
	return u, err
}

// to be used when the value is assumed to be a local path
// Using this signals "Yes, I really am ignoring the SAS and ExtraQuery on purpose",
// and will result in a panic in the case of programmer error of calling this method
// when those fields have values
func (r ResourceString) ValueLocal() string {
	if r.SAS != "" || r.ExtraQuery != "" {
		panic("resourceString is not a local resource string")
	}
	return r.Value
}

func (r ResourceString) addParamsToUrl(u *url.URL, sas, extraQuery string) {
	for _, p := range []string{sas, extraQuery} {
		if p == "" {
			continue
		}
		if len(u.RawQuery) > 0 {
			u.RawQuery += "&" + p
		} else {
			u.RawQuery = p
		}
	}
}

// Replace azcopy path separators (/) with the OS path separator
func ConsolidatePathSeparators(path string) string {
	pathSep := DeterminePathSeparator(path)

	return strings.ReplaceAll(path, AZCOPY_PATH_SEPARATOR_STRING, pathSep)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//Transfers describes each file/folder being transferred in a given JobPartOrder, and
//other auxilliary details of this order.
type Transfers struct {
	List                []CopyTransfer
	TotalSizeInBytes    uint64
	FileTransferCount   uint32
	FolderTransferCount uint32
}

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrderRequest struct {
	Version         Version         // version of azcopy
	JobID           JobID           // Guid - job identifier
	PartNum         PartNumber      // part number of the job
	IsFinalPart     bool            // to determine the final part for a specific job
	ForceWrite      OverwriteOption // to determine if the existing needs to be overwritten or not. If set to true, existing blobs are overwritten
	ForceIfReadOnly bool            // Supplements ForceWrite with addition setting for Azure Files objects with read-only attribute
	AutoDecompress  bool            // if true, source data with encodings that represent compression are automatically decompressed when downloading
	Priority        JobPriority     // priority of the task
	FromTo          FromTo
	Fpo             FolderPropertyOption // passed in from front-end to ensure that front-end and STE agree on the desired behaviour for the job
	// list of blobTypes to exclude.
	ExcludeBlobType []azblob.BlobType

	SourceRoot      ResourceString
	DestinationRoot ResourceString

	Transfers      Transfers
	LogLevel       LogLevel
	BlobAttributes BlobTransferAttributes
	CommandString  string // commandString hold the user given command which is logged to the Job log file
	CredentialInfo CredentialInfo

	PreserveSMBPermissions         PreservePermissionsOption
	PreserveSMBInfo                bool
	S2SGetPropertiesInBackend      bool
	S2SSourceChangeValidation      bool
	DestLengthValidation           bool
	S2SInvalidMetadataHandleOption InvalidMetadataHandleOption
	S2SPreserveBlobTags            bool
	CpkOptions                     CpkOptions
}

// CredentialInfo contains essential credential info which need be transited between modules,
// and used during creating Azure storage client Credential.
type CredentialInfo struct {
	CredentialType    CredentialType
	OAuthTokenInfo    OAuthTokenInfo
	S3CredentialInfo  S3CredentialInfo
	GCPCredentialInfo GCPCredentialInfo
}

type GCPCredentialInfo struct {
}

// S3CredentialInfo contains essential credential info which need to build up S3 client.
type S3CredentialInfo struct {
	Endpoint string
	Region   string
}

type CopyJobPartOrderErrorType string

var ECopyJobPartOrderErrorType CopyJobPartOrderErrorType

func (CopyJobPartOrderErrorType) NoTransfersScheduledErr() CopyJobPartOrderErrorType {
	return CopyJobPartOrderErrorType("NoTransfersScheduledErr")
}

type CopyJobPartOrderResponse struct {
	ErrorMsg   CopyJobPartOrderErrorType
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
	BlobType                 BlobType              // The type of a blob - BlockBlob, PageBlob, AppendBlob
	ContentType              string                // The content type specified for the blob.
	ContentEncoding          string                // Specifies which content encodings have been applied to the blob.
	ContentLanguage          string                // Specifies the language of the content
	ContentDisposition       string                // Specifies the content disposition
	CacheControl             string                // Specifies the cache control header
	BlockBlobTier            BlockBlobTier         // Specifies the tier to set on the block blobs.
	PageBlobTier             PageBlobTier          // Specifies the tier to set on the page blobs.
	Metadata                 string                // User-defined Name-value pairs associated with the blob
	NoGuessMimeType          bool                  // represents user decision to interpret the content-encoding from source file
	PreserveLastModifiedTime bool                  // when downloading, tell engine to set file's timestamp to timestamp of blob
	PutMd5                   bool                  // when uploading, should we create and PUT Content-MD5 hashes
	MD5ValidationOption      HashValidationOption  // when downloading, how strictly should we validate MD5 hashes?
	BlockSizeInBytes         int64                 // when uploading/downloading/copying, specify the size of each chunk
	DeleteSnapshotsOption    DeleteSnapshotsOption // when deleting, specify what to do with the snapshots
	BlobTagsString           string                // when user explicitly provides blob tags
}

type JobIDDetails struct {
	JobId         JobID
	CommandString string
	StartTime     int64
	JobStatus     JobStatus
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
	JobID     JobID
	// TODO: added for debugging purpose. remove later
	ActiveConnections int64 `json:",string"`
	// CompleteJobOrdered determines whether the Job has been completely ordered or not
	CompleteJobOrdered bool
	JobStatus          JobStatus

	TotalTransfers uint32 `json:",string"` // = FileTransfers + FolderPropertyTransfers. It also = TransfersCompleted + TransfersFailed + TransfersSkipped
	// FileTransfers and FolderPropertyTransfers just break the total down into the two types.
	// The name FolderPropertyTransfers is used to emphasise that is is only counting transferring the properties and existence of
	// folders. A "folder property transfer" does not include any files that may be in the folder. Those are counted as
	// FileTransfers.
	FileTransfers           uint32 `json:",string"`
	FolderPropertyTransfers uint32 `json:",string"`

	TransfersCompleted uint32 `json:",string"`
	TransfersFailed    uint32 `json:",string"`
	TransfersSkipped   uint32 `json:",string"`

	// includes bytes sent in retries (i.e. has double counting, if there are retries) and in failed transfers
	BytesOverWire uint64 `json:",string"`

	// does not include failed transfers or bytes sent in retries (i.e. no double counting). Includes successful transfers and transfers in progress
	TotalBytesTransferred uint64 `json:",string"`

	// sum of the total transfer enumerated so far.
	TotalBytesEnumerated uint64 `json:",string"`
	// sum of total bytes expected in the job (i.e. based on our current expectation of which files will be successful)
	TotalBytesExpected uint64 `json:",string"`

	PercentComplete float32 `json:",string"`

	// Stats measured from the network pipeline
	// Values are all-time values, for the duration of the job.
	// Will be zero if read outside the process running the job (e.g. with 'jobs show' command)
	AverageIOPS            int     `json:",string"`
	AverageE2EMilliseconds int     `json:",string"`
	ServerBusyPercentage   float32 `json:",string"`
	NetworkErrorPercentage float32 `json:",string"`

	FailedTransfers  []TransferDetail
	SkippedTransfers []TransferDetail
	PerfConstraint   PerfConstraint
	PerfStrings      []string `json:"-"`

	PerformanceAdvice []PerformanceAdvice
	IsCleanupJob      bool
}

// wraps the standard ListJobSummaryResponse with sync-specific stats
type ListSyncJobSummaryResponse struct {
	ListJobSummaryResponse
	DeleteTotalTransfers     uint32 `json:",string"`
	DeleteTransfersCompleted uint32 `json:",string"`
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
	Src                string
	Dst                string
	IsFolderProperties bool
	TransferStatus     TransferStatus
	TransferSize       uint64
	ErrorCode          int32  `json:",string"`
	ErrorMessage       string `json:",string"`
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
