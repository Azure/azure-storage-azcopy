package ste

import (
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"sync/atomic"
	"google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"os"
)

//These constant defines the various types of source and destination of the transfers

const dataSchemaVersion = 0 // To be Incremented every time when we release azcopy with changed dataschema

type JobStatusCode uint32

func (status JobStatusCode) String() (statusString string){
	switch uint32(status){
	case 0:
		return "InProgress"
	case 1:
		return "JobPaused"
	case 2:
		return "JobCancelled"
	case 3:
		return "JobCompleted"
	default:
		return "InvalidStatusCode"
	}
}

type TransferStatus uint32

func (status TransferStatus) String() (statusString string){
	switch uint32(status){
	case 0:
		return "InProgress"
	case 1:
		return "TransferComplete"
	case 2:
		return "TransferFailed"
	case 254:
		return "TransferAny"
	default:
		return "InvalidStatusCode"
	}
}

const (
	// Transfer is currently executing or cancelled before failure or successful execution
	TransferInProgress TransferStatus = 0

	// Transfer has completed successfully
	TransferComplete   TransferStatus = 1

	// Transfer has failed due to some error. This status does represent the state when transfer is cancelled
	TransferFailed     TransferStatus = 2

	// Transfer is any of the three possible state (InProgress, Completer or Failed)

	TransferAny        TransferStatus = TransferStatus(254)
)

const (
	// Job Part is currently executing
	InProgress JobStatusCode = 0

	// Job Part is currently paused and no transfer of Job is currently executing
	Paused JobStatusCode = 1

	// Job Part is cancelled and all transfers of the JobPart are cancelled
	Cancelled JobStatusCode = 2

	// Job Part has completed and no transfer of JobPart is currently executing
	Completed JobStatusCode = 3
)

// JobPartPlan represent the header of Job Part's Memory Map File
type JobPartPlanHeader struct {
	Version            uint32 // represent the version of data schema format of header
	Id                 [128 / 8]byte // represents the 18 byte JobId
	PartNum            uint32 // represents the part number of the JobOrder
	IsFinalPart        bool // represents whether this part is final part or not
	Priority           uint8 // represents the priority of JobPart order (High, Medium and Low)
	TTLAfterCompletion uint32 // Time to live after completion is used to persists the file on disk of specified time after the completion of JobPartOrder
	SrcLocationType    common.LocationType // represents type of source location
	DstLocationType    common.LocationType // represents type of destination location
	NumTransfers       uint32 // represents the number of transfer the JobPart order has
	LogSeverity        pipeline.LogLevel // represent the log verbosity level of logs for the specific Job
	BlobData           JobPartPlanBlobData // represent the optional attributes of JobPart Order
	// jobStatus represents the current status of JobPartPlan
	// It can have these possible values - InProgress, Paused, Cancelled and Completed
	// jobStatus is a private member whose value can be accessed by getJobStatus and setJobStatus
	jobStatus          JobStatusCode
}

func (jPartPlanHeader *JobPartPlanHeader) getJobStatus() (JobStatusCode){
	return JobStatusCode(atomic.LoadUint32((*uint32)(&jPartPlanHeader.jobStatus)))
}

func (jPartPlanHeader *JobPartPlanHeader)setJobStatus(status JobStatusCode) {
	atomic.StoreUint32((*uint32)(&jPartPlanHeader.jobStatus), uint32(status))
}

// JobPartPlan represent the header of Job Part's Optional Attributes in Memory Map File
type JobPartPlanBlobData struct {
	ContentTypeLength     uint8
	ContentType           [256]byte
	ContentEncodingLength uint8
	ContentEncoding       [256]byte
	MetaDataLength        uint16
	MetaData              [1000]byte
	BlockSize             uint64
}

// JobPartPlan represent the header of Job Part's Transfer in Memory Map File
type JobPartPlanTransfer struct {
	Offset         uint64
	SrcLength      uint16
	DstLength      uint16
	ChunkNum       uint16
	ModifiedTime   uint32
	SourceSize     uint64
	CompletionTime uint64
	transferStatus TransferStatus
}

// getTransferStatus returns the transfer status of current transfer of job part atomically
func (jPartPlanTransfer *JobPartPlanTransfer) getTransferStatus() (TransferStatus){
	return TransferStatus(atomic.LoadUint32((*uint32)(&jPartPlanTransfer.transferStatus)))
}

// getTransferStatus sets the transfer status of current transfer to given status atomically
func (jPartPlanTransfer *JobPartPlanTransfer) setTransferStatus(status TransferStatus){
	atomic.StoreUint32((*uint32)(&jPartPlanTransfer.transferStatus), uint32(status))
}

const (
	HighJobPriority    = 0
	MediumJobPriority  = 1
	LowJobPriority     = 2
	DefaultJobPriority = HighJobPriority
)

const (
	MAX_SIZE_CONTENT_TYPE     = 256
	MAX_SIZE_CONTENT_ENCODING = 256
	MAX_SIZE_META_DATA        = 1000
)
