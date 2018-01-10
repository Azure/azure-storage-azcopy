package main

import (
	"github.com/Azure/azure-storage-azcopy/common"
)

//These constant defines the various types of source and destination of the transfers

const dataSchemaVersion = 1 // To be Incremented every time when we release azcopy with changed dataschema

// JobPartPlan represent the header of Job Part's Memory Map File
type JobPartPlanHeader struct {
	Version uint32 // represent the version of data schema format
	Id [128 / 8] byte
	PartNum uint32
	IsFinalPart bool
	Priority uint8
	TTLAfterCompletion uint32
	SrcLocationType common.LocationType
	DstLocationType common.LocationType
	NumTransfers uint32
	//Status uint8
	BlobData JobPartPlanBlobData
}

type JobPartPlanBlobData struct {
	ContentTypeLength     uint8
	ContentType           [256]byte
	ContentEncodingLength uint8
	ContentEncoding       [256]byte
	MetaDataLength        uint16
	MetaData              [1000]byte
	BlockSizeInKB         uint64
}

type JobPartPlanTransfer struct {
	Offset         uint64
	SrcLength      uint16
	DstLength      uint16
	ChunkNum       uint16
	ModifiedTime   uint32
	Status         uint8
	SourceSize     uint64
	CompletionTime uint64
}



//todo comments
type JobPartPlanTransferChunk struct {
	BlockId [128 / 8]byte
	Status uint8
}

type CommandType int
const (
	REQUEST_COMMAND_AZCOPY_UPLOAD CommandType = 1 + iota
	REQUEST_COMMAND_AZCOPY_DOWNLOAD
)

const (
	TransferEntryChunkUpdateSuccess = "updated the chunk %d of transfer %d of Job %s"
)
const (
	DirectoryListingError = "not able to list contents of directory %s"
	FileCreationError = "Error %s Occured while creating the File for JobId %s \n"
	FileAlreadyExists = "file %s already exists"
	TransferIndexOutOfBoundError = "transfer %d of JobPart %s does not exists. Transfer Index exceeds number of transfer for this JobPart"
	MemoryMapFileUnmappedAlreadyError = "memory map file already unmapped. Map it again to use further"
	ChunkIndexOutOfBoundError = "given chunk %d of transfer %d of JobPart %s does not exists. Chunk Index exceeds number of chunks for transfer %d"
	MemoryMapFileInitializationError = "error memory mapping the file for jobId %s and partNo %d with err %s"
	InvalidFileName = "invalid file name for JobId %s and partNo %d"
	TransferTaskOffsetCalculationError = "calculated offset %d and actual offset %d of Job %s part %d and transfer entry %d does not match"
	InvalidJobId = "no active job for Job Id %s"
	InvalidPartNo = "no active part %s for Job Id %s"
	TransferStatusMarshallingError = "error marshalling the transfer status for Job Id %s and part No %d"
	InvalidHttpRequestBody = "the http Request Does not have a valid body definition"
	HttpRequestBodyReadError = "error reading the HTTP Request Body"
	HttpRequestUnmarshalError = "error UnMarshalling the HTTP Request Body"
	InvalidJobIDError = ""
	DateFormat = ""
	InvalidDirectoryError = ""
	InvalidArgumentsError = ""
	ContentEncodingLengthError = ""
	ContentTypeLengthError = ""
	MetaDataLengthError = ""
)

const (
	ChunkTransferStatusInactive = 0
	ChunkTransferStatusActive = 1
	ChunkTransferStatusProgress = 2
	ChunkTransferStatusComplete = 3
)

const (
	TransferStatusInactive = 0
	TransferStatusActive = 1
	TransferStatusProgress = 2
	TransferStatusComplete = 3
	TransferStatusFailed = 4
)

const (
	HighJobPriority = 3
	MediumJobPriority = 2
	LowJobPriority = 1
	DefaultJobPriority = HighJobPriority
)

const (
	BLOCK_LENGTH = 1000000 // in bytes
	MAX_NUMBER_ROUTINES = 10
	MAX_SIZE_CONTENT_TYPE = 256
	MAX_SIZE_CONTENT_ENCODING = 256
	MAX_SIZE_META_DATA = 1000
	OffSetToNumTransfer = 29
	OffsetToDataLength = 33
)

const (
	SuccessfulAZCopyRequest = "Succesfully Trigger the AZCopy request"
	UnsuccessfulAZCopyRequest = "Not able to trigger the AZCopy request"
	InvalidParametersInAZCopyRequest = "invalid parameters in az copy request"
	ACCOUNT_NAME = "azcopynextgendev1"
	ACCOUNT_KEY = "iVpnmZUb1aI9vMJ+yx+NOiBVJ63E25Ejs3ZA74Uqgml9bjrRQ4CEayQdZV7KqRnw0CrYQzc456+SwyI1KpyitA=="
	CONTAINER_NAME = "mycontainer"
	CRC64BitExample ="AFC0A0012976B444"
)

type statusQuery struct {
	Guid string
	PartNo string
	TransferIndex uint32
	ChunkIndex uint16
}
