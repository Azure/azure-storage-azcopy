package ste

import (
	"errors"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// dataSchemaVersion defines the data schema version of JobPart order files supported by
// current version of azcopy
// To be Incremented every time when we release azcopy with changed dataSchema
const DataSchemaVersion common.Version = 16

const (
	CustomHeaderMaxBytes  = 256
	MetadataMaxBytes      = 1000 // If > 65536, then jobPartPlanBlobData's MetadataLength field's type must change
	BlobTagsMaxByte       = 4000
	MaxErrorMessageLength = 1000
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type JobPartPlanMMF common.MMF

func (mmf *JobPartPlanMMF) Plan() *JobPartPlanHeader {
	// getJobPartPlanPointer returns the memory map JobPartPlanHeader pointer
	// casting the mmf slice's address  to JobPartPlanHeader Pointer
	return (*JobPartPlanHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(mmf)).Data))
}
func (mmf *JobPartPlanMMF) Unmap() { (*common.MMF)(mmf).Unmap() }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// JobPartPlanHeader represents the header of Job Part's memory-mapped file
type JobPartPlanHeader struct {
	// Once set, the following fields are constants; they should never be modified
	Version                common.Version    // The version of data schema format of header; see the dataSchemaVersion constant
	StartTime              int64             // The start time of this part
	JobID                  common.JobID      // Job Part's JobID
	PartNum                common.PartNumber // Job Part's part number (0+)
	SourceRootLength       uint16            // The length of the source root path
	SourceRoot             [1000]byte        // The root directory of the source
	SourceExtraQueryLength uint16
	SourceExtraQuery       [1000]byte // Extra query params applicable to the source
	DestinationRootLength  uint16     // The length of the destination root path
	DestinationRoot        [1000]byte // The root directory of the destination
	DestExtraQueryLength   uint16
	DestExtraQuery         [1000]byte                  // Extra query params applicable to the dest
	IsFinalPart            bool                        // True if this is the Job's last part; else false
	ForceWrite             common.OverwriteOption      // True if the existing blobs needs to be overwritten.
	ForceIfReadOnly        bool                        // Supplements ForceWrite with an additional setting for Azure Files. If true, the read-only attribute will be cleared before we overwrite
	AutoDecompress         bool                        // if true, source data with encodings that represent compression are automatically decompressed when downloading
	Priority               common.JobPriority          // The Job Part's priority
	TTLAfterCompletion     uint32                      // Time to live after completion is used to persists the file on disk of specified time after the completion of JobPartOrder
	FromTo                 common.FromTo               // The location of the transfer's source & destination
	Fpo                    common.FolderPropertyOption // option specifying how folders will be handled
	CommandStringLength    uint32
	NumTransfers           uint32              // The number of transfers in the Job part
	LogLevel               common.LogLevel     // This Job Part's minimal log level
	DstBlobData            JobPartPlanDstBlob  // Additional data for blob destinations
	DstLocalData           JobPartPlanDstLocal // Additional data for local destinations

	PreservePermissions common.PreservePermissionsOption
	PreserveSMBInfo     bool
	// S2SGetPropertiesInBackend represents whether to enable get S3 objects' or Azure files' properties during s2s copy in backend.
	S2SGetPropertiesInBackend bool
	// S2SSourceChangeValidation represents whether user wants to check if source has changed after enumerating.
	S2SSourceChangeValidation bool
	// DestLengthValidation represents whether the user wants to check if the destination has a different content-length
	DestLengthValidation bool
	// S2SInvalidMetadataHandleOption represents how user wants to handle invalid metadata.
	S2SInvalidMetadataHandleOption common.InvalidMetadataHandleOption

	// Any fields below this comment are NOT constants; they may change over as the job part is processed.
	// Care must be taken to read/write to these fields in a thread-safe way!

	// jobStatus_doNotUse represents the current status of JobPartPlan
	// jobStatus_doNotUse is a private member whose value can be accessed by Status and SetJobStatus
	// jobStatus_doNotUse should not be directly accessed anywhere except by the Status and SetJobStatus
	atomicJobStatus common.JobStatus

	// For delete operation specify what to do with snapshots
	DeleteSnapshotsOption common.DeleteSnapshotsOption
}

// Status returns the job status stored in JobPartPlanHeader in thread-safe manner
func (jpph *JobPartPlanHeader) JobStatus() common.JobStatus {
	return jpph.atomicJobStatus.AtomicLoad()
}

// SetJobStatus sets the job status in JobPartPlanHeader in thread-safe manner
func (jpph *JobPartPlanHeader) SetJobStatus(newJobStatus common.JobStatus) {
	jpph.atomicJobStatus.AtomicStore(newJobStatus)
}

// Transfer api gives memory map JobPartPlanTransfer header for given index
func (jpph *JobPartPlanHeader) Transfer(transferIndex uint32) *JobPartPlanTransfer {
	// get memory map JobPartPlan Header Pointer
	if transferIndex >= jpph.NumTransfers {
		panic(errors.New("requesting a transfer index greater than what is available"))
	}

	// (Job Part Plan's file address) + (header size) --> beginning of transfers in file
	// Add (transfer size) * (transfer index)
	return (*JobPartPlanTransfer)(unsafe.Pointer((uintptr(unsafe.Pointer(jpph)) + unsafe.Sizeof(*jpph) + uintptr(jpph.CommandStringLength)) + (unsafe.Sizeof(JobPartPlanTransfer{}) * uintptr(transferIndex))))
}

// CommandString returns the command string given by user when job was created
func (jpph *JobPartPlanHeader) CommandString() string {
	commandSlice := []byte{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&commandSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(unsafe.Sizeof(*jpph)) // Address of Job Part Plan + Command String Length
	sh.Len = int(jpph.CommandStringLength)
	sh.Cap = sh.Len
	return string(commandSlice)
}

func (jpph *JobPartPlanHeader) TransferSrcDstRelatives(transferIndex uint32) (relSource, relDest string) {
	jppt := jpph.Transfer(transferIndex)

	srcSlice := []byte{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&srcSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(jppt.SrcOffset) // Address of Job Part Plan + this transfer's src string offset
	sh.Len = int(jppt.SrcLength)
	sh.Cap = sh.Len
	srcRelative := string(srcSlice)

	dstSlice := []byte{}
	sh = (*reflect.SliceHeader)(unsafe.Pointer(&dstSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(jppt.SrcOffset) + uintptr(jppt.SrcLength) // Address of Job Part Plan + this transfer's src string offset + length of this transfer's src string
	sh.Len = int(jppt.DstLength)
	sh.Cap = sh.Len
	dstRelative := string(dstSlice)

	return srcRelative, dstRelative
}

// TransferSrcDstDetail returns the source and destination string for a transfer at given transferIndex in JobPartOrder
// Also indication of entity type since that's often necessary to avoid ambiguity about what the source and dest are
func (jpph *JobPartPlanHeader) TransferSrcDstStrings(transferIndex uint32) (source, destination string, isFolder bool) {
	srcRoot := string(jpph.SourceRoot[:jpph.SourceRootLength])
	srcExtraQuery := string(jpph.SourceExtraQuery[:jpph.SourceExtraQueryLength])
	dstRoot := string(jpph.DestinationRoot[:jpph.DestinationRootLength])
	dstExtraQuery := string(jpph.DestExtraQuery[:jpph.DestExtraQueryLength])

	jppt := jpph.Transfer(transferIndex)
	isFolder = jppt.EntityType == common.EEntityType.Folder()

	srcSlice := []byte{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&srcSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(jppt.SrcOffset) // Address of Job Part Plan + this transfer's src string offset
	sh.Len = int(jppt.SrcLength)
	sh.Cap = sh.Len
	srcRelative := string(srcSlice)

	dstSlice := []byte{}
	sh = (*reflect.SliceHeader)(unsafe.Pointer(&dstSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(jppt.SrcOffset) + uintptr(jppt.SrcLength) // Address of Job Part Plan + this transfer's src string offset + length of this transfer's src string
	sh.Len = int(jppt.DstLength)
	sh.Cap = sh.Len
	dstRelative := string(dstSlice)

	return common.GenerateFullPathWithQuery(srcRoot, srcRelative, srcExtraQuery),
		common.GenerateFullPathWithQuery(dstRoot, dstRelative, dstExtraQuery),
		isFolder
}

func (jpph *JobPartPlanHeader) getString(offset int64, length int16) string {
	tempSlice := []byte{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&tempSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(offset) // Address of Job Part Plan + this string's offset
	sh.Len = int(length)
	sh.Cap = sh.Len

	return string(tempSlice)
}

// TransferSrcPropertiesAndMetadata returns the SrcHTTPHeaders, properties and metadata for a transfer at given transferIndex in JobPartOrder
// TODO: Refactor return type to an object
func (jpph *JobPartPlanHeader) TransferSrcPropertiesAndMetadata(transferIndex uint32) (h common.ResourceHTTPHeaders, metadata common.Metadata, blobType azblob.BlobType, blobTier azblob.AccessTierType,
	s2sGetPropertiesInBackend bool, DestLengthValidation bool, s2sSourceChangeValidation bool, s2sInvalidMetadataHandleOption common.InvalidMetadataHandleOption, entityType common.EntityType, blobVersionID string, blobTags common.BlobTags) {
	var err error
	t := jpph.Transfer(transferIndex)

	s2sGetPropertiesInBackend = jpph.S2SGetPropertiesInBackend
	s2sSourceChangeValidation = jpph.S2SSourceChangeValidation
	s2sInvalidMetadataHandleOption = jpph.S2SInvalidMetadataHandleOption
	DestLengthValidation = jpph.DestLengthValidation

	offset := t.SrcOffset + int64(t.SrcLength) + int64(t.DstLength)

	entityType = t.EntityType

	if t.SrcContentTypeLength != 0 {
		h.ContentType = jpph.getString(offset, t.SrcContentTypeLength)
		offset += int64(t.SrcContentTypeLength)
	}
	if t.SrcContentEncodingLength != 0 {
		h.ContentEncoding = jpph.getString(offset, t.SrcContentEncodingLength)
		offset += int64(t.SrcContentEncodingLength)
	}
	if t.SrcContentLanguageLength != 0 {
		h.ContentLanguage = jpph.getString(offset, t.SrcContentLanguageLength)
		offset += int64(t.SrcContentLanguageLength)
	}
	if t.SrcContentDispositionLength != 0 {
		h.ContentDisposition = jpph.getString(offset, t.SrcContentDispositionLength)
		offset += int64(t.SrcContentDispositionLength)
	}
	if t.SrcCacheControlLength != 0 {
		h.CacheControl = jpph.getString(offset, t.SrcCacheControlLength)
		offset += int64(t.SrcCacheControlLength)
	}
	if t.SrcContentMD5Length != 0 {
		h.ContentMD5 = []byte(jpph.getString(offset, t.SrcContentMD5Length))
		offset += int64(t.SrcContentMD5Length)
	}
	if t.SrcMetadataLength != 0 {
		tmpMetaData := jpph.getString(offset, t.SrcMetadataLength)
		metadata, err = common.UnMarshalToCommonMetadata(tmpMetaData)
		common.PanicIfErr(err)
		offset += int64(t.SrcMetadataLength)
	}
	if t.SrcBlobTypeLength != 0 {
		tmpBlobTypeStr := []byte(jpph.getString(offset, t.SrcBlobTypeLength))
		blobType = azblob.BlobType(tmpBlobTypeStr)
		offset += int64(t.SrcBlobTypeLength)
	}
	if t.SrcBlobTierLength != 0 {
		tmpBlobTierStr := []byte(jpph.getString(offset, t.SrcBlobTierLength))
		blobTier = azblob.AccessTierType(tmpBlobTierStr)
		offset += int64(t.SrcBlobTierLength)
	}
	if t.SrcBlobVersionIDLength != 0 {
		blobVersionID = jpph.getString(offset, t.SrcBlobVersionIDLength)
		offset += int64(t.SrcBlobVersionIDLength)
	}
	if t.SrcBlobTagsLength != 0 {
		blobTagsString := jpph.getString(offset, t.SrcBlobTagsLength)
		blobTags = common.ToCommonBlobTagsMap(blobTagsString)
		offset += int64(t.SrcBlobTagsLength)
	}
	return
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// JobPartPlanDstBlob holds additional settings required when the destination is a blob
type JobPartPlanDstBlob struct {
	// Once set, the following fields are constants; they should never be modified

	BlobType common.BlobType
	// represents user decision to interpret the content-encoding from source file
	NoGuessMimeType bool

	// Specifies the length of MIME content type of the blob
	ContentTypeLength uint16

	// Specifies the MIME content type of the blob. The default type is application/octet-stream
	ContentType [CustomHeaderMaxBytes]byte

	// Specifies length of content encoding which have been applied to the blob.
	ContentEncodingLength uint16

	// Specifies which content encodings have been applied to the blob.
	ContentEncoding [CustomHeaderMaxBytes]byte

	// Specifies length of content language which has been applied to the blob.
	ContentLanguageLength uint16

	// Specifies which content language has been applied to the blob.
	ContentLanguage [CustomHeaderMaxBytes]byte

	// Specifies length of content disposition which has been applied to the blob.
	ContentDispositionLength uint16

	// Specifies the content disposition of the blob
	ContentDisposition [CustomHeaderMaxBytes]byte

	// Specifies the length of the cache control which has been applied to the blob.
	CacheControlLength uint16

	// Specifies the cache control of the blob
	CacheControl [CustomHeaderMaxBytes]byte

	// Specifies the tier if this is a block or page blob
	BlockBlobTier common.BlockBlobTier
	PageBlobTier  common.PageBlobTier

	// Controls uploading of MD5 hashes
	PutMd5 bool

	MetadataLength uint16
	Metadata       [MetadataMaxBytes]byte

	BlobTagsLength uint16
	BlobTags       [BlobTagsMaxByte]byte

	CpkInfo            bool
	IsSourceEncrypted  bool
	CpkScopeInfo       [CustomHeaderMaxBytes]byte
	CpkScopeInfoLength uint16

	// Specifies the maximum size of block which determines the number of chunks and chunk size of a transfer
	BlockSize int64
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// jobPartPlanDstLocal holds additional settings required when the destination is a local file
type JobPartPlanDstLocal struct {
	// Once set, the following fields are constants; they should never be modified

	// Specifies whether the timestamp of destination file has to be set to the modified time of source file
	PreserveLastModifiedTime bool

	// says how MD5 verification failures should be actioned
	MD5VerificationOption common.HashValidationOption
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// JobPartPlanTransfer represent the header of Job Part's Transfer in Memory Map File
type JobPartPlanTransfer struct {
	// Once set, the following fields are constants; they should never be modified

	// SrcOffset represents the actual start offset transfer header written in JobPartOrder file
	SrcOffset int64
	// SrcLength represents the actual length of source string for specific transfer
	SrcLength int16
	// DstLength represents the actual length of destination string for specific transfer
	DstLength int16
	// ChunkCount represents the num of chunks a transfer is split into
	//ChunkCount uint16	// TODO: Remove this, we need to determine it at runtime
	// EntityType indicates whether this is a file or a folder
	// We use a dedicated field for this because the alternative (of doing something fancy the names) was too complex and error-prone
	EntityType common.EntityType
	// ModifiedTime represents the last time at which source was modified before start of transfer stored as nanoseconds.
	ModifiedTime int64
	// SourceSize represents the actual size of the source on disk
	SourceSize int64
	// CompletionTime represents the time at which transfer was completed
	CompletionTime uint64

	// For S2S copy, per Transfer source's properties
	// TODO: ensure the length is enough
	SrcContentTypeLength        int16
	SrcContentEncodingLength    int16
	SrcContentLanguageLength    int16
	SrcContentDispositionLength int16
	SrcCacheControlLength       int16
	SrcContentMD5Length         int16
	SrcMetadataLength           int16
	SrcBlobTypeLength           int16
	SrcBlobTierLength           int16
	SrcBlobVersionIDLength      int16
	SrcBlobTagsLength           int16

	// Any fields below this comment are NOT constants; they may change over as the transfer is processed.
	// Care must be taken to read/write to these fields in a thread-safe way!

	// atomicTransferStatus represents the status of current transfer (TransferInProgress, TransferFailed or TransfersCompleted)
	// atomicTransferStatus should not be directly accessed anywhere except by transferStatus and setTransferStatus
	atomicTransferStatus common.TransferStatus

	// atomicErrorCode represents the storageError error code of the error with which the transfer got failed.
	// atomicErrorCode has a default value (0) which means either there was no error or transfer failed because some non storageError.
	// atomicErrorCode should not be directly accessed anywhere except by transferStatus and setTransferStatus
	atomicErrorCode int32

	// errorMessageLength represents the length of the error message for failed transfers.
	errorMessageLength int32
	errorMessage       [MaxErrorMessageLength]byte
}

// TransferStatus returns the transfer's status
func (jppt *JobPartPlanTransfer) TransferStatus() common.TransferStatus {
	return jppt.atomicTransferStatus.AtomicLoad()
}

// SetTransferStatus sets the transfer's status
// overWrite flags if set to true overWrites the failed status.
// If overWrite flag is set to false, then status of transfer is set to failed won't be overWritten.
// overWrite flag is used while resuming the failed transfers where the errorCode are set to default i.e 0
func (jppt *JobPartPlanTransfer) SetTransferStatus(status common.TransferStatus, overWrite bool) {
	if !overWrite {
		common.AtomicMorphInt32((*int32)(&jppt.atomicTransferStatus),
			func(startVal int32) (val int32, morphResult interface{}) {
				// start value < 0 means that transfer status is already a failed value.
				// If current transfer status has already failed value, then it will not be changed.
				return common.Iffint32(startVal < 0, startVal, int32(status)), nil
			})
	} else {
		(&jppt.atomicTransferStatus).AtomicStore(status)
	}
}

// ErrorCode returns the transfer's errorCode.
func (jppt *JobPartPlanTransfer) ErrorCode() int32 {
	return atomic.LoadInt32(&jppt.atomicErrorCode)
}

// ErrorMessage returns the transfer's error message.
func (jppt *JobPartPlanTransfer) ErrorMessage() string {
	return string(jppt.errorMessage[:atomic.LoadInt32(&jppt.errorMessageLength)])
}

// SetErrorMessage sets the error message if transfer failed.
// overWrite flags if set to true overWrites the errorMessage.
// If overWrite flag is set to false, then errorMessage won't be overwritten.
func (jppt *JobPartPlanTransfer) SetErrorMessage(errorMessage string, overwrite bool) {
	savedErrorMessageLength := atomic.LoadInt32(&jppt.errorMessageLength)
	currentErrorMessageLength := int32(len(errorMessage))

	// Make sure error message does not exceed max length.
	if currentErrorMessageLength > MaxErrorMessageLength {
		currentErrorMessageLength = MaxErrorMessageLength
	}

	// Overwrite, if this is the first error or caller wants this new errorMessage to overwrite the existing one.
	if (savedErrorMessageLength == 0) || overwrite {
		if atomic.CompareAndSwapInt32(&jppt.errorMessageLength, savedErrorMessageLength, currentErrorMessageLength) {
			copy(jppt.errorMessage[:], []byte(errorMessage[:currentErrorMessageLength]))
		}
	}
}

// SetErrorCode sets the error code of the error if transfer failed.
// overWrite flags if set to true overWrites the atomicErrorCode.
// If overWrite flag is set to false, then errorCode won't be overwritten.
func (jppt *JobPartPlanTransfer) SetErrorCode(errorCode int32, overwrite bool) {
	if !overwrite {
		common.AtomicMorphInt32(&jppt.atomicErrorCode,
			func(startErrorCode int32) (val int32, morphResult interface{}) {
				// startErrorCode != 0 means that error code is already set.
				// If current error code is already set to some error code, then it will not be changed.
				return common.Iffint32(startErrorCode != 0, startErrorCode, errorCode), nil
			})
	} else {
		atomic.StoreInt32(&jppt.atomicErrorCode, errorCode)
	}
}
