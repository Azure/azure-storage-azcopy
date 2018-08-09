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

package common

import (
	"encoding/json"
	"math"
	"reflect"
	"sync/atomic"
	"time"

	"fmt"

	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/JeffreyRichter/enum/enum"
)

const (
	AZCOPY_PATH_SEPARATOR_STRING = "/"
	AZCOPY_PATH_SEPARATOR_CHAR   = '/'
	OS_PATH_SEPARATOR            = string(os.PathSeparator)
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type JobID UUID

func NewJobID() JobID {
	return JobID(NewUUID())
}

//var EmptyJobId JobID = JobID{}
func (j JobID) IsEmpty() bool {
	return j == JobID{}
}

func ParseJobID(jobID string) (JobID, error) {
	uuid, err := ParseUUID(jobID)
	if err != nil {
		return JobID{}, err
	}
	return JobID(uuid), nil
}

func (j JobID) String() string {
	return UUID(j).String()
}

// Implementing MarshalJSON() method for type JobID
func (j JobID) MarshalJSON() ([]byte, error) {
	return json.Marshal(UUID(j))
}

// Implementing UnmarshalJSON() method for type JobID
func (j *JobID) UnmarshalJSON(b []byte) error {
	var u UUID
	if err := json.Unmarshal(b, &u); err != nil {
		return err
	}
	*j = JobID(u)
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type PartNumber uint32
type Version uint32
type Status uint32

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type OutputFormat uint32

var EOutputFormat = OutputFormat(0)

func (OutputFormat) Text() OutputFormat { return OutputFormat(0) }
func (OutputFormat) Json() OutputFormat { return OutputFormat(1) }

func (of *OutputFormat) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(of), s, true)
	if err == nil {
		*of = val.(OutputFormat)
	}
	return err
}

var EExitCode = ExitCode(0)

type ExitCode uint32

func (ExitCode) Success() ExitCode { return ExitCode(0) }
func (ExitCode) Error() ExitCode   { return ExitCode(1) }

type LogLevel uint8

var ELogLevel = LogLevel(pipeline.LogNone)

func (LogLevel) None() LogLevel    { return LogLevel(pipeline.LogNone) }
func (LogLevel) Fatal() LogLevel   { return LogLevel(pipeline.LogFatal) }
func (LogLevel) Panic() LogLevel   { return LogLevel(pipeline.LogPanic) }
func (LogLevel) Error() LogLevel   { return LogLevel(pipeline.LogError) }
func (LogLevel) Warning() LogLevel { return LogLevel(pipeline.LogWarning) }
func (LogLevel) Info() LogLevel    { return LogLevel(pipeline.LogInfo) }
func (LogLevel) Debug() LogLevel   { return LogLevel(pipeline.LogDebug) }

func (ll *LogLevel) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ll), s, true, true)
	if err == nil {
		*ll = val.(LogLevel)
	}
	return err
}

func (ll LogLevel) String() string {
	switch ll {
	case ELogLevel.None():
		return "NONE"
	case ELogLevel.Fatal():
		return "FATAL"
	case ELogLevel.Panic():
		return "PANIC"
	case ELogLevel.Error():
		return "ERR"
	case ELogLevel.Warning():
		return "WARN"
	case ELogLevel.Info():
		return "INFO"
	case ELogLevel.Debug():
		return "DBG"
	default:
		return enum.StringInt(ll, reflect.TypeOf(ll))
	}
}

func (ll LogLevel) ToPipelineLogLevel() pipeline.LogLevel {
	// This assumes that pipeline's LogLevel values can fit in a byte (which they can)
	return pipeline.LogLevel(ll)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EJobPriority = JobPriority(0)

// JobPriority defines the transfer priorities supported by the Storage Transfer Engine's channels
// The default priority is Normal
type JobPriority uint8

func (JobPriority) Normal() JobPriority { return JobPriority(0) }
func (JobPriority) Low() JobPriority    { return JobPriority(1) }
func (jp JobPriority) String() string {
	return enum.StringInt(uint8(jp), reflect.TypeOf(jp))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EJobStatus = JobStatus(0)

// JobStatus indicates the status of a Job; the default is InProgress.
type JobStatus uint32 // Must be 32-bit for atomic operations

func (j *JobStatus) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(j), s, true, true)
	if err == nil {
		*j = val.(JobStatus)
	}
	return err
}

// Implementing MarshalJSON() method for type JobStatus
func (j JobStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.String())
}

// Implementing UnmarshalJSON() method for type JobStatus
func (j *JobStatus) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return j.Parse(s)
}

func (j *JobStatus) AtomicLoad() JobStatus {
	return JobStatus(atomic.LoadUint32((*uint32)(j)))
}

func (j *JobStatus) AtomicStore(newJobStatus JobStatus) {
	atomic.StoreUint32((*uint32)(j), uint32(newJobStatus))
}

func (JobStatus) InProgress() JobStatus { return JobStatus(0) }
func (JobStatus) Paused() JobStatus     { return JobStatus(1) }
func (JobStatus) Cancelling() JobStatus { return JobStatus(2) }
func (JobStatus) Cancelled() JobStatus  { return JobStatus(3) }
func (JobStatus) Completed() JobStatus  { return JobStatus(4) }
func (js JobStatus) String() string {
	return enum.StringInt(js, reflect.TypeOf(js))
}

////////////////////////////////////////////////////////////////

var ELocation = Location(0)

// Location indicates the type of Location
type Location uint8

func (Location) Unknown() Location { return Location(0) }
func (Location) Local() Location   { return Location(1) }
func (Location) Pipe() Location    { return Location(2) }
func (Location) Blob() Location    { return Location(3) }
func (Location) File() Location    { return Location(4) }
func (Location) BlobFS() Location  { return Location(5) }
func (l Location) String() string {
	return enum.StringInt(uint32(l), reflect.TypeOf(l))
}

// fromToValue returns the fromTo enum value for given
// from / To location combination. In 16 bits fromTo
// value, first 8 bits represents from location
func fromToValue(from Location, to Location) FromTo {
	return FromTo((FromTo(from) << 8) | FromTo(to))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EFromTo = FromTo(0)

// FromTo defines the different types of sources/destination location combinations
// FromTo is 16 bit where first 8 bit represents the from location and other 8 bits
// represents the to location
type FromTo uint16

func (FromTo) Unknown() FromTo     { return FromTo(0) }
func (FromTo) LocalBlob() FromTo   { return FromTo(fromToValue(ELocation.Local(), ELocation.Blob())) }
func (FromTo) LocalFile() FromTo   { return FromTo(fromToValue(ELocation.Local(), ELocation.File())) }
func (FromTo) BlobLocal() FromTo   { return FromTo(fromToValue(ELocation.Blob(), ELocation.Local())) }
func (FromTo) FileLocal() FromTo   { return FromTo(fromToValue(ELocation.File(), ELocation.Local())) }
func (FromTo) BlobPipe() FromTo    { return FromTo(fromToValue(ELocation.Blob(), ELocation.Pipe())) }
func (FromTo) PipeBlob() FromTo    { return FromTo(fromToValue(ELocation.Pipe(), ELocation.Blob())) }
func (FromTo) FilePipe() FromTo    { return FromTo(fromToValue(ELocation.File(), ELocation.Pipe())) }
func (FromTo) PipeFile() FromTo    { return FromTo(fromToValue(ELocation.Pipe(), ELocation.File())) }
func (FromTo) BlobTrash() FromTo   { return FromTo(fromToValue(ELocation.Blob(), ELocation.Unknown())) }
func (FromTo) FileTrash() FromTo   { return FromTo(fromToValue(ELocation.File(), ELocation.Unknown())) }
func (FromTo) LocalBlobFS() FromTo { return FromTo(fromToValue(ELocation.Local(), ELocation.BlobFS())) }
func (FromTo) BlobFSLocal() FromTo { return FromTo(fromToValue(ELocation.BlobFS(), ELocation.Local())) }
func (FromTo) BlobBlob() FromTo    { return FromTo(fromToValue(ELocation.Blob(), ELocation.Blob())) }
func (FromTo) FileBlob() FromTo    { return FromTo(fromToValue(ELocation.File(), ELocation.Blob())) }

func (ft FromTo) String() string {
	return enum.StringInt(ft, reflect.TypeOf(ft))
}
func (ft *FromTo) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ft), s, true, true)
	if err == nil {
		*ft = val.(FromTo)
	}
	return err
}

func (ft *FromTo) FromAndTo(s string) (srcLocation, dstLocation Location, err error) {
	srcLocation = ELocation.Unknown()
	dstLocation = ELocation.Unknown()
	val, err := enum.ParseInt(reflect.TypeOf(ft), s, true, true)
	if err == nil {
		dstLocation = Location(((1 << 8) - 1) & val.(FromTo))
		srcLocation = Location((((1 << 16) - 1) & val.(FromTo)) >> 8)
		return
	}
	err = fmt.Errorf("unable to parse the from and to Location from given FromTo %s", s)
	return
}

func (ft *FromTo) To() Location {
	return Location(((1 << 8) - 1) & *ft)
}

func (ft *FromTo) From() Location {
	return Location((((1 << 16) - 1) & *ft) >> 8)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ETransferStatus = TransferStatus(0)

type TransferStatus int32 // Must be 32-bit for atomic operations; negative #s represent a specific failure code

// Transfer is ready to transfer and not started transferring yet
func (TransferStatus) NotStarted() TransferStatus { return TransferStatus(0) }

// Transfer started & at least 1 chunk has successfully been transfered.
// Used to resume a transfer that started to avoid transfering all chunks thereby improving performance
func (TransferStatus) Started() TransferStatus { return TransferStatus(1) }

// Transfer successfully completed
func (TransferStatus) Success() TransferStatus { return TransferStatus(2) }

// Transfer failed due to some error. This status does represent the state when transfer is cancelled
func (TransferStatus) Failed() TransferStatus { return TransferStatus(-1) }

// Transfer failed due to failure while Setting blob tier.
func (TransferStatus) BlobTierFailure() TransferStatus { return TransferStatus(-2) }

func (TransferStatus) BlobAlreadyExistsFailure() TransferStatus { return TransferStatus(-3) }

func (TransferStatus) FileAlreadyExistsFailure() TransferStatus { return TransferStatus(-4) }

func (ts TransferStatus) ShouldTransfer() bool {
	return ts == ETransferStatus.NotStarted() || ts == ETransferStatus.Started()
}
func (ts TransferStatus) DidFail() bool { return ts < 0 }

// Transfer is any of the three possible state (InProgress, Completer or Failed)
func (TransferStatus) All() TransferStatus { return TransferStatus(math.MaxInt8) }
func (ts TransferStatus) String() string {
	return enum.StringInt(ts, reflect.TypeOf(ts))
}
func (ts *TransferStatus) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ts), s, false, true)
	if err == nil {
		*ts = val.(TransferStatus)
	}
	return err
}

func (ts *TransferStatus) AtomicLoad() TransferStatus {
	return TransferStatus(atomic.LoadInt32((*int32)(ts)))
}
func (ts *TransferStatus) AtomicStore(newTransferStatus TransferStatus) {
	atomic.StoreInt32((*int32)(ts), int32(newTransferStatus))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EBlockBlobTier = BlockBlobTier(0)

type BlockBlobTier uint8

func (BlockBlobTier) None() BlockBlobTier    { return BlockBlobTier(0) }
func (BlockBlobTier) Hot() BlockBlobTier     { return BlockBlobTier(1) }
func (BlockBlobTier) Cold() BlockBlobTier    { return BlockBlobTier(2) }
func (BlockBlobTier) Cool() BlockBlobTier    { return BlockBlobTier(3) }
func (BlockBlobTier) Archive() BlockBlobTier { return BlockBlobTier(4) }

func (bbt BlockBlobTier) String() string {
	return enum.StringInt(bbt, reflect.TypeOf(bbt))
}

func (bbt *BlockBlobTier) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(bbt), s, true, true)
	if err == nil {
		*bbt = val.(BlockBlobTier)
	}
	return err
}

func (bbt BlockBlobTier) ToAccessTierType() azblob.AccessTierType {
	return azblob.AccessTierType(bbt.String())
}

func (bbt BlockBlobTier) MarshalJSON() ([]byte, error) {
	return json.Marshal(bbt.String())
}

// Implementing UnmarshalJSON() method for type BlockBlobTier.
func (bbt *BlockBlobTier) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return bbt.Parse(s)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EPageBlobTier = PageBlobTier(0)

type PageBlobTier uint8

func (PageBlobTier) None() PageBlobTier { return PageBlobTier(0) }
func (PageBlobTier) P10() PageBlobTier  { return PageBlobTier(10) }
func (PageBlobTier) P20() PageBlobTier  { return PageBlobTier(20) }
func (PageBlobTier) P30() PageBlobTier  { return PageBlobTier(30) }
func (PageBlobTier) P4() PageBlobTier   { return PageBlobTier(4) }
func (PageBlobTier) P40() PageBlobTier  { return PageBlobTier(40) }
func (PageBlobTier) P50() PageBlobTier  { return PageBlobTier(50) }
func (PageBlobTier) P6() PageBlobTier   { return PageBlobTier(6) }

func (pbt PageBlobTier) String() string {
	return enum.StringInt(pbt, reflect.TypeOf(pbt))
}

func (pbt *PageBlobTier) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(pbt), s, true, true)
	if err == nil {
		*pbt = val.(PageBlobTier)
	}
	return err
}

func (pbt PageBlobTier) ToAccessTierType() azblob.AccessTierType {
	return azblob.AccessTierType(pbt.String())
}

func (pbt PageBlobTier) MarshalJSON() ([]byte, error) {
	return json.Marshal(pbt.String())
}

// Implementing UnmarshalJSON() method for type BlockBlobTier.
func (pbt *PageBlobTier) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return pbt.Parse(s)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ECredentialType = CredentialType(0)

// CredentialType defines the different types of credentials
type CredentialType uint8

func (CredentialType) Unknown() CredentialType    { return CredentialType(0) }
func (CredentialType) OAuthToken() CredentialType { return CredentialType(1) }
func (CredentialType) Anonymous() CredentialType  { return CredentialType(2) } // For SAS or public.
func (CredentialType) SharedKey() CredentialType  { return CredentialType(3) }

func (ct CredentialType) String() string {
	return enum.StringInt(ct, reflect.TypeOf(ct))
}
func (ct *CredentialType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ct), s, true, true)
	if err == nil {
		*ct = val.(CredentialType)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
const (
	DefaultBlockBlobBlockSize = 8 * 1024 * 1024
	//DefaultAppendBlobBlockSize = 4 * 1024 * 1024
	DefaultPageBlobChunkSize  = 4 * 1024 * 1024
	DefaultAzureFileChunkSize = 4 * 1024 * 1024
	MaxNumberOfBlocksPerBlob  = 50000
)

// This struct represent a single transfer entry with source and destination details
type CopyTransfer struct {
	Source           string
	Destination      string
	LastModifiedTime time.Time //represents the last modified time of source which ensures that source hasn't changed while transferring
	SourceSize       int64     // size of the source entity in bytes.

	// Properties for service to service copy
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ContentLanguage    string
	CacheControl       string
	ContentMD5         []byte
	Metadata           Metadata

	// Properties for blob copy only
	BlobType azblob.BlobType
	//BlobTier           string //TODO
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Metadata used in AzCopy.
type Metadata map[string]string

// ToAzBlobMetadata converts metadata to azblob's metadata.
func (m Metadata) ToAzBlobMetadata() azblob.Metadata {
	return azblob.Metadata(m)
}

// ToAzFileMetadata converts metadata to azfile's metadata.
func (m Metadata) ToAzFileMetadata() azfile.Metadata {
	return azfile.Metadata(m)
}

// FromAzBlobMetadataToCommonMetadata converts azblob's metadata to common metadata.
func FromAzBlobMetadataToCommonMetadata(m azblob.Metadata) Metadata {
	return Metadata(m)
}

// FromAzFileMetadataToCommonMetadata converts azfile's metadata to common metadata.
func FromAzFileMetadataToCommonMetadata(m azfile.Metadata) Metadata {
	return Metadata(m)
}

// Marshal marshals metadata to string.
func (m Metadata) Marshal() (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// UnMarshalToCommonMetadata unmarshals string to common metadata.
func UnMarshalToCommonMetadata(metadataString string) (Metadata, error) {
	var result Metadata
	if metadataString != "" {
		err := json.Unmarshal([]byte(metadataString), &result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
