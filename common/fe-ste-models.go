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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"math"
	"reflect"
	"sync/atomic"
	"time"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
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

type LogLevel byte

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
	return EnumHelper{}.StringInteger(uint8(jp), reflect.TypeOf(jp))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EJobStatus = JobStatus(0)

// JobStatus indicates the status of a Job; the default is InProgress.
type JobStatus uint32 // Must be 32-bit for atomic operations

func (j *JobStatus) Parse(s string) error {
	val, err := EnumHelper{}.Parse(reflect.TypeOf(j), s, true)
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
func (JobStatus) Cancelled() JobStatus  { return JobStatus(2) }
func (JobStatus) Completed() JobStatus  { return JobStatus(3) }
func (js JobStatus) String() string {
	return EnumHelper{}.StringInteger(uint32(js), reflect.TypeOf(js))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EFromTo = FromTo(0)

// FromTo defines the different types of sources/destination location combinations
type FromTo uint8

func (FromTo) Unknown() FromTo   { return FromTo(0) }
func (FromTo) LocalBlob() FromTo { return FromTo(1) }
func (FromTo) LocalFile() FromTo { return FromTo(2) }
func (FromTo) BlobLocal() FromTo { return FromTo(3) }
func (FromTo) FileLocal() FromTo { return FromTo(4) }
func (FromTo) BlobPipe() FromTo  { return FromTo(5) }
func (FromTo) PipeBlob() FromTo  { return FromTo(6) }
func (FromTo) FilePipe() FromTo  { return FromTo(7) }
func (FromTo) PipeFile() FromTo  { return FromTo(8) }
func (FromTo) BlobTrash() FromTo { return FromTo(9) }

func (ft FromTo) String() string {
	return EnumHelper{}.StringInteger(ft, reflect.TypeOf(ft))
}
func (ft *FromTo) Parse(s string) error {
	val, err := EnumHelper{}.Parse(reflect.TypeOf(ft), s, true)
	if err == nil {
		*ft = val.(FromTo)
	}
	return err
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

func (ts TransferStatus) ShouldTransfer() bool {
	return ts == ETransferStatus.NotStarted() || ts == ETransferStatus.Started()
}
func (ts TransferStatus) DidFail() bool { return ts < 0 }

// Transfer is any of the three possible state (InProgress, Completer or Failed)
func (TransferStatus) All() TransferStatus { return TransferStatus(math.MaxInt8) }
func (ts TransferStatus) String() string {
	return EnumHelper{}.StringInteger(ts, reflect.TypeOf(ts))
}
func (ts *TransferStatus) Parse(s string) error {
	val, err := EnumHelper{}.Parse(reflect.TypeOf(ts), s, false)
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
	return EnumHelper{}.StringInteger(bbt, reflect.TypeOf(bbt))
}

func (bbt *BlockBlobTier) Parse(s string) error {
	val, err := EnumHelper{}.Parse(reflect.TypeOf(bbt), s, true)
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
	return EnumHelper{}.String(pbt, reflect.TypeOf(pbt))
}

func (pbt *PageBlobTier) Parse(s string) error {
	val, err := EnumHelper{}.Parse(reflect.TypeOf(pbt), s, true)
	if err == nil {
		*pbt = val.(PageBlobTier)
	}
	return err
}

func (pbt PageBlobTier) ToAccessTierType() 	azblob.AccessTierType {
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

/*type BlockBlobTier azblob.AccessTierType

func (bt BlockBlobTier) Parse(s string) (BlockBlobTier, error) {
	if strings.EqualFold(s, "") {
		return BlockBlobTier(azblob.AccessTierNone), nil
	} else if strings.EqualFold(s, "Hot") {
		return BlockBlobTier(azblob.AccessTierHot), nil
	} else if strings.EqualFold(s, "Cool") {
		return BlockBlobTier(azblob.AccessTierCool), nil
	} else if strings.EqualFold(s, "Archive") {
		return BlockBlobTier(azblob.AccessTierArchive), nil
	} else {
		return "", fmt.Errorf("invalid block blob tier passed %s", s)
	}
}

func (bt BlockBlobTier) String() string {
	return string(bt)
}

// Implementing MarshalJSON() method for type BlockBlobTier.
func (bt BlockBlobTier) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(bt))
}

// Implementing UnmarshalJSON() method for type BlockBlobTier.
func (bt *BlockBlobTier) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	blockBlobTier, err := BlockBlobTier("").Parse(s)
	if err != nil {
		return err
	}
	*bt = blockBlobTier
	return nil
}

type PageBlobTier azblob.AccessTierType

func (pbt PageBlobTier) Parse(s string) (PageBlobTier, error) {
	if strings.EqualFold(s, "") {
		return PageBlobTier(azblob.AccessTierNone), nil
	} else if strings.EqualFold(s, "P10") {
		return PageBlobTier(azblob.AccessTierP10), nil
	} else if strings.EqualFold(s, "P20") {
		return PageBlobTier(azblob.AccessTierP20), nil
	} else if strings.EqualFold(s, "P30") {
		return PageBlobTier(azblob.AccessTierP30), nil
	} else if strings.EqualFold(s, "P4") {
		return PageBlobTier(azblob.AccessTierP4), nil
	} else if strings.EqualFold(s, "P40") {
		return PageBlobTier(azblob.AccessTierP40), nil
	} else if strings.EqualFold(s, "P50") {
		return PageBlobTier(azblob.AccessTierP50), nil
	} else if strings.EqualFold(s, "P6") {
		return PageBlobTier(azblob.AccessTierP6), nil
	} else {
		return " ", fmt.Errorf("failed to parse user given blob tier %s", s)
	}
}

func (pbt PageBlobTier) String() string {
	return string(pbt)
}

// Implementing MarshalJSON() method for type PageBlobTier.
func (pbt PageBlobTier) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(pbt))
}

// Implementing UnmarshalJSON() method for type PageBlobTier.
func (pbt *PageBlobTier) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	psgeBlobTier, err := PageBlobTier("").Parse(s)
	if err != nil {
		return err
	}
	*pbt = psgeBlobTier
	return nil
}
*/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const (
	DefaultBlockBlobBlockSize = 100 * 1024 * 1024
	//DefaultAppendBlobBlockSize = 4 * 1024 * 1024
	DefaultPageBlobChunkSize  = 4 * 1024 * 1024
	DefaultAzureFileChunkSize = 4 * 1024 * 1024
)

////////////////////////////////////////////////

// represents the raw copy command input from the user
type CopyCmdArgsAndFlags struct {
	// from arguments
	Source                string
	Destination           string
	BlobUrlForRedirection string

	// inferred from arguments
	fromTo FromTo

	// filters from flags
	Include        string
	Exclude        string
	Recursive      bool
	FollowSymlinks bool
	WithSnapshots  bool

	// options from flags
	BlockSize                uint32
	BlobType                 string //TODO: remeber to delete this
	BlobTier                 string
	Metadata                 string
	ContentType              string
	ContentEncoding          string
	NoGuessMimeType          bool
	PreserveLastModifiedTime bool
	IsaBackgroundOp          bool
	Acl                      string
	LogVerbosity             byte
}

// This struct represent a single transfer entry with source and destination details
type CopyTransfer struct {
	Source           string
	Destination      string
	LastModifiedTime time.Time //represents the last modified time of source which ensures that source hasn't changed while transferring
	SourceSize       int64     // size of the source entity in bytes
}
