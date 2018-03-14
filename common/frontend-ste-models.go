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
	"time"
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

func ParseJobId(jobID string) (JobID, error) {
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

var EJobPriority = JobPriority{}

// JobPriority defines the transfer priorities supported by the Storage Transfer Engine's channels
// The default priority is Normal
type JobPriority EnumUint8

func (JobPriority) Normal() JobPriority { return JobPriority{0} }
func (JobPriority) Low() JobPriority    { return JobPriority{1} }
func (jp JobPriority) String() string {
	return EnumUint8(jp).String(reflect.TypeOf(jp))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EJobStatus = JobStatus{}

// JobStatus indicates the status of a Job; the default is InProgress.
type JobStatus EnumUint32 // Must be 32-bit for atomic operations

func (JobStatus) InProgress() JobStatus { return JobStatus{0} }
func (JobStatus) Paused() JobStatus     { return JobStatus{1} }
func (JobStatus) Cancelled() JobStatus  { return JobStatus{2} }
func (JobStatus) Completed() JobStatus  { return JobStatus{3} }
func (js JobStatus) String() string {
	return EnumUint32(js).String(reflect.TypeOf(js))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ELocation = Location{}

// define the different types of sources/destination locations
type Location EnumUint8

func (Location) Unknown() Location { return Location{0} }
func (Location) Local() Location   { return Location{1} }
func (Location) Blob() Location    { return Location{2} }
func (Location) File() Location    { return Location{3} }
func (l Location) String() string {
	return EnumUint8(l).String(reflect.TypeOf(l))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ETransferStatus = TransferStatus{}

type TransferStatus EnumInt32 // Must be 32-bit for atomic operations; negative #s represent a specific failure code

// Transfer is ready to transfer and not started transferring yet
func (TransferStatus) NotStarted() TransferStatus { return TransferStatus{0} }

// Transfer started & at least 1 chunk has successfully been transfered.
// Used to resume a transfer that started to avoid transfering all chunks thereby improving performance
func (TransferStatus) Started() TransferStatus { return TransferStatus{1} }

// Transfer successfully completed
func (TransferStatus) Success() TransferStatus { return TransferStatus{2} }

// Transfer failed due to some error. This status does represent the state when transfer is cancelled
func (TransferStatus) Failed() TransferStatus { return TransferStatus{-1} }

func (ts TransferStatus) ShouldTransfer() bool {
	return ts == TransferStatus{}.NotStarted() || ts == TransferStatus{}.Started()
}
func (ts TransferStatus) DidFail() bool { return ts.Value < 0 }

// Transfer is any of the three possible state (InProgress, Completer or Failed)
func (TransferStatus) All() TransferStatus { return TransferStatus{math.MaxInt8} }
func (ts TransferStatus) String() string {
	return EnumInt32(ts).String(reflect.TypeOf(ts))
}
func (ts TransferStatus) Parse(s string) (TransferStatus, error) {
	e, err := EnumInt32{}.Parse(reflect.TypeOf(ts), s, true)
	return TransferStatus(e), err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EBlobType = BlobType{}

type BlobType EnumUint8

func (BlobType) None() BlobType   { return BlobType{0} }
func (BlobType) Block() BlobType  { return BlobType{1} }
func (BlobType) Append() BlobType { return BlobType{2} }
func (BlobType) Page() BlobType   { return BlobType{3} }
func (bt BlobType) String() string {
	return EnumUint8(bt).String(reflect.TypeOf(bt))
}

func (bt BlobType) Parse(s string) (BlobType, error) {
	e, err := EnumUint8{}.Parse(reflect.TypeOf(bt), s, false)
	return BlobType(e), err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const (
	DefaultBlockBlobBlockSize  = 100 * 1024 * 1024
	DefaultAppendBlobBlockSize = 4 * 1024 * 1024
	DefaultPageBlobChunkSize   = 4 * 1024 * 1024
)

////////////////////////////////////////////////

// represents the raw copy command input from the user
type CopyCmdArgsAndFlags struct {
	// from arguments
	Source                string
	Destination           string
	BlobUrlForRedirection string

	// inferred from arguments
	SourceType      Location
	DestinationType Location

	// filters from flags
	Include        string
	Exclude        string
	Recursive      bool
	FollowSymlinks bool
	WithSnapshots  bool

	// options from flags
	BlockSize                uint32
	BlobType                 string
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
