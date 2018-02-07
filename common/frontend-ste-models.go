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
	"errors"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"time"
	"github.com/Azure/azure-storage-azcopy/ste"
)

type JobID uuid
type PartNumber uint32
type Version uint32
type Status uint32

// represents the raw copy command input from the user
type CopyCmdArgsAndFlags struct {
	// from arguments
	Source      string
	Destination string

	// inferred from arguments
	SourceType      LocationType
	DestinationType LocationType

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
	LogVerbosity             uint8
}

// ListCmdArgsAndFlags represents the raw list command input from the user
type ListCmdArgsAndFlags struct {
	JobId          string
	TransferStatus string
}

// define the different types of sources/destinations
type LocationType uint8

const (
	Local   LocationType = 0
	Blob    LocationType = 1
	Unknown LocationType = 2
)

// This struct represent a single transfer entry with source and destination details
type CopyTransfer struct {
	Source           string
	Destination      string
	LastModifiedTime time.Time //represents the last modified time of source which ensures that source hasn't changed while transferring
	SourceSize       int64     // size of the source entity in bytes
}

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrder struct {
	Version            uint32     // version of the azcopy
	ID                 string      // Guid - job identifier    //todo use uuid from go sdk
	PartNum            PartNumber // part number of the job
	IsFinalPart        bool       // to determine the final part for a specific job
	Priority           uint8      // priority of the task
	SourceType         LocationType
	DestinationType    LocationType
	Transfers          []CopyTransfer
	LogVerbosity       pipeline.LogLevel
	IsaBackgroundOp    bool
	OptionalAttributes BlobTransferAttributes
}

// represents the raw list command input from the user when requested the list of transfer with given status for given JobId
type ListJobPartsTransfers struct {
	JobId                  string
	ExpectedTransferStatus Status
}

// This struct represents the optional attribute for blob request header
type BlobTransferAttributes struct {
	ContentType              string //The content type specified for the blob.
	ContentEncoding          string //Specifies which content encodings have been applied to the blob.
	Metadata                 string //User-defined name-value pairs associated with the blob
	NoGuessMimeType          bool   // represents user decision to interpret the content-encoding from source file
	PreserveLastModifiedTime bool   // when downloading, tell engine to set file's timestamp to timestamp of blob
	BlockSizeinBytes         uint32
}

// ExistingJobDetails represent the Job with JobId and
type ExistingJobDetails struct {
	JobIds []string
}

// represents the JobProgress Summary response for list command when requested the Job Progress Summary for given JobId
type JobProgressSummary struct {
	CompleteJobOrdered             bool
	JobStatus                      ste.JobStatusCode
	TotalNumberOfTransfer          uint32
	TotalNumberofTransferCompleted uint32
	TotalNumberofFailedTransfer    uint32
	//NumberOfTransferCompletedafterCheckpoint uint32
	//NumberOfTransferFailedAfterCheckpoint    uint32
	PercentageProgress          uint32
	FailedTransfers             []TransferDetail
	ThroughputInBytesPerSeconds float64
}

// represents the Details and details of a single transfer
type TransferDetail struct {
	Src            string
	Dst            string
	TransferStatus Status
}

// represents the list of Details and details of number of transfers
type TransfersDetail struct {
	Details []TransferDetail
}

const (
	StatusCompleted  = 1
	StatusInProgress = 2
)

const DefaultBlockSize = 4 * 1024 * 1024
