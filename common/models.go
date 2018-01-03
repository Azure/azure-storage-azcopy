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
	"time"
	"encoding/json"
)

// represents the raw input from the user
type CopyCmdArgsAndFlags struct {
	// from arguments
	Source      string
	Destination string

	// inferred from arguments
	SourceType LocationType
	DestinationType LocationType

	// filters from flags
	Include        string
	Exclude        string
	Recursive      bool
	FollowSymlinks bool
	WithSnapshots  bool

	// options from flags
	BlockSize                int
	BlobType                 string
	BlobTier                 string
	Metadata                 string
	ContentType              string
	ContentEncoding          string
	NoGuessMimeType          bool
	PreserveLastModifiedTime bool
	Acl                      string
}

// define the different types of sources/destinations
type LocationType string
const (
	Local LocationType = "local"
	Blob LocationType = "blob"
	Unknown LocationType = "unknown"
)

// This struct represent a single transfer entry with source and destination details
type CopyTask struct {
	Source string
	Destination string
	SourceLastModifiedTime time.Time
}

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrder struct {
	JobId string   // Guid - job identifier
	PartNumber int // part number of the job
	Version uint32 // version of the azcopy
	Priority uint32 // priority of the task
	IsFinalPart bool // to determine the final part for a specific job
	SourceType LocationType
	DestinationType LocationType
	TaskList []CopyTask
}

// This struct represents the required attribute for blob request header
type BlobData struct {
	ContentType string   //The content type specified for the blob.
	ContentEncoding string  //Specifies which content encodings have been applied to the blob.
	MetaData string   //User-defined name-value pairs associated with the blob
	NoGuessMimeType bool // upload
	PreserveLastModifiedTime bool // download
}

//This struct represents the require attribute in request header for BlockBlobTransfer
type BlockBlobData struct {
	JobBlobData BlobData
	JobBlockSize uint16
}

//This struct represents the require attribute in request header for PageBlobTransfer
type PageBlobData struct {
	JobBlobData BlobData
}

//This struct represents the require attribute in request header for AppendBlobTransfer
type AppendBlobData struct {
	JobBlobData BlobData
	BlockSize int
}

//This struct represents the Job Info for BlockBlob Transfer sent to Storage Engine
type JobPartToBlockBlob struct {
	JobPart CopyJobPartOrder
	Data BlockBlobData
}

//This struct represents the Job Info for PageBlob Transfer sent to Storage Engine
type JobPartToPageBlob struct {
	JobPart CopyJobPartOrder
	Data PageBlobData
}

//This struct represents the Job Info for AppendBlob Transfer sent to Storage Engine
type JobPartToAppendBlob struct {
	JobPart CopyJobPartOrder
	Data AppendBlobData
}

type JobPartToUnknown struct {
	JobPart CopyJobPartOrder
	Data json.RawMessage
}

type TransferJob struct {
	ChunkSize uint32 // TODO make type consistent

	// specify the source and its type
	Source     string
	SourceType LocationType

	// specify the destination and its type
	Destination     string
	DestinationType LocationType

	// testing purpose
	// count the number of chunks that are done
	Count uint32
	Id uint32

	ContentType string
	ContentEncodig string
	MetaData string
}