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

// represents a single copy task
// source and destination are both entities (file, blob)
type CopyTask struct {
	Source string
	Destination string
}

// represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrder struct {
	// job identifier
	JobId string
	PartNumber int
	Version uint32
	Priority uint32
	IsFinalPart bool

	//TODO set these!!
	// job metadata
	SourceType LocationType
	DestinationType LocationType

	// flags
	BlockSize int // upload

	ContentType string // upload
	ContentEncoding string // upload
	Metadata string // upload
	NoGuessMimeType bool // upload
	PreserveLastModifiedTime bool // download

	Acl string // ignore for now
	BlobTier string // ignore for now
	DestinationBlobType string // ignore for now

	// source and destination pairs
	TaskList []CopyTask
}
