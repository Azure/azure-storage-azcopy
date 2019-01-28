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

package ste

import (
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// Abstraction of the methods needed to copy one file from URL to a remote location
type s2sCopier interface {

	// ChunkSize returns the chunk size that should be used
	ChunkSize() uint32

	// NumChunks returns the number of chunks that will be required for the target file
	NumChunks() uint32

	// RemoteFileExists is called to see whether the file already exists at the remote location (so we know whether we'll be overwriting it)
	RemoteFileExists() (bool, error)

	// Prologue is called automatically before the first chunkFunc is generated.
	// Implementation should do any initialization that is necessary - e.g.
	// creating the remote file for those destinations that require an explicit
	// creation step. Implementations should call jptm.FailActiveS2SCopy if anything
	// goes wrong during the prologue.
	Prologue()

	// GenerateCopyFunc returns a func() that will copy the specified portion of the source URL file to the remote location.
	GenerateCopyFunc(chunkID common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc

	// Epilogue will be called automatically once we know all the chunk funcs have been processed.
	// Implementation should interact with its jptm to do
	// post-success processing if transfer has been successful so far,
	// or post-failure processing otherwise.  Implementations should
	// use jptm.FailActiveS2SCopy if anything fails during the epilogue.
	Epilogue()
}

type s2sCopierFactory func(jptm IJobPartTransferMgr, source string, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error)

func getNumCopyChunks(fileSize int64, chunkSize uint32) uint32 {
	numChunks := uint32(1) // as uploads, for s2s copies, we always map zero-size source files to ONE (empty) chunk
	if fileSize > 0 {
		chunkSizeI := int64(chunkSize)
		numChunks = common.Iffuint32(
			fileSize%chunkSizeI == 0,
			uint32(fileSize/chunkSizeI),
			uint32(fileSize/chunkSizeI)+1)
	}
	return numChunks
}

func createCopyChunkFunc(jptm IJobPartTransferMgr, id common.ChunkID, body func()) chunkFunc {
	// For s2s copy, we set the chunk status to done as soon as the chunkFunc completes.
	return createChunkFunc(true, jptm, id, body)
}

// By default presign expires after 7 days, which is considered enough for large amounts of files transfer.
// This value could be further tuned, or exposed to user for customization, according to user feedback.
const defaultPresignExpires = time.Hour * 7 * 24

var s3ClientFactory = common.NewS3ClientFactory()

func prepareSourceURL(jptm IJobPartTransferMgr, source string) (*url.URL, error) {
	u, err := url.Parse(source)
	if err != nil {
		return nil, err
	}

	fromTo := jptm.FromTo()
	// If it's S3 source, presign the source.
	if fromTo == common.EFromTo.S3Blob() {
		s3URLPart, err := common.NewS3URLParts(*u)
		if err != nil {
			return nil, err
		}

		s3Client, err := s3ClientFactory.GetS3Client(
			jptm.Context(),
			common.CredentialInfo{
				CredentialType: common.ECredentialType.S3AccessKey(),
				S3CredentialInfo: common.S3CredentialInfo{
					Endpoint: s3URLPart.Endpoint,
					Region:   s3URLPart.Region,
				},
			},
			common.CredentialOpOptions{
				LogInfo:  func(str string) { jptm.Log(pipeline.LogInfo, str) },
				LogError: func(str string) { jptm.Log(pipeline.LogError, str) },
				Panic:    jptm.Panic,
			})
		if err != nil {
			return nil, err
		}

		return s3Client.PresignedGetObject(s3URLPart.BucketName, s3URLPart.ObjectKey, defaultPresignExpires, url.Values{})
	}

	return u, err
}
