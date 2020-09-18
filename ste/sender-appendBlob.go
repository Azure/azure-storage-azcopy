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
	"context"
	"net/url"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"golang.org/x/sync/semaphore"

	"github.com/Azure/azure-storage-azcopy/common"
)

type appendBlobSenderBase struct {
	jptm              IJobPartTransferMgr
	destAppendBlobURL azblob.AppendBlobURL
	chunkSize         int64
	numChunks         uint32
	pacer             pacer
	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  azblob.BlobHTTPHeaders
	metadataToApply azblob.Metadata

	soleChunkFuncSemaphore *semaphore.Weighted
}

type appendBlockFunc = func()

func newAppendBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider ISourceInfoProvider) (*appendBlobSenderBase, error) {
	transferInfo := jptm.Info()

	// compute chunk count
	chunkSize := transferInfo.BlockSize
	// If the given chunk Size for the Job is greater than maximum append blob block size i.e 4 MB,
	// then set chunkSize as 4 MB.
	chunkSize = common.Iffint64(
		chunkSize > common.MaxAppendBlobBlockSize,
		common.MaxAppendBlobBlockSize,
		chunkSize)

	srcSize := transferInfo.SourceSize
	numChunks := getNumChunks(srcSize, chunkSize)

	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destAppendBlobURL := azblob.NewAppendBlobURL(*destURL, p)

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	return &appendBlobSenderBase{
		jptm:                   jptm,
		destAppendBlobURL:      destAppendBlobURL,
		chunkSize:              chunkSize,
		numChunks:              numChunks,
		pacer:                  pacer,
		headersToApply:         props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		metadataToApply:        props.SrcMetadata.ToAzBlobMetadata(),
		soleChunkFuncSemaphore: semaphore.NewWeighted(1)}, nil
}

func (s *appendBlobSenderBase) SendableEntityType() common.EntityType {
	return common.EEntityType.File()
}

func (s *appendBlobSenderBase) ChunkSize() int64 {
	return s.chunkSize
}

func (s *appendBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *appendBlobSenderBase) RemoteFileExists() (bool, time.Time, error) {
	return remoteObjectExists(s.destAppendBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{}))
}

// Returns a chunk-func for sending append blob to remote
func (s *appendBlobSenderBase) generateAppendBlockToRemoteFunc(id common.ChunkID, appendBlock appendBlockFunc) chunkFunc {
	// Copy must be totally sequential for append blobs
	// The way we enforce that is simple: we won't even CREATE
	// a chunk func, until all previously-scheduled chunk funcs have completed
	// Here we block until there are no other chunkfuncs in existence for this blob
	err := s.soleChunkFuncSemaphore.Acquire(s.jptm.Context(), 1)
	if err != nil {
		// Must have been cancelled
		// We must still return a chunk func, so return a no-op one
		return createSendToRemoteChunkFunc(s.jptm, id, func() {})
	}

	return createSendToRemoteChunkFunc(s.jptm, id, func() {

		// Here, INSIDE the chunkfunc, we release the semaphore when we have finished running
		defer s.soleChunkFuncSemaphore.Release(1)

		jptm := s.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		appendBlock()
	})
}

func (s *appendBlobSenderBase) Prologue(ps common.PrologueState) (destinationModified bool) {
	// sometimes, specifically when reading local files, we have more info
	// about the file type at this time than what we had before
	s.headersToApply.ContentType = ps.GetInferredContentType(s.jptm)

	destinationModified = true
	_, err := s.destAppendBlobURL.Create(s.jptm.Context(), s.headersToApply, s.metadataToApply, azblob.BlobAccessConditions{})
	if err != nil {
		s.jptm.FailActiveSend("Creating blob", err)
		return
	}
	return
}

func (s *appendBlobSenderBase) Epilogue() {
	// Empty function because you don't have to commit on an append blob
}

func (s *appendBlobSenderBase) Cleanup() {
	jptm := s.jptm
	// Cleanup
	if jptm.IsDeadInflight() {
		// There is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: particularly, given that this is an APPEND blob, do we really need to delete it?  But if we don't delete it,
		//   it will still be in an ambiguous situation with regard to how much has been added to it.  Probably best to delete
		//   to be consistent with other
		deletionContext, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFunc()
		_, err := s.destAppendBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(s.destAppendBlobURL.String(), "Delete (incomplete) Append Blob ", err)
		}
	}
}
