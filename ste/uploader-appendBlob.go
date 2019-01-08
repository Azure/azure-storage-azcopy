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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"golang.org/x/sync/semaphore"
	"net/url"
	"time"
)

type appendBlobUploader struct {
	jptm                   IJobPartTransferMgr
	appendBlobUrl          azblob.AppendBlobURL
	chunkSize              uint32
	numChunks              uint32
	pipeline               pipeline.Pipeline
	pacer                  *pacer
	soleChunkFuncSemaphore *semaphore.Weighted
}

func newAppendBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error) {
	// compute chunk count
	info := jptm.Info()
	fileSize := info.SourceSize
	chunkSize := info.BlockSize

	// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
	// then set maximum pageSize will be 4 MB.
	chunkSize = common.Iffuint32(
		chunkSize > common.MaxAppendBlobBlockSize,
		common.MaxAppendBlobBlockSize,
		chunkSize)

	numChunks := getNumUploadChunks(fileSize, chunkSize)

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	return &appendBlobUploader{
		jptm:          jptm,
		appendBlobUrl: azblob.NewBlobURL(*destURL, p).ToAppendBlobURL(),
		chunkSize:     chunkSize,
		numChunks:     numChunks,
		pipeline:      p,
		pacer:         pacer,
		soleChunkFuncSemaphore: semaphore.NewWeighted(1),
	}, nil
}

func (u *appendBlobUploader) ChunkSize() uint32 {
	return u.chunkSize
}

func (u *appendBlobUploader) NumChunks() uint32 {
	return u.numChunks
}

func (u *appendBlobUploader) RemoteFileExists() (bool, error) {
	_, err := u.appendBlobUrl.GetProperties(u.jptm.Context(), azblob.BlobAccessConditions{})
	return err == nil, nil
}

func (u *appendBlobUploader) Prologue(leadingBytes []byte) {
	jptm := u.jptm
	info := jptm.Info()

	blobHTTPHeaders, metaData := jptm.BlobDstData(leadingBytes)
	_, err := u.appendBlobUrl.Create(jptm.Context(), blobHTTPHeaders, metaData, azblob.BlobAccessConditions{})
	if err != nil {
		status, msg := ErrorEx{err}.ErrorCodeAndString()
		jptm.LogUploadError(info.Source, info.Destination, "Blob Create Error "+msg, status)
		jptm.FailActiveUpload(err)
		return
	}
}

func (u *appendBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	// Uploads must be totally sequential for append blobs
	// The way we enforce that is simple: we won't even CREATE
	// a chunk func, until all previously-scheduled chunk funcs have completed
	// Here we block until there are no other chunkfuncs in existence for this blob
	err := u.soleChunkFuncSemaphore.Acquire(u.jptm.Context(), 1)
	if err != nil {
		// Must have been cancelled
		// We must still return a chunk func, so return a no-op one
		return createUploadChunkFunc(u.jptm, id, func() {})
	}

	return createUploadChunkFunc(u.jptm, id, func() {

		// Here, INSIDE the chunkfunc, we release the semaphore when we have finished running
		defer u.soleChunkFuncSemaphore.Release(1)

		jptm := u.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err = u.appendBlobUrl.AppendBlock(jptm.Context(), body, azblob.AppendBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveUpload(err)
			return
		}
	})
}

func (u *appendBlobUploader) Epilogue() {
	jptm := u.jptm
	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		// If the transfer status value < 0, then transfer failed with some failure
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: should we really do this deletion?  What if we are in an overwrite-existing-blob
		//    situation. Deletion has very different semantics then, compared to not deleting.
		deletionContext, _ := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := u.appendBlobUrl.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(u.appendBlobUrl.String(), "Delete (incomplete) Append Blob ", err)
		}
	}
}
