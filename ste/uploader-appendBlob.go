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
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
	"golang.org/x/sync/semaphore"
)

type appendBlobUploader struct {
	jptm                   IJobPartTransferMgr
	appendBlobUrl          azblob.AppendBlobURL
	chunkSize              uint32
	numChunks              uint32
	pipeline               pipeline.Pipeline
	pacer                  *pacer
	soleChunkFuncSemaphore *semaphore.Weighted
	md5Channel             chan []byte
	creationTimeHeaders    *azblob.BlobHTTPHeaders
}

func newAppendBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error) {
	// compute chunk count
	info := jptm.Info()
	fileSize := info.SourceSize
	chunkSize := info.BlockSize

	// If the given chunk Size for the Job is greater than max append blob block size, then
	// then set it to the max.
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
		jptm:                   jptm,
		appendBlobUrl:          azblob.NewBlobURL(*destURL, p).ToAppendBlobURL(),
		chunkSize:              chunkSize,
		numChunks:              numChunks,
		pipeline:               p,
		pacer:                  pacer,
		soleChunkFuncSemaphore: semaphore.NewWeighted(1),
		md5Channel:             newMd5Channel(),
	}, nil
}

func (u *appendBlobUploader) ChunkSize() uint32 {
	return u.chunkSize
}

func (u *appendBlobUploader) NumChunks() uint32 {
	return u.numChunks
}

func (u *appendBlobUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *appendBlobUploader) RemoteFileExists() (bool, error) {
	return remoteObjectExists(u.appendBlobUrl.GetProperties(u.jptm.Context(), azblob.BlobAccessConditions{}))
}

func (u *appendBlobUploader) Prologue(leadingBytes []byte) {
	jptm := u.jptm

	blobHTTPHeaders, metaData := jptm.BlobDstData(leadingBytes)
	_, err := u.appendBlobUrl.Create(jptm.Context(), blobHTTPHeaders, metaData, azblob.BlobAccessConditions{})
	if err != nil {
		jptm.FailActiveUpload("Creating blob", err)
		return
	}
	// Save headers to re-use, with same values, in epilogue
	u.creationTimeHeaders = &blobHTTPHeaders
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

		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err = u.appendBlobUrl.AppendBlock(jptm.Context(), body, azblob.AppendBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveUpload("Appending block", err)
			return
		}
	})
}

// TODO: Confirm with john about the epilogue difference.
func (u *appendBlobUploader) Epilogue() {
	jptm := u.jptm

	// set content MD5 (only way to do this is to re-PUT all the headers, this time with the MD5 included)
	if jptm.TransferStatus() > 0 {
		tryPutMd5Hash(jptm, u.md5Channel, func(md5Hash []byte) error {
			epilogueHeaders := *u.creationTimeHeaders
			epilogueHeaders.ContentMD5 = md5Hash
			_, err := u.appendBlobUrl.SetHTTPHeaders(jptm.Context(), epilogueHeaders, azblob.BlobAccessConditions{})
			return err
		})
	}

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		// If the transfer status value < 0, then transfer failed with some failure
		// there is a possibility that some uncommitted blocks will be there
		// Delete the uncommitted blobs
		// TODO: should we really do this deletion?  What if we are in an overwrite-existing-blob
		//    situation. Deletion has very different semantics then, compared to not deleting.
		deletionContext, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFn()
		_, err := u.appendBlobUrl.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(u.appendBlobUrl.String(), "Delete (incomplete) Append Blob ", err)
		}
	}
}
