// Copyright © Microsoft <wastore@microsoft.com>
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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"math"
)

type blobFSUploader struct {
	blobFSSenderBase
	md5Channel chan []byte
}

func newBlobFSUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newBlobFSSenderBase(jptm, destination, p, pacer, sip)
	if err != nil {
		return nil, err
	}

	return &blobFSUploader{blobFSSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil

}

func (u *blobFSUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *blobFSUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newPacedRequestBody(jptm.Context(), reader, u.pacer)
		_, err := u.fileURL().AppendData(jptm.Context(), id.OffsetInFile(), body) // note: AppendData is really UpdatePath with "append" action
		if err != nil {
			jptm.FailActiveUpload("Uploading range", err)
			return
		}
	})
}

func (u *blobFSUploader) Epilogue() {
	jptm := u.jptm

	// flush
	if jptm.IsLive() {
		ss := jptm.Info().SourceSize
		md5Hash, ok := <-u.md5Channel
		if ok {
			// Flush incrementally to avoid timeouts on a full flush
			for i := int64(math.Min(float64(ss), float64(u.flushThreshold))); ; i = int64(math.Min(float64(ss), float64(i+u.flushThreshold))) {
				// Close only at the end of the file, keep all uncommitted data before then.
				_, err := u.fileURL().FlushData(jptm.Context(), i, md5Hash, *u.creationTimeHeaders, i != ss, i == ss)
				if err != nil {
					jptm.FailActiveUpload("Flushing data", err)
					break // don't return, since need cleanup below
				}

				if i == ss {
					break
				}
			}
		} else {
			jptm.FailActiveUpload("Getting hash", errNoHash) // don't return, since need cleanup below
		}
	}
}
