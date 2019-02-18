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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type appendBlobUploader struct {
	appendBlobSenderBase

	logger ISenderLogger
}

func newAppendBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error) {
	senderBase, err := newAppendBlobSenderBase(jptm, destination, p, pacer)
	if err != nil {
		return nil, err
	}

	return &appendBlobUploader{appendBlobSenderBase: *senderBase, logger: &uploaderLogger{jptm: jptm}}, nil
}

func (u *appendBlobUploader) Prologue(state PrologueState) {
	blobHTTPHeaders, metadata := u.jptm.BlobDstData(state.leadingBytes)
	u.prologue(blobHTTPHeaders, metadata, u.logger)
}

func (u *appendBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	appendBlockFromLocal := func() {
		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err := u.destAppendBlobURL.AppendBlock(u.jptm.Context(), body, azblob.AppendBlobAccessConditions{}, nil)
		if err != nil {
			u.jptm.FailActiveUpload("Appending block", err)
			return
		}
	}

	return u.generateAppendBlockToRemoteFunc(id, appendBlockFromLocal)
}

func (u *appendBlobUploader) Epilogue() {
	u.epilogue()
}
