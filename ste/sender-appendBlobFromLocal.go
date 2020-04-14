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
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type appendBlobUploader struct {
	appendBlobSenderBase

	md5Channel chan []byte
}

func newAppendBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newAppendBlobSenderBase(jptm, destination, p, pacer, sip)
	if err != nil {
		return nil, err
	}

	return &appendBlobUploader{appendBlobSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil
}

func (u *appendBlobUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *appendBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	appendBlockFromLocal := func() {
		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newPacedRequestBody(u.jptm.Context(), reader, u.pacer)
		_, err := u.destAppendBlobURL.AppendBlock(u.jptm.Context(), body,
			azblob.AppendBlobAccessConditions{
				AppendPositionAccessConditions: azblob.AppendPositionAccessConditions{IfAppendPositionEqual: id.OffsetInFile()},
			}, nil)
		if err != nil {
			u.jptm.FailActiveUpload("Appending block", err)
			return
		}
	}

	return u.generateAppendBlockToRemoteFunc(id, appendBlockFromLocal)
}

func (u *appendBlobUploader) Epilogue() {
	jptm := u.jptm

	// set content MD5 (only way to do this is to re-PUT all the headers, this time with the MD5 included)
	if jptm.IsLive() {
		tryPutMd5Hash(jptm, u.md5Channel, func(md5Hash []byte) error {
			epilogueHeaders := u.headersToApply
			epilogueHeaders.ContentMD5 = md5Hash
			_, err := u.destAppendBlobURL.SetHTTPHeaders(jptm.Context(), epilogueHeaders, azblob.BlobAccessConditions{})
			return err
		})
	}

	u.appendBlobSenderBase.Epilogue()
}

func (u *appendBlobUploader) GetDestinationLength() (int64, error) {
	prop, err := u.destAppendBlobURL.GetProperties(u.jptm.Context(), azblob.BlobAccessConditions{})

	if err != nil {
		return -1, err
	}

	return prop.ContentLength(), nil
}
