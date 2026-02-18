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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/pacer"
)

type appendBlobUploader struct {
	appendBlobSenderBase

	md5Channel chan []byte
	sip        ISourceInfoProvider
}

func (u *appendBlobUploader) Prologue(ps common.PrologueState) (destinationModified bool) {
	if u.jptm.Info().PreservePOSIXProperties {
		if unixSIP, ok := u.sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
			// Clone the metadata before we write to it, we shouldn't be writing to the same metadata as every other blob.
			u.metadataToApply = u.metadataToApply.Clone()

			statAdapter, err := unixSIP.GetUNIXProperties()
			if err != nil {
				u.jptm.FailActiveSend("GetUNIXProperties", err)
			}

			common.AddStatToBlobMetadata(statAdapter, u.metadataToApply, u.jptm.Info().PosixPropertiesStyle)
		}
	}

	return u.appendBlobSenderBase.Prologue(ps)
}

func newAppendBlobUploader(jptm IJobPartTransferMgr, destination string, pacer pacer.Interface, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newAppendBlobSenderBase(jptm, destination, pacer, sip)
	if err != nil {
		return nil, err
	}

	return &appendBlobUploader{appendBlobSenderBase: *senderBase, md5Channel: newMd5Channel(), sip: sip}, nil
}

func (u *appendBlobUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *appendBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	appendBlockFromLocal := func() {
		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		pacerReq := <-u.pacer.InitiateRequest(reader.Length(), u.jptm.Context())
		offset := id.OffsetInFile()
		var timeoutFromCtx bool
		ctx := withTimeoutNotification(u.jptm.Context(), &timeoutFromCtx)
		_, err := u.destAppendBlobClient.AppendBlock(ctx, pacerReq.WrapRequestBody(reader),
			&appendblob.AppendBlockOptions{
				AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{AppendPosition: &offset},
				CPKInfo:                        u.jptm.CpkInfo(),
				CPKScopeInfo:                   u.jptm.CpkScopeInfo(),
			})
		errString, err := u.transformAppendConditionMismatchError(timeoutFromCtx, offset, reader.Length(), err)
		if err != nil {
			errString = "Appending block" + errString
			u.jptm.FailActiveUpload(errString, err)
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
			epilogueHeaders.BlobContentMD5 = md5Hash
			_, err := u.destAppendBlobClient.SetHTTPHeaders(jptm.Context(), epilogueHeaders, nil)
			return err
		})
	}

	u.appendBlobSenderBase.Epilogue()
}
