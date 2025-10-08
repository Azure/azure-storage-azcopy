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
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/http"
)

type urlToAzureFileCopier struct {
	azureFileSenderBase
	srcURL string
}

func newURLToAzureFileCopier(jptm IJobPartTransferMgr, destination string, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	srcInfoProvider := sip.(IRemoteSourceInfoProvider) // "downcast" to the type we know it really has

	senderBase, err := newAzureFileSenderBase(jptm, destination, pacer, sip)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	return &urlToAzureFileCopier{azureFileSenderBase: *senderBase, srcURL: srcURL}, nil
}

func (u *urlToAzureFileCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		// TODO consider optimizations for sparse files
		// they are often compared to page blobs, but unlike vhd images, Azure Files may not be sparse in general
		if u.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// upload the range (including application of global pacing. We don't have a separate wait reason for global pacing
		// so just do it inside the S2SCopyOnWire state)
		u.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		if err := u.pacer.RequestTrafficAllocation(u.jptm.Context(), adjustedChunkSize); err != nil {
			u.jptm.FailActiveUpload("Pacing block (global level)", err)
		}
		// destination auth is OAuth, so we need to use the special policy to add the x-ms-file-request-intent header since the SDK has not yet implemented it.
		token, err := u.jptm.GetS2SSourceTokenCredential(u.jptm.Context())
		if err != nil {
			u.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}
		ctx := u.ctx
		if u.addFileRequestIntent || (token != nil && u.jptm.FromTo().From().IsFile()) {
			ctx = context.WithValue(u.ctx, addFileRequestIntent, true)
		}
		ctx = context.WithValue(ctx, removeSourceContentCRC64, true)
		_, err = u.getFileClient().UploadRangeFromURL(
			ctx, u.srcURL, id.OffsetInFile(), id.OffsetInFile(), adjustedChunkSize,
			&file.UploadRangeFromURLOptions{
				CopySourceAuthorization: token,
			})
		if err != nil {
			u.jptm.FailActiveS2SCopy("Uploading range from URL", err)
			return
		}
	})
}

func (u *urlToAzureFileCopier) Epilogue() {
	u.azureFileSenderBase.Epilogue()
}

type fileRequestIntent struct{}

var addFileRequestIntent = fileRequestIntent{}

type sourceContentCRC64 struct{}

var removeSourceContentCRC64 = sourceContentCRC64{}

type fileUploadRangeFromURLFixPolicy struct {
}

func newFileUploadRangeFromURLFixPolicy() policy.Policy {
	return &fileUploadRangeFromURLFixPolicy{}
}

func (r *fileUploadRangeFromURLFixPolicy) Do(req *policy.Request) (*http.Response, error) {
	if value := req.Raw().Context().Value(removeSourceContentCRC64); value != nil {
		delete(req.Raw().Header, "x-ms-source-content-crc64")
	}

	if value := req.Raw().Context().Value(addFileRequestIntent); value != nil {
		req.Raw().Header["x-ms-file-request-intent"] = []string{"backup"}
	}
	return req.Next()
}
