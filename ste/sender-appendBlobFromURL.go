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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type urlToAppendBlobCopier struct {
	appendBlobSenderBase

	srcURL string
}

func newURLToAppendBlobCopier(jptm IJobPartTransferMgr, destination string, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	senderBase, err := newAppendBlobSenderBase(jptm, destination, srcInfoProvider)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	return &urlToAppendBlobCopier{
		appendBlobSenderBase: *senderBase,
		srcURL:               srcURL}, nil
}

// Returns a chunk-func for blob copies
func (c *urlToAppendBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	appendBlockFromURL := func() {
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		offset := id.OffsetInFile()
		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}
		var timeoutFromCtx bool
		ctx := withTimeoutNotification(c.jptm.Context(), &timeoutFromCtx)
		_, err = c.destAppendBlobClient.AppendBlockFromURL(ctx, c.srcURL,
			&appendblob.AppendBlockFromURLOptions{
				Range:                          blob.HTTPRange{Offset: offset, Count: adjustedChunkSize},
				AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{AppendPosition: &offset},
				CPKInfo:                        c.jptm.CpkInfo(),
				CPKScopeInfo:                   c.jptm.CpkScopeInfo(),
				CopySourceAuthorization:        token,
			})
		errString, err := c.transformAppendConditionMismatchError(timeoutFromCtx, offset, adjustedChunkSize, err)
		if err != nil {
			errString = "Appending block from URL" + errString
			c.jptm.FailActiveS2SCopy(errString, err)
			return
		}
	}

	return c.generateAppendBlockToRemoteFunc(id, appendBlockFromURL)
}
