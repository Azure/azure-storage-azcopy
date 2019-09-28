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
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

type urlToAzureFileCopier struct {
	azureFileSenderBase
	srcURL url.URL
}

func newURLToAzureFileCopier(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (ISenderBase, error) {
	srcInfoProvider := sip.(IRemoteSourceInfoProvider) // "downcast" to the type we know it really has

	senderBase, err := newAzureFileSenderBase(jptm, destination, p, pacer, sip)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	return &urlToAzureFileCopier{azureFileSenderBase: *senderBase, srcURL: *srcURL}, nil
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
		_, err := u.fileURL.UploadRangeFromURL(
			u.ctx, u.srcURL, id.OffsetInFile(), id.OffsetInFile(), adjustedChunkSize)
		if err != nil {
			u.jptm.FailActiveS2SCopy("Uploading range from URL", err)
			return
		}
	})
}

func (u *urlToAzureFileCopier) Epilogue() {}
