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
	"fmt"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type pageBlobUploader struct {
	pageBlobSenderBase
}

func newPageBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer, sip ISourceInfoProvider) (ISenderBase, error) {
	senderBase, err := newPageBlobSenderBase(jptm, destination, p, pacer, sip, azblob.AccessTierNone)
	if err != nil {
		return nil, err
	}

	return &pageBlobUploader{pageBlobSenderBase: *senderBase}, nil
}

func (u *pageBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm
		if u.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		if reader.HasPrefetchedEntirelyZeros() {
			// for this destination type, there is no need to upload ranges than consist entirely of zeros
			jptm.Log(pipeline.LogDebug,
				fmt.Sprintf("Not uploading range from %d to %d,  all bytes are zero",
					id.OffsetInFile, id.OffsetInFile+reader.Length()))
			return
		}

		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err := u.destPageBlobURL.UploadPages(jptm.Context(), id.OffsetInFile, body, azblob.PageBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveUpload("Uploading page", err)
			return
		}
	})
}
