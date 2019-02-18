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

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type urlToPageBlobCopier struct {
	pageBlobSenderBase

	srcURL         url.URL
	logger         ISenderLogger
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, srcInfoProvider s2sSourceInfoProvider, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error) {

	destBlobTier := azblob.AccessTierNone
	// If the source is page blob, preserve source's blob tier.
	if blobSrcInfoProvider, ok := srcInfoProvider.(s2sBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == azblob.BlobPageBlob {
			destBlobTier = blobSrcInfoProvider.BlobTier()
		}
	}

	senderBase, err := newPageBlobSenderBase(jptm, destination, p, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	return &urlToPageBlobCopier{
		pageBlobSenderBase: *senderBase,
		srcURL:         *srcURL,
		logger:         &s2sCopierLogger{jptm: jptm}}, nil
}

// Returns a chunk-func for blob copies
func (c *urlToPageBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		if c.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		s2sPacer := newS2SPacer(c.pacer)

		// Set the latest service version from sdk as service version in the context, to use UploadPagesFromURL API.
		ctxWithLatestServiceVersion := context.WithValue(c.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		_, err := c.destPageBlobURL.UploadPagesFromURL(
			ctxWithLatestServiceVersion, c.srcURL, id.OffsetInFile, id.OffsetInFile, adjustedChunkSize, azblob.PageBlobAccessConditions{}, nil)
		if err != nil {
			c.jptm.FailActiveS2SCopy("Uploading page from URL", err)
			return
		}
		s2sPacer.Done(adjustedChunkSize)
	})
}

