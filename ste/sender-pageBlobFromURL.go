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
	srcHTTPHeaders azblob.BlobHTTPHeaders
	srcMetadata    azblob.Metadata
	destBlobTier   azblob.AccessTierType
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, srcInfoProvider s2sSourceInfoProvider, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error) {
	senderBase, err := newPageBlobSenderBase(jptm, destination, p, pacer)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	srcProperties, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	var azblobMetadata azblob.Metadata
	if srcProperties.SrcMetadata != nil {
		azblobMetadata = srcProperties.SrcMetadata.ToAzBlobMetadata()
	}

	// Get blob tier, by default set none.
	destBlobTier := azblob.AccessTierNone
	// If the source is page blob, preserve source's blob tier.
	if blobSrcInfoProvider, ok := srcInfoProvider.(s2sBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == azblob.BlobPageBlob {
			destBlobTier = blobSrcInfoProvider.BlobTier()
		}
	}
	// If user set blob tier explictly for copy, use it accorcingly.
	_, pageBlobTierOverride := jptm.BlobTiers()
	if pageBlobTierOverride != common.EPageBlobTier.None() {
		destBlobTier = pageBlobTierOverride.ToAccessTierType()
	}

	return &urlToPageBlobCopier{
		pageBlobSenderBase: *senderBase,

		srcURL:         *srcURL,
		srcHTTPHeaders: srcProperties.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		srcMetadata:    azblobMetadata,
		destBlobTier:   destBlobTier}, nil
}

func (c *urlToPageBlobCopier) Prologue(state PrologueState) {
	c.prologue(c.srcHTTPHeaders, c.srcMetadata, c.destBlobTier, c)
}

// Returns a chunk-func for blob copies
func (c *urlToPageBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {

	putPageFromURL := func() {
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
	}

	return c.generatePutPageToRemoteFunc(id, putPageFromURL)
}

func (c *urlToPageBlobCopier) Epilogue() {
	c.epilogue()
}

func (c *urlToPageBlobCopier) FailActiveSend(where string, err error) {
	c.jptm.FailActiveS2SCopy(where, err)
}

func (c *urlToPageBlobCopier) FailActiveSendWithStatus(where string, err error, failureStatus common.TransferStatus) {
	c.jptm.FailActiveS2SCopyWithStatus(where, err, failureStatus)
}
