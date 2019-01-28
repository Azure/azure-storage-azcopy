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
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type urlToPageBlobCopier struct {
	jptm            IJobPartTransferMgr
	srcURL          url.URL
	destPageBlobURL azblob.PageBlobURL
	srcSize         int64
	chunkSize       uint32
	numChunks       uint32
	pacer           *pacer
	srcHTTPHeaders  azblob.BlobHTTPHeaders
	srcMetadata     azblob.Metadata
	destBlobTier    azblob.AccessTierType
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, source string, destination string, p pipeline.Pipeline, pacer *pacer) (s2sCopier, error) {
	// compute chunk count
	info := jptm.Info()
	srcSize := info.SourceSize
	chunkSize := info.BlockSize

	// If the given chunk Size for the Job is invalild for page blob or greater than maximum page size,
	// then set chunkSize as maximum pageSize.
	chunkSize = common.Iffuint32(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)

	numChunks := getNumCopyChunks(srcSize, chunkSize)

	srcURL, err := prepareSourceURL(jptm, info.Source)
	if err != nil {
		return nil, err
	}
	destURL, err := url.Parse(info.Destination)
	if err != nil {
		return nil, err
	}

	destPageBlobURL := azblob.NewPageBlobURL(*destURL, p)

	var azblobMetadata azblob.Metadata
	if info.SrcMetadata != nil {
		azblobMetadata = info.SrcMetadata.ToAzBlobMetadata()
	}

	// Get blob tier properly.
	destBlobTier := azblob.AccessTierNone
	if info.SrcBlobType == azblob.BlobPageBlob {
		destBlobTier = info.SrcBlobTier
	}
	_, pageBlobTierOverride := jptm.BlobTiers()
	if pageBlobTierOverride != common.EPageBlobTier.None() {
		destBlobTier = pageBlobTierOverride.ToAccessTierType()
	}

	return &urlToPageBlobCopier{
		jptm:            jptm,
		srcURL:          *srcURL,
		destPageBlobURL: destPageBlobURL,
		srcSize:         info.SourceSize,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pacer:           pacer,
		srcHTTPHeaders:  info.SrcHTTPHeaders,
		srcMetadata:     azblobMetadata,
		destBlobTier:    destBlobTier}, nil
}

func (c *urlToPageBlobCopier) ChunkSize() uint32 {
	return c.chunkSize
}

func (c *urlToPageBlobCopier) NumChunks() uint32 {
	return c.numChunks
}

func (c *urlToPageBlobCopier) RemoteFileExists() (bool, error) {
	return remoteObjectExists(c.destPageBlobURL.GetProperties(c.jptm.Context(), azblob.BlobAccessConditions{}))
}

func (c *urlToPageBlobCopier) Prologue() {
	jptm := c.jptm

	if _, err := c.destPageBlobURL.Create(jptm.Context(), c.srcSize, 0, c.srcHTTPHeaders, c.srcMetadata, azblob.BlobAccessConditions{}); err != nil {
		jptm.FailActiveS2SCopy("Creating blob", err)
		return
	}

	// Set tier, https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	if c.destBlobTier != azblob.AccessTierNone {
		// Ensure destBlobTier is not block blob tier, i.e. not Hot, Cool and Archive.
		var blockBlobTier common.BlockBlobTier
		if err := blockBlobTier.Parse(string(c.destBlobTier)); err != nil {
			// Set the latest service version from sdk as service version in the context.
			ctxWithLatestServiceVersion := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
			if _, err := c.destPageBlobURL.SetTier(ctxWithLatestServiceVersion, c.destBlobTier, azblob.LeaseAccessConditions{}); err != nil {
				jptm.FailActiveS2SCopyWithStatus("Setting PageBlob tier ", err, common.ETransferStatus.BlobTierFailure())
				return
			}
		}
	}
}

// Returns a chunk-func for blob copies
func (c *urlToPageBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	return createCopyChunkFunc(c.jptm, id, func() {
		jptm := c.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		// Set the latest service version from sdk as service version in the context, to use UploadPagesFromURL API.
		ctxWithLatestServiceVersion := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		_, err := c.destPageBlobURL.UploadPagesFromURL(
			ctxWithLatestServiceVersion, c.srcURL, id.OffsetInFile, id.OffsetInFile, adjustedChunkSize, azblob.PageBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveS2SCopy("Uploading page from URL", err)
			return
		}
	})
}

func (c *urlToPageBlobCopier) Epilogue() {
	jptm := c.jptm

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		deletionContext, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFunc()
		_, err := c.destPageBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(c.destPageBlobURL.String(), "Delete (incomplete) Page Blob ", err)
		}
	}
}
