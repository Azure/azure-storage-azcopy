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
	"bytes"
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type urlToBlockBlobCopier struct {
	blockBlobSenderBase

	srcURL url.URL
}

func newURLToBlockBlobCopier(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	// Get blob tier, by default set none.
	destBlobTier := azblob.AccessTierNone
	// If the source is block blob, preserve source's blob tier.
	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == azblob.BlobBlockBlob {
			destBlobTier = blobSrcInfoProvider.BlobTier()
		}
	}

	senderBase, err := newBlockBlobSenderBase(jptm, destination, p, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	return &urlToBlockBlobCopier{
		blockBlobSenderBase: *senderBase,
		srcURL:              *srcURL}, nil
}

// Returns a chunk-func for blob copies
func (c *urlToBlockBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	if blockIndex == 0 && adjustedChunkSize == 0 {
		setPutListNeed(&c.atomicPutListIndicator, putListNotNeeded)
		return c.generateCreateEmptyBlob(id)
	}
	// Small blobs from all sources will be copied over to destination using PutBlobFromUrl with the exception of files
	fromTo := c.blockBlobSenderBase.jptm.FromTo()
	if c.NumChunks() == 1 && adjustedChunkSize <= int64(azblob.BlockBlobMaxUploadBlobBytes) && fromTo.From() != common.ELocation.File() {
		/*
		 * siminsavani: FYI: For GCP, if the blob is the entirety of the file, GCP still returns
		 * invalid error from service due to PutBlockFromUrl.
		 */
		setPutListNeed(&c.atomicPutListIndicator, putListNotNeeded)
		return c.generateStartPutBlobFromURL(id, blockIndex, adjustedChunkSize)

	}
	setPutListNeed(&c.atomicPutListIndicator, putListNeeded)
	return c.generatePutBlockFromURL(id, blockIndex, adjustedChunkSize)
}

// generateCreateEmptyBlob generates a func to create empty blob in destination.
// This could be replaced by sync version of copy blob from URL.
func (c *urlToBlockBlobCopier) generateCreateEmptyBlob(id common.ChunkID) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		jptm := c.jptm

		jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		// Create blob and finish.
		if !ValidateTier(jptm, c.destBlobTier, c.destBlockBlobURL.BlobURL, c.jptm.Context()) {
			c.destBlobTier = azblob.DefaultAccessTier
		}

		blobTags := c.blobTagsToApply
		separateSetTagsRequired := separateSetTagsRequired(blobTags)
		if separateSetTagsRequired || len(blobTags) == 0 {
			blobTags = nil
		}

		// TODO: Remove this snippet once service starts supporting CPK with blob tier
		destBlobTier := c.destBlobTier
		if c.cpkToApply.EncryptionScope != nil || (c.cpkToApply.EncryptionKey != nil && c.cpkToApply.EncryptionKeySha256 != nil) {
			destBlobTier = azblob.AccessTierNone
		}

		if _, err := c.destBlockBlobURL.Upload(c.jptm.Context(), bytes.NewReader(nil), c.headersToApply, c.metadataToApply, azblob.BlobAccessConditions{}, destBlobTier, blobTags, c.cpkToApply); err != nil {
			jptm.FailActiveSend("Creating empty blob", err)
			return
		}

		if separateSetTagsRequired {
			if _, err := c.destBlockBlobURL.SetTags(jptm.Context(), nil, nil, nil, c.blobTagsToApply); err != nil {
				c.jptm.Log(pipeline.LogWarning, err.Error())
			}
		}
	})
}

// generatePutBlockFromURL generates a func to copy the block of src data from given startIndex till the given chunkSize.
func (c *urlToBlockBlobCopier) generatePutBlockFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		// step 1: generate block ID
		encodedBlockID := c.generateEncodedBlockID()

		// step 2: save the block ID into the list of block IDs
		c.setBlockID(blockIndex, encodedBlockID)

		// step 3: put block to remote
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		// Set the latest service version from sdk as service version in the context, to use StageBlockFromURL API
		ctxWithLatestServiceVersion := context.WithValue(c.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		_, err := c.destBlockBlobURL.StageBlockFromURL(ctxWithLatestServiceVersion, encodedBlockID, c.srcURL,
			id.OffsetInFile(), adjustedChunkSize, azblob.LeaseAccessConditions{}, azblob.ModifiedAccessConditions{}, c.cpkToApply)
		if err != nil {
			c.jptm.FailActiveSend("Staging block from URL", err)
			return
		}
	})
}

func (c *urlToBlockBlobCopier) generateStartPutBlobFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {

		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		ctxWithLatestServiceVersion := context.WithValue(c.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}

		_, err := c.destBlockBlobURL.PutBlobFromURL(ctxWithLatestServiceVersion, azblob.BlobHTTPHeaders{}, c.srcURL, c.metadataToApply,
			azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{}, nil, nil, azblob.DefaultAccessTier, nil,
			azblob.ClientProvidedKeyOptions{})

		if err != nil {
			c.jptm.FailActiveSend("Put Blob from URL", err)
			return
		}

	})
}

// GetDestinationLength gets the destination length.
func (c *urlToBlockBlobCopier) GetDestinationLength() (int64, error) {
	ctxWithLatestServiceVersion := context.WithValue(c.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
	properties, err := c.destBlockBlobURL.GetProperties(ctxWithLatestServiceVersion, azblob.BlobAccessConditions{}, c.cpkToApply)
	if err != nil {
		return -1, err
	}

	return properties.ContentLength(), nil
}
