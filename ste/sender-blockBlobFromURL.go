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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// urlToBlockBlobCopier extends blockBlobSenderBase parent to include URL-specific functionality
type urlToBlockBlobCopier struct {
	blockBlobSenderBase

	srcURL               string
	addFileRequestIntent bool // Necessary for FileBlob Oauth copies
}

func newURLToBlockBlobCopier(jptm IJobPartTransferMgr, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	// Get blob tier, by default set none.
	var destBlobTier *blob.AccessTier
	// If the source is block blob, preserve source's blob tier.
	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == blob.BlobTypeBlockBlob {
			destBlobTier = blobSrcInfoProvider.BlobTier()
		}
	}

	senderBase, err := newBlockBlobSenderBase(jptm, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	// Check if source is Files
	intentBool := false
	if _, ok := srcInfoProvider.(*fileSourceInfoProvider); ok {
		sUrl, _ := file.ParseURL(srcURL)
		intentBool = sUrl.SAS.Signature() == "" // No SAS means using OAuth
	}
	return &urlToBlockBlobCopier{
		blockBlobSenderBase:  *senderBase,
		srcURL:               srcURL,
		addFileRequestIntent: intentBool}, nil
}

// Returns a chunk-func for blob copies
func (c *urlToBlockBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	/*
	 * There was a optimization here to use PutBlob for zero-byte blobs instead of PutBlobFromURL.
	 * It was removed because of these reasons:
	 * 1. Both apis are different in some aspects. For put blob service verifies the content md5.
	 * This is not required if check-md5 is false. Using same calls helps us be consistent.
	 * 2. If the source only has list (and no read) permissions, we will still put the blob here
	 * While it is arguable that content can be inferred from size, it is better to fail transfer
	 * for blobs of all sizes.
	 */
	// Small blobs from all sources will be copied over to destination using PutBlobFromUrl
	if c.NumChunks() == 1 && adjustedChunkSize <= int64(common.MaxPutBlobSize) {
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

// generatePutBlockFromURL generates a func to copy the block of src data from given startIndex till the given chunkSize.
func (c *urlToBlockBlobCopier) generatePutBlockFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		// step 1: generate block ID
		encodedBlockID := c.generateEncodedBlockID(blockIndex)

		// step 2: save the block ID into the list of block IDs
		c.setBlockID(blockIndex, encodedBlockID)

		if c.ChunkAlreadyTransferred(blockIndex) {
			c.jptm.LogAtLevelForCurrentTransfer(common.LogDebug, fmt.Sprintf("Skipping chunk %d as it was already transferred.", blockIndex))
			atomic.AddInt32(&c.atomicChunksWritten, 1)
			return
		}

		// step 3: put block to remote
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}
		options := &blockblob.StageBlockFromURLOptions{
			Range:                   blob.HTTPRange{Offset: id.OffsetInFile(), Count: adjustedChunkSize},
			CPKInfo:                 c.jptm.CpkInfo(),
			CPKScopeInfo:            c.jptm.CpkScopeInfo(),
			CopySourceAuthorization: token,
		}

		// Informs SDK to add xms-file-request-intent header
		if c.addFileRequestIntent {
			fileIntent := blob.FileRequestIntentTypeBackup
			options.FileRequestIntent = &fileIntent
		}

		_, err = c.destBlockBlobClient.StageBlockFromURL(c.jptm.Context(), encodedBlockID, c.srcURL, options)

		if err != nil {
			c.jptm.FailActiveSend("Staging block from URL", err)
			return
		}

		atomic.AddInt32(&c.atomicChunksWritten, 1)
	})
}

func (c *urlToBlockBlobCopier) generateStartPutBlobFromURL(id common.ChunkID, blockIndex int32, adjustedChunkSize int64) chunkFunc {
	return createSendToRemoteChunkFunc(c.jptm, id, func() {

		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		// Create blob and finish.
		if !ValidateTier(c.jptm, c.destBlobTier, c.destBlockBlobClient, c.jptm.Context(), false) {
			c.destBlobTier = nil
		}

		blobTags := c.blobTagsToApply
		setTags := separateSetTagsRequired(blobTags)
		if setTags || len(blobTags) == 0 {
			blobTags = nil
		}

		// TODO: Remove this snippet once service starts supporting CPK with blob tier
		destBlobTier := c.destBlobTier
		if c.jptm.IsSourceEncrypted() {
			destBlobTier = nil
		}

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}

		options := &blockblob.UploadBlobFromURLOptions{
			HTTPHeaders:             &c.headersToApply,
			Metadata:                c.metadataToApply,
			Tier:                    destBlobTier,
			Tags:                    blobTags,
			CPKInfo:                 c.jptm.CpkInfo(),
			CPKScopeInfo:            c.jptm.CpkScopeInfo(),
			CopySourceAuthorization: token,
		}
		// Informs SDK to add xms-file-request-intent header
		if c.addFileRequestIntent {
			fileRequestIntent := blob.FileRequestIntentTypeBackup
			options.FileRequestIntent = &fileRequestIntent
		}
		_, err = c.destBlockBlobClient.UploadBlobFromURL(c.jptm.Context(), c.srcURL, options)

		if err != nil {
			c.jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Committing block list (with tags)", "Committing block list"), err)
			return
		}

		atomic.AddInt32(&c.atomicChunksWritten, 1)

		if setTags {
			if _, err := c.destBlockBlobClient.SetTags(c.jptm.Context(), c.blobTagsToApply, nil); err != nil {
				c.jptm.FailActiveSend("Set blob tags", err)
			}
		}
	})
}
