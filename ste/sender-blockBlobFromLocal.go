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
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type blockBlobUploader struct {
	blockBlobSenderBase

	md5Channel chan []byte
}

func newBlockBlobUploader(jptm IJobPartTransferMgr, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newBlockBlobSenderBase(jptm, sip, nil)
	if err != nil {
		return nil, err
	}

	return &blockBlobUploader{blockBlobSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil
}

func (s *blockBlobUploader) Prologue(ps common.PrologueState) (destinationModified bool) {
	if s.jptm.Info().PreservePOSIXProperties {

		if unixSIP, ok := s.sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
			// Clone the metadata before we write to it, we shouldn't be writing to the same metadata as every other blob.
			s.metadataToApply = s.metadataToApply.Clone()

			statAdapter, err := unixSIP.GetUNIXProperties()
			if err != nil {
				s.jptm.FailActiveSend("GetUNIXProperties", err)
			}

			common.AddStatToBlobMetadata(statAdapter, s.metadataToApply)
		}
	}

	return s.blockBlobSenderBase.Prologue(ps)
}

func (u *blockBlobUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

// Returns a chunk-func for blob uploads
func (u *blockBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	if chunkIsWholeFile {
		if blockIndex > 0 {
			panic("chunk cannot be whole file where there is more than one chunk")
		}
		setPutListNeed(&u.atomicPutListIndicator, putListNotNeeded)
		return u.generatePutWholeBlob(id, reader)
	} else {
		setPutListNeed(&u.atomicPutListIndicator, putListNeeded)
		return u.generatePutBlock(id, blockIndex, reader)
	}
}

// generatePutBlock generates a func to upload the block of src data from given startIndex till the given chunkSize.
func (u *blockBlobUploader) generatePutBlock(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader) chunkFunc {
	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		// step 1: generate block ID
		encodedBlockID := u.generateEncodedBlockID(blockIndex)

		if u.ChunkAlreadyTransferred(blockIndex) {
			u.jptm.LogAtLevelForCurrentTransfer(common.LogDebug,
				fmt.Sprintf("Skipping chunk %d as it was already transferred.", blockIndex))
			atomic.AddInt32(&u.atomicChunksWritten, 1)
			return
		}

		// step 2: save the block ID into the list of block IDs
		u.setBlockID(blockIndex, encodedBlockID)

		// step 3: put block to remote
		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		_, err := u.destBlockBlobClient.StageBlock(u.jptm.Context(), encodedBlockID, reader,
			&blockblob.StageBlockOptions{
				CPKInfo:      u.jptm.CpkInfo(),
				CPKScopeInfo: u.jptm.CpkScopeInfo(),
			})
		if err != nil {
			u.jptm.FailActiveUpload("Staging block", err)
			return
		}

		atomic.AddInt32(&u.atomicChunksWritten, 1)
	})
}

// generates PUT Blob (for a blob that fits in a single put request)
func (u *blockBlobUploader) generatePutWholeBlob(id common.ChunkID, reader common.SingleChunkReader) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		// Upload the blob
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		var err error
		if !ValidateTier(jptm, u.destBlobTier, u.destBlockBlobClient, u.jptm.Context(), false) {
			u.destBlobTier = nil
		}

		blobTags := u.blobTagsToApply
		setTags := separateSetTagsRequired(blobTags)
		if setTags || len(blobTags) == 0 {
			blobTags = nil
		}

		// TODO: Remove this snippet once service starts supporting CPK with blob tier
		destBlobTier := u.destBlobTier
		if u.jptm.IsSourceEncrypted() {
			destBlobTier = nil
		}

		if jptm.Info().SourceSize == 0 {
			_, err = u.destBlockBlobClient.Upload(jptm.Context(), streaming.NopCloser(bytes.NewReader(nil)),
				&blockblob.UploadOptions{
					HTTPHeaders:  &u.headersToApply,
					Metadata:     u.metadataToApply,
					Tier:         destBlobTier,
					Tags:         blobTags,
					CPKInfo:      jptm.CpkInfo(),
					CPKScopeInfo: jptm.CpkScopeInfo(),
				})
		} else {
			// File with content

			// Get the MD5 that was computed as we read the file
			md5Hash, ok := <-u.md5Channel
			if !ok {
				jptm.FailActiveUpload("Getting hash", errNoHash)
				return
			}
			if len(md5Hash) != 0 {
				u.headersToApply.BlobContentMD5 = md5Hash
			}

			// Upload the file
			_, err = u.destBlockBlobClient.Upload(jptm.Context(), reader,
				&blockblob.UploadOptions{
					HTTPHeaders:  &u.headersToApply,
					Metadata:     u.metadataToApply,
					Tier:         destBlobTier,
					Tags:         blobTags,
					CPKInfo:      jptm.CpkInfo(),
					CPKScopeInfo: jptm.CpkScopeInfo(),
				})
		}

		// if the put blob is a failure, update the transfer status to failed
		if err != nil {
			jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Committing block list (with tags)", "Committing block list"), err)
			return
		}

		atomic.AddInt32(&u.atomicChunksWritten, 1)

		if setTags {
			if _, err := u.destBlockBlobClient.SetTags(jptm.Context(), u.blobTagsToApply, nil); err != nil {
				jptm.FailActiveSend("Set blob tags", err)
			}
		}
	})
}

func (u *blockBlobUploader) Epilogue() {
	jptm := u.jptm

	shouldPutBlockList := getPutListNeed(&u.atomicPutListIndicator)

	if jptm.IsLive() && shouldPutBlockList == putListNeeded {

		md5Hash, ok := <-u.md5Channel
		if ok {
			if len(md5Hash) != 0 {
				u.headersToApply.BlobContentMD5 = md5Hash
			}
		} else {
			jptm.FailActiveSend("Getting hash", errNoHash)
			return
		}
	}

	u.blockBlobSenderBase.Epilogue()
}
