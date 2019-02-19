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
	"encoding/base64"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type blockBlobUploader struct {
	blockBlobSenderBase
}

func newBlockBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer, sip sourceInfoProvider) (ISenderBase, error) {
	senderBase, err := newBlockBlobSenderBase(jptm, destination, p, pacer, sip, azblob.AccessTierNone)
	if err != nil {
		return nil, err
	}

	return &blockBlobUploader{blockBlobSenderBase: *senderBase}, nil
}

// Returns a chunk-func for blob uploads
func (u *blockBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	if chunkIsWholeFile {
		if blockIndex > 0 {
			panic("chunk cannot be whole file where there is more than one chunk")
		}
		setPutListNeed(&u.putListIndicator, putListNotNeeded)
		return u.generatePutWholeBlob(id, blockIndex, reader)
	} else {
		setPutListNeed(&u.putListIndicator, putListNeeded)
		return u.generatePutBlock(id, blockIndex, reader)
	}
}

// generatePutBlock generates a func to upload the block of src data from given startIndex till the given chunkSize.
func (u *blockBlobUploader) generatePutBlock(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader) chunkFunc {
	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		// step 1: generate block ID
		blockID := common.NewUUID().String()
		encodedBlockID := base64.StdEncoding.EncodeToString([]byte(blockID))

		// step 2: save the block ID into the list of block IDs
		u.setBlockID(blockIndex, encodedBlockID)

		// step 3: put block to remote
		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err := u.destBlockBlobURL.StageBlock(u.jptm.Context(), encodedBlockID, body, azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			u.jptm.FailActiveUpload("Staging block", err)
			return
		}
	})
}

// generates PUT Blob (for a blob that fits in a single put request)
func (u *blockBlobUploader) generatePutWholeBlob(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		// Upload the blob
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		var err error
		if jptm.Info().SourceSize == 0 {
			_, err = u.destBlockBlobURL.Upload(jptm.Context(), bytes.NewReader(nil), u.headersToApply, u.metadataToApply, azblob.BlobAccessConditions{})
		} else {
			body := newLiteRequestBodyPacer(reader, u.pacer)
			_, err = u.destBlockBlobURL.Upload(jptm.Context(), body, u.headersToApply, u.metadataToApply, azblob.BlobAccessConditions{})
		}

		// if the put blob is a failure, update the transfer status to failed
		if err != nil {
			jptm.FailActiveUpload("Uploading blob", err)
			return
		}
	})
}
