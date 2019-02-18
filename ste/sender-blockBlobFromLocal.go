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

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

type blockBlobUploader struct {
	blockBlobSenderBase

	leadingBytes []byte // no lock because is written before first chunk-func go routine is scheduled
	logger       ISenderLogger
}

func newBlockBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer, sip sourceInfoProvider) (uploader, error) {
	senderBase, err := newBlockBlobSenderBase(jptm, destination, p, pacer)
	if err != nil {
		return nil, err
	}

	return &blockBlobUploader{blockBlobSenderBase: *senderBase, logger: &uploaderLogger{jptm: jptm}}, nil
}

func (u *blockBlobUploader) SetLeadingBytes(leadingBytes []byte) {
	u.leadingBytes = leadingBytes
}

func (u *blockBlobUploader) Prologue(state PrologueState) {
	// block blobs don't need any work done at this stage
	// But we do need to remember the leading bytes because we'll need them later
	u.leadingBytes = state.leadingBytes
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
	putBlockFromLocal := func(encodedBlockID string) {
		u.jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err := u.destBlockBlobURL.StageBlock(u.jptm.Context(), encodedBlockID, body, azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			u.jptm.FailActiveUpload("Staging block", err)
			return
		}
	}

	return u.generatePutBlockToRemoteFunc(id, blockIndex, putBlockFromLocal)
}

// generates PUT Blob (for a blob that fits in a single put request)
func (u *blockBlobUploader) generatePutWholeBlob(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		// Get blob http headers and metadata.
		blobHTTPHeader, metaData := jptm.BlobDstData(u.leadingBytes)

		// Upload the blob
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		var err error
		if jptm.Info().SourceSize == 0 {
			_, err = u.destBlockBlobURL.Upload(jptm.Context(), bytes.NewReader(nil), blobHTTPHeader, metaData, azblob.BlobAccessConditions{})
		} else {
			body := newLiteRequestBodyPacer(reader, u.pacer)
			_, err = u.destBlockBlobURL.Upload(jptm.Context(), body, blobHTTPHeader, metaData, azblob.BlobAccessConditions{})
		}

		// if the put blob is a failure, update the transfer status to failed
		if err != nil {
			jptm.FailActiveUpload("Uploading blob", err)
			return
		}
	})
}

func (u *blockBlobUploader) Epilogue() {
	// fetching the blob http headers with content-type, content-encoding attributes
	// fetching the metadata passed with the JobPartOrder
	blobHTTPHeader, metadata := u.jptm.BlobDstData(u.leadingBytes)
	blockBlobTier, _ := u.jptm.BlobTiers()

	u.epilogue(blobHTTPHeader, metadata, blockBlobTier.ToAccessTierType(), u.logger)
}
