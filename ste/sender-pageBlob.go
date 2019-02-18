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

type pageBlobSenderBase struct {
	jptm            IJobPartTransferMgr
	destPageBlobURL azblob.PageBlobURL
	srcSize         int64
	chunkSize       uint32
	numChunks       uint32
	pacer           *pacer
}

type putPageFunc = func()

func newPageBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (*pageBlobSenderBase, error) {
	transferInfo := jptm.Info()

	// compute chunk count
	chunkSize := transferInfo.BlockSize
	// If the given chunk Size for the Job is invalild for page blob or greater than maximum page size,
	// then set chunkSize as maximum pageSize.
	chunkSize = common.Iffuint32(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)

	srcSize := transferInfo.SourceSize
	numChunks := getNumChunks(srcSize, chunkSize)

	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destPageBlobURL := azblob.NewPageBlobURL(*destURL, p)

	return &pageBlobSenderBase{
		jptm:            jptm,
		destPageBlobURL: destPageBlobURL,
		srcSize:         srcSize,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pacer:           pacer}, nil
}

func (s *pageBlobSenderBase) ChunkSize() uint32 {
	return s.chunkSize
}

func (s *pageBlobSenderBase) NumChunks() uint32 {
	return s.numChunks
}

func (s *pageBlobSenderBase) RemoteFileExists() (bool, error) {
	return remoteObjectExists(s.destPageBlobURL.GetProperties(s.jptm.Context(), azblob.BlobAccessConditions{}))
}

func (s *pageBlobSenderBase) prologue(httpHeader azblob.BlobHTTPHeaders, metadata azblob.Metadata, accessTier azblob.AccessTierType, logger ISenderLogger) {
	if _, err := s.destPageBlobURL.Create(s.jptm.Context(), s.srcSize, 0, httpHeader, metadata, azblob.BlobAccessConditions{}); err != nil {
		logger.FailActiveSend("Creating blob", err)
		return
	}

	// Set tier, https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	if accessTier != azblob.AccessTierNone {
		// Set the latest service version from sdk as service version in the context.
		ctxWithLatestServiceVersion := context.WithValue(s.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		if _, err := s.destPageBlobURL.SetTier(ctxWithLatestServiceVersion, accessTier, azblob.LeaseAccessConditions{}); err != nil {
			logger.FailActiveSendWithStatus("Setting PageBlob tier ", err, common.ETransferStatus.BlobTierFailure())
			return
		}
	}
}

// Returns a chunk-func for blob copies
func (s *pageBlobSenderBase) generatePutPageToRemoteFunc(id common.ChunkID, putPage putPageFunc) chunkFunc {
	return createSendToRemoteChunkFunc(s.jptm, id, func() {
		jptm := s.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		putPage()
	})
}

func (s *pageBlobSenderBase) epilogue() {
	jptm := s.jptm

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		deletionContext, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelFunc()
		_, err := s.destPageBlobURL.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(s.destPageBlobURL.String(), "Delete (incomplete) Page Blob ", err)
		}
	}
}
