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
	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  azblob.BlobHTTPHeaders
	metadataToApply azblob.Metadata
	destBlobTier    azblob.AccessTierType
}

func newPageBlobSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer, srcInfoProvider ISourceInfoProvider, inferredAccessTierType azblob.AccessTierType) (*pageBlobSenderBase, error) {
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

	props, err := srcInfoProvider.Properties()
	if err != nil {
		return nil, err
	}

	// If user set blob tier explicitly, override any value that our caller
	// may have guessed.
	destBlobTier := inferredAccessTierType
	_, pageBlobTierOverride := jptm.BlobTiers()
	if pageBlobTierOverride != common.EPageBlobTier.None() {
		destBlobTier = pageBlobTierOverride.ToAccessTierType()
	}

	return &pageBlobSenderBase{
		jptm:            jptm,
		destPageBlobURL: destPageBlobURL,
		srcSize:         srcSize,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pacer:           pacer,
		headersToApply:  props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		metadataToApply: props.SrcMetadata.ToAzBlobMetadata(),
		destBlobTier:    destBlobTier,
	}, nil
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

func (s *pageBlobSenderBase) Prologue(ps common.PrologueState) {
	if ps.CanInferContentType() {
		// sometimes, specifically when reading local files, we have more info
		// about the file type at this time than what we had before
		s.headersToApply.ContentType = ps.GetInferredContentType(s.jptm)
	}

	if _, err := s.destPageBlobURL.Create(s.jptm.Context(),
		s.srcSize,
		0,
		s.headersToApply,
		s.metadataToApply,
		azblob.BlobAccessConditions{}); err != nil {
		s.jptm.FailActiveSend("Creating blob", err)
		return
	}

	// Set tier, https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	if s.destBlobTier != azblob.AccessTierNone {
		// Set the latest service version from sdk as service version in the context.
		ctxWithLatestServiceVersion := context.WithValue(s.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		if _, err := s.destPageBlobURL.SetTier(ctxWithLatestServiceVersion, s.destBlobTier, azblob.LeaseAccessConditions{}); err != nil {
			s.jptm.FailActiveSendWithStatus("Setting PageBlob tier ", err, common.ETransferStatus.BlobTierFailure())
			return
		}
	}
}

func (s *pageBlobSenderBase) Epilogue() {
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
