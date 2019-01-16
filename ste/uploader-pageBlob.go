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
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"time"
)

type pageBlobUploader struct {
	jptm        IJobPartTransferMgr
	pageBlobUrl azblob.PageBlobURL
	chunkSize   uint32
	numChunks   uint32
	pipeline    pipeline.Pipeline
	pacer       *pacer
}

func newPageBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error) {
	// compute chunk count
	info := jptm.Info()
	fileSize := info.SourceSize
	chunkSize := info.BlockSize
	// If the given chunk Size for the Job is greater than maximum page size i.e 4 MB
	// then set maximum pageSize will be 4 MB.
	chunkSize = common.Iffuint32(
		chunkSize > common.DefaultPageBlobChunkSize || (chunkSize%azblob.PageBlobPageBytes != 0),
		common.DefaultPageBlobChunkSize,
		chunkSize)
	numChunks := getNumUploadChunks(fileSize, chunkSize)

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	return &pageBlobUploader{
		jptm:        jptm,
		pageBlobUrl: azblob.NewBlobURL(*destURL, p).ToPageBlobURL(),
		chunkSize:   chunkSize,
		numChunks:   numChunks,
		pipeline:    p,
		pacer:       pacer,
	}, nil
}

func (u *pageBlobUploader) ChunkSize() uint32 {
	return u.chunkSize
}

func (u *pageBlobUploader) NumChunks() uint32 {
	return u.numChunks
}

func (u *pageBlobUploader) RemoteFileExists() (bool, error) {
	_, err := u.pageBlobUrl.GetProperties(u.jptm.Context(), azblob.BlobAccessConditions{})
	return err == nil, nil
}

func (u *pageBlobUploader) Prologue(leadingBytes []byte) {
	jptm := u.jptm
	info := jptm.Info()

	// create
	blobHTTPHeaders, metaData := jptm.BlobDstData(leadingBytes)
	_, err := u.pageBlobUrl.Create(jptm.Context(), info.SourceSize,
		0, blobHTTPHeaders, metaData, azblob.BlobAccessConditions{})
	if err != nil {
		jptm.FailActiveUpload("Creating blob", err)
		return
	}

	// set tier
	_, pageBlobTier := jptm.BlobTiers()
	if pageBlobTier != common.EPageBlobTier.None() {
		// for blob tier, set the latest service version from sdk as service version in the context.
		ctxWithValue := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)
		_, err = u.pageBlobUrl.SetTier(ctxWithValue, pageBlobTier.ToAccessTierType(), azblob.LeaseAccessConditions{})
		if err != nil {
			jptm.FailActiveUploadWithStatus("Setting PageBlob tier ", err, common.ETransferStatus.BlobTierFailure())
			return
		}
	}
}

func (u *pageBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createUploadChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		if reader.HasPrefetchedEntirelyZeros() {
			// for this destination type, there is no need to upload ranges than consist entirely of zeros
			jptm.Log(pipeline.LogDebug,
				fmt.Sprintf("Not uploading range from %d to %d,  all bytes are zero",
					id.OffsetInFile, id.OffsetInFile+reader.Length()))
			return
		}

		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, u.pacer)
		_, err := u.pageBlobUrl.UploadPages(jptm.Context(), id.OffsetInFile, body, azblob.PageBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveUpload("Uploading page", err)
			return
		}
	})
}

func (u *pageBlobUploader) Epilogue() {
	jptm := u.jptm

	// Cleanup
	if jptm.TransferStatus() <= 0 { // TODO: <=0 or <0?
		deletionContext, _ := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := u.pageBlobUrl.Delete(deletionContext, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			jptm.LogError(u.pageBlobUrl.String(), "Delete (incomplete) Page Blob ", err)
		}
	}
}
