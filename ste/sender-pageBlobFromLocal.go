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

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type pageBlobUploader struct {
	pageBlobSenderBase

	md5Channel chan []byte
}

func newPageBlobUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newPageBlobSenderBase(jptm, destination, p, pacer, sip, azblob.AccessTierNone)
	if err != nil {
		return nil, err
	}

	return &pageBlobUploader{pageBlobSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil
}

func (u *pageBlobUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *pageBlobUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		defer reader.Close() // In case of memory leak in sparse file case.

		if u.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		if reader.HasPrefetchedEntirelyZeros() {
			var destContainsData bool
			// We check if we should actually skip this page,
			// in the event the page blob uploader is sending to a managed disk.
			if u.destPageRangeOptimizer != nil {
				destContainsData = u.destPageRangeOptimizer.doesRangeContainData(
					azblob.PageRange{
						Start: id.OffsetInFile(),
						End:   id.OffsetInFile() + reader.Length() - 1,
					})
			}

			// If neither the source nor destination contain data, it's safe to skip.
			if !destContainsData {
				// for this destination type, there is no need to upload ranges than consist entirely of zeros
				jptm.Log(pipeline.LogDebug,
					fmt.Sprintf("Not uploading range from %d to %d,  all bytes are zero",
						id.OffsetInFile(), id.OffsetInFile()+reader.Length()))
				return
			}
		}

		// control rate of sending (since page blobs can effectively have per-blob throughput limits)
		// Note that this level of control here is specific to the individual page blob, and is additional
		// to the application-wide pacing that we (optionally) do below when writing the response body.
		jptm.LogChunkStatus(id, common.EWaitReason.FilePacer())
		if err := u.filePacer.RequestTrafficAllocation(jptm.Context(), reader.Length()); err != nil {
			jptm.FailActiveUpload("Pacing block", err)
		}

		// send it
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newPacedRequestBody(jptm.Context(), reader, u.pacer)
		enrichedContext := withRetryNotification(jptm.Context(), u.filePacer)
		_, err := u.destPageBlobURL.UploadPages(enrichedContext, id.OffsetInFile(), body, azblob.PageBlobAccessConditions{}, nil)
		if err != nil {
			jptm.FailActiveUpload("Uploading page", err)
			return
		}
	})
}

func (u *pageBlobUploader) Epilogue() {
	jptm := u.jptm

	// set content MD5 (only way to do this is to re-PUT all the headers, this time with the MD5 included)
	if jptm.IsLive() && !u.isInManagedDiskImportExportAccount() {
		tryPutMd5Hash(jptm, u.md5Channel, func(md5Hash []byte) error {
			epilogueHeaders := u.headersToApply
			epilogueHeaders.ContentMD5 = md5Hash
			_, err := u.destPageBlobURL.SetHTTPHeaders(jptm.Context(), epilogueHeaders, azblob.BlobAccessConditions{})
			return err
		})
	}

	u.pageBlobSenderBase.Epilogue()
}

func (u *pageBlobUploader) GetDestinationLength() (int64, error) {
	prop, err := u.destPageBlobURL.GetProperties(u.jptm.Context(), azblob.BlobAccessConditions{})

	if err != nil {
		return -1, err
	}

	return prop.ContentLength(), nil
}
