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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
)

type blobDownloader struct{}

func newBlobDownloader() downloader {
	return &blobDownloader{}
}

// Returns a chunk-func for blob downloads
func (bd *blobDownloader) GenerateDownloadFunc(jptm IJobPartTransferMgr, srcPipeline pipeline.Pipeline, destWriter common.ChunkedFileWriter, id common.ChunkID, length int64, pacer *pacer) chunkFunc {
	return createDownloadChunkFunc(jptm, id, func() {

		// Step 1: Download blob from start Index till startIndex + adjustedChunkSize
		info := jptm.Info()
		u, _ := url.Parse(info.Source)
		srcBlobURL := azblob.NewBlobURL(*u, srcPipeline)
		// At this point we create an HTTP(S) request for the desired portion of the blob, and
		// wait until we get the headers back... but we have not yet read its whole body.
		// The Download method encapsulates any retries that may be necessary to get to the point of receiving response headers.
		jptm.LogChunkStatus(id, common.EWaitReason.HeaderResponse())
		get, err := srcBlobURL.Download(jptm.Context(), id.OffsetInFile, length, azblob.BlobAccessConditions{}, false)
		if err != nil {
			jptm.FailActiveDownload("Downloading response body", err) // cancel entire transfer because this chunk has failed
			return
		}

		// step 2: Enqueue the response body to be written out to disk
		// The retryReader encapsulates any retries that may be necessary while downloading the body
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		retryReader := get.Body(azblob.RetryReaderOptions{
			MaxRetryRequests: destWriter.MaxRetryPerDownloadBody(),
			NotifyFailedRead: common.NewReadLogFunc(jptm, u),
		})
		defer retryReader.Close()
		err = destWriter.EnqueueChunk(jptm.Context(), id, length, newLiteResponseBodyPacer(retryReader, pacer), true)
		if err != nil {
			jptm.FailActiveDownload("Enqueuing chunk", err)
			return
		}
	})
}
