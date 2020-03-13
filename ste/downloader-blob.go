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

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type blobDownloader struct {
	// filePacer is necessary because page blobs have per-blob throughput limits. The limits depend on
	// what type of page blob it is (e.g. premium) and can be significantly lower than the blob account limit.
	// Using a automatic pacer here lets us find the right rate for this particular page blob, at which
	// we won't be trying to move the faster than the Service wants us to.
	filePacer autopacer

	// used to avoid downloading zero ranges of page blobs
	pageRangeOptimizer *pageRangeOptimizer
}

func newBlobDownloader() downloader {
	return &blobDownloader{
		filePacer: newNullAutoPacer(), // defer creation of real one, if needed, to Prologue
	}

}

func (bd *blobDownloader) Prologue(jptm IJobPartTransferMgr, srcPipeline pipeline.Pipeline) {
	if jptm.Info().SrcBlobType == azblob.BlobPageBlob {
		// page blobs need a file-specific pacer
		// See comments in uploader-pageBlob for the reasons, since the same reasons apply are are explained there
		bd.filePacer = newPageBlobAutoPacer(pageBlobInitialBytesPerSecond, jptm.Info().BlockSize, false, jptm.(common.ILogger))

		u, _ := url.Parse(jptm.Info().Source)
		bd.pageRangeOptimizer = newPageRangeOptimizer(azblob.NewPageBlobURL(*u, srcPipeline),
			context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion))
		bd.pageRangeOptimizer.fetchPages()
	}
}

func (bd *blobDownloader) Epilogue() {
	_ = bd.filePacer.Close()
}

// Returns a chunk-func for blob downloads
func (bd *blobDownloader) GenerateDownloadFunc(jptm IJobPartTransferMgr, srcPipeline pipeline.Pipeline, destWriter common.ChunkedFileWriter, id common.ChunkID, length int64, pacer pacer) chunkFunc {
	return createDownloadChunkFunc(jptm, id, func() {

		// If the range does not contain any data, write out empty data to disk without performing download
		if bd.pageRangeOptimizer != nil && !bd.pageRangeOptimizer.doesRangeContainData(
			azblob.PageRange{Start: id.OffsetInFile(), End: id.OffsetInFile() + length - 1}) {

			// queue an empty chunk
			err := destWriter.EnqueueChunk(jptm.Context(), id, length, dummyReader{}, false)
			if err != nil {
				jptm.FailActiveDownload("Enqueuing chunk", err)
			}
			return
		}

		// Control rate of data movement (since page blobs can effectively have per-blob throughput limits)
		// Note that this level of control here is specific to the individual page blob, and is additional
		// to the application-wide pacing that we (optionally) do below when reading the response body.
		// Note also that the resulting throughput is somewhat ragged for downloads, and does not track the
		// pacer's target rate as closely as it does for uploads. Presumably this is just because its
		// hard to accurately control throughput from the receiving end. I.e. not a pacer bug, but just
		// something inherent in the nature of REST downloads. So, as at March 2018, we are just living
		// with it as known issue when downloading paced blobs.
		jptm.LogChunkStatus(id, common.EWaitReason.FilePacer())
		if err := bd.filePacer.RequestTrafficAllocation(jptm.Context(), length); err != nil {
			jptm.FailActiveDownload("Pacing block", err)
		}

		// download blob from start Index till startIndex + adjustedChunkSize
		info := jptm.Info()
		u, _ := url.Parse(info.Source)
		srcBlobURL := azblob.NewBlobURL(*u, srcPipeline)
		isNewStyleImpExp := isInManagedDiskImportExportAccount(*u)
		isOldStyleDiskExport := isInLegacyDiskExportAccount(*u)

		// set access conditions, to protect against inconsistencies from changes-while-being-read
		accessConditions := azblob.BlobAccessConditions{ModifiedAccessConditions: azblob.ModifiedAccessConditions{IfUnmodifiedSince: jptm.LastModifiedTime()}}
		if isNewStyleImpExp || isOldStyleDiskExport {
			// no access conditions (and therefore no if-modified checks) are supported on managed disk import/export (md-impexp)
			// They are also unsupported on old "md-" style export URLs on the new (2019) large size disks.
			// And if fact you can't have an md- URL in existence if the blob is mounted as a disk, so it won't be getting changed anyway, so we just treat all md-disks the same
			accessConditions = azblob.BlobAccessConditions{}
		}

		// At this point we create an HTTP(S) request for the desired portion of the blob, and
		// wait until we get the headers back... but we have not yet read its whole body.
		// The Download method encapsulates any retries that may be necessary to get to the point of receiving response headers.
		jptm.LogChunkStatus(id, common.EWaitReason.HeaderResponse())
		enrichedContext := withRetryNotification(jptm.Context(), bd.filePacer)
		get, err := srcBlobURL.Download(enrichedContext, id.OffsetInFile(), length, accessConditions, false)
		if err != nil {
			jptm.FailActiveDownload("Downloading response body", err) // cancel entire transfer because this chunk has failed
			return
		}

		// Enqueue the response body to be written out to disk
		// The retryReader encapsulates any retries that may be necessary while downloading the body
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		retryReader := get.Body(azblob.RetryReaderOptions{
			MaxRetryRequests: destWriter.MaxRetryPerDownloadBody(),
			NotifyFailedRead: common.NewReadLogFunc(jptm, u),
		})
		defer retryReader.Close()
		err = destWriter.EnqueueChunk(jptm.Context(), id, length, newPacedResponseBody(jptm.Context(), retryReader, pacer), true)
		if err != nil {
			jptm.FailActiveDownload("Enqueuing chunk", err)
			return
		}
	})
}

type dummyReader struct{}

func (dummyReader) Read(p []byte) (n int, err error) {
	return len(p), nil
}
