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
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

type urlToPageBlobCopier struct {
	pageBlobSenderBase

	srcURL      url.URL
	srcPageList *azblob.PageList
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	destBlobTier := azblob.AccessTierNone
	var srcPageList *azblob.PageList
	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == azblob.BlobPageBlob {
			// if the source is page blob, preserve source's blob tier.
			destBlobTier = blobSrcInfoProvider.BlobTier()

			// also get the page ranges so that we can skip the empty parts at the expense of one HTTP request
			srcPageBlobURL := azblob.NewPageBlobURL(*srcURL, p)
			ctx := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)

			// ignore the error, since according to the REST API documentation:
			// in a highly fragmented page blob with a large number of writes,
			// a Get Page Ranges request can fail due to an internal server timeout.
			// thus, if the page blob is not sparse, it's ok for it to fail
			srcPageList, _ = srcPageBlobURL.GetPageRanges(ctx, 0, 0, azblob.BlobAccessConditions{})
		}
	}

	senderBase, err := newPageBlobSenderBase(jptm, destination, p, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	return &urlToPageBlobCopier{
		pageBlobSenderBase: *senderBase,
		srcURL:             *srcURL,
		srcPageList:        srcPageList}, nil
}

// Returns a chunk-func for blob copies
func (c *urlToPageBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		if c.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// if there's no data at the source, skip this chunk
		if !c.doesRangeContainData(azblob.PageRange{Start: id.OffsetInFile(), End: id.OffsetInFile() + adjustedChunkSize - 1}) {
			return
		}

		// control rate of sending (since page blobs can effectively have per-blob throughput limits)
		// Note that this level of control here is specific to the individual page blob, and is additional
		// to the application-wide pacing that we do with c.pacer
		c.jptm.LogChunkStatus(id, common.EWaitReason.FilePacer())
		if err := c.filePacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block (file level)", err)
		}

		// set the latest service version from sdk as service version in the context, to use UploadPagesFromURL API.
		// AND enrich the context for 503 (ServerBusy) detection
		enrichedContext := withRetryNotification(
			context.WithValue(c.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion),
			c.filePacer)

		// upload the page (including application of global pacing. We don't have a separate wait reason for global pacing
		// so just do it inside the S2SCopyOnWire state)
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block (global level)", err)
		}
		_, err := c.destPageBlobURL.UploadPagesFromURL(
			enrichedContext, c.srcURL, id.OffsetInFile(), id.OffsetInFile(), adjustedChunkSize, nil,
			azblob.PageBlobAccessConditions{}, azblob.ModifiedAccessConditions{})
		if err != nil {
			c.jptm.FailActiveS2SCopy("Uploading page from URL", err)
			return
		}
	})
}

// GetDestinationLength gets the destination length.
func (c *urlToPageBlobCopier) GetDestinationLength() (int64, error) {
	properties, err := c.destPageBlobURL.GetProperties(c.jptm.Context(), azblob.BlobAccessConditions{})
	if err != nil {
		return -1, err
	}

	return properties.ContentLength(), nil
}

// check whether a particular given range is worth transferring, i.e. whether there's data at the source
func (c *urlToPageBlobCopier) doesRangeContainData(givenRange azblob.PageRange) bool {
	// if we have no page list stored, then assume there's data everywhere
	if c.srcPageList == nil {
		return true
	}

	// note that the page list is ordered in increasing order (in terms of position)
	for _, srcRange := range c.srcPageList.PageRange {
		if givenRange.End < srcRange.Start {
			// case 1: due to the nature of the list (it's sorted), if we've reached such a srcRange
			// we've checked all the appropriate srcRange already and haven't found any overlapping srcRange
			// given range:		|   |
			// source range:			|   |
			return false
		} else if srcRange.End < givenRange.Start {
			// case 2: the givenRange comes after srcRange, continue checking
			// given range:				|   |
			// source range:	|   |
			continue
		} else {
			// case 3: srcRange and givenRange overlap somehow
			// we don't particularly care how it overlaps
			return true
		}
	}

	// went through all srcRanges, but nothing overlapped
	return false
}
