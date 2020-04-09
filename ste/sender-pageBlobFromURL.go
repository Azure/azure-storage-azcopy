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
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/Azure/azure-storage-azcopy/common"
)

type urlToPageBlobCopier struct {
	pageBlobSenderBase

	srcURL                   url.URL
	sourcePageRangeOptimizer *pageRangeOptimizer // nil if src is not a page blob
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	destBlobTier := azblob.AccessTierNone
	var pageRangeOptimizer *pageRangeOptimizer
	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == azblob.BlobPageBlob {
			// if the source is page blob, preserve source's blob tier.
			destBlobTier = blobSrcInfoProvider.BlobTier()

			// capture the necessary info so that we can perform optimizations later
			pageRangeOptimizer = newPageRangeOptimizer(azblob.NewPageBlobURL(*srcURL, p),
				context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion))
		}
	}

	senderBase, err := newPageBlobSenderBase(jptm, destination, p, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	return &urlToPageBlobCopier{
		pageBlobSenderBase:       *senderBase,
		srcURL:                   *srcURL,
		sourcePageRangeOptimizer: pageRangeOptimizer}, nil
}

func (c *urlToPageBlobCopier) Prologue(ps common.PrologueState) (destinationModified bool) {
	destinationModified = c.pageBlobSenderBase.Prologue(ps)

	if c.sourcePageRangeOptimizer != nil {
		c.sourcePageRangeOptimizer.fetchPages()
	}

	return
}

// Returns a chunk-func for blob copies
func (c *urlToPageBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(c.jptm, id, func() {
		if c.jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		// if there's no data at the source (and the destination for managed disks), skip this chunk
		pageRange := azblob.PageRange{Start: id.OffsetInFile(), End: id.OffsetInFile() + adjustedChunkSize - 1}
		if c.sourcePageRangeOptimizer != nil && !c.sourcePageRangeOptimizer.doesRangeContainData(pageRange) {
			var destContainsData bool

			if c.destPageRangeOptimizer != nil {
				destContainsData = c.destPageRangeOptimizer.doesRangeContainData(pageRange)
			}

			if !destContainsData {
				return
			}
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

// isolate the logic to fetch page ranges for a page blob, and check whether a given range has data
// for two purposes:
//	1. capture the necessary info to do so, so that fetchPages can be invoked anywhere
//  2. open to extending the logic, which could be re-used for both download and s2s scenarios
type pageRangeOptimizer struct {
	srcPageBlobURL azblob.PageBlobURL
	ctx            context.Context
	srcPageList    *azblob.PageList // nil if src is not a page blob, or it was not possible to get a response
}

func newPageRangeOptimizer(srcPageBlobURL azblob.PageBlobURL, ctx context.Context) *pageRangeOptimizer {
	return &pageRangeOptimizer{srcPageBlobURL: srcPageBlobURL, ctx: ctx}
}

func (p *pageRangeOptimizer) fetchPages() {
	// don't fetch page blob list if optimizations are not desired,
	// the lack of page list indicates that there's data everywhere
	if !strings.EqualFold(common.GetLifecycleMgr().GetEnvironmentVariable(
		common.EEnvironmentVariable.OptimizeSparsePageBlobTransfers()), "true") {
		return
	}

	// according to the REST API documentation:
	// in a highly fragmented page blob with a large number of writes,
	// a Get Page Ranges request can fail due to an internal server timeout.
	// thus, if the page blob is not sparse, it's ok for it to fail
	// TODO follow up with the service folks to confirm the scale at which the timeouts occur
	// TODO perhaps we need to add more logic here to optimize for more cases
	limitedContext := withNoRetryForBlob(p.ctx) // we don't want retries here. If it doesn't work the first time, we don't want to chew up (lots) time retrying
	pageList, err := p.srcPageBlobURL.GetPageRanges(limitedContext, 0, 0, azblob.BlobAccessConditions{})
	if err == nil {
		p.srcPageList = pageList
	}
}

// check whether a particular given range is worth transferring, i.e. whether there's data at the source
func (p *pageRangeOptimizer) doesRangeContainData(givenRange azblob.PageRange) bool {
	// if we have no page list stored, then assume there's data everywhere
	// (this is particularly important when we are using this code not just for performance, but also
	// for correctness - as we do when using on the destination of a managed disk upload)
	if p.srcPageList == nil {
		return true
	}

	// note that the page list is ordered in increasing order (in terms of position)
	for _, srcRange := range p.srcPageList.PageRange {
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
