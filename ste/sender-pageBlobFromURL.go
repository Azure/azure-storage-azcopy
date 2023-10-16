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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type urlToPageBlobCopier struct {
	pageBlobSenderBase

	srcURL                   string
	sourcePageRangeOptimizer *pageRangeOptimizer // nil if src is not a page blob
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, destination string, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	var destBlobTier *blob.AccessTier
	var pageRangeOptimizer *pageRangeOptimizer
	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		if blobSrcInfoProvider.BlobType() == blob.BlobTypePageBlob {
			// if the source is page blob, preserve source's blob tier.
			destBlobTier = blobSrcInfoProvider.BlobTier()
			srcPageBlobClient := common.CreatePageBlobClient(srcURL, jptm.S2SSourceCredentialInfo(), jptm.CredentialOpOptions(), jptm.ClientOptions())

			// capture the necessary info so that we can perform optimizations later
			pageRangeOptimizer = newPageRangeOptimizer(srcPageBlobClient, jptm.Context())
		}
	}

	senderBase, err := newPageBlobSenderBase(jptm, destination, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	return &urlToPageBlobCopier{
		pageBlobSenderBase:       *senderBase,
		srcURL:                   srcURL,
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
		pageRange := pageblob.PageRange{Start: to.Ptr(id.OffsetInFile()), End: to.Ptr(id.OffsetInFile() + adjustedChunkSize - 1)}
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
			c.jptm.Context(),
			c.filePacer)

		// upload the page (including application of global pacing. We don't have a separate wait reason for global pacing
		// so just do it inside the S2SCopyOnWire state)
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())
		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block (global level)", err)
		}
		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}
		_, err = c.destPageBlobClient.UploadPagesFromURL(enrichedContext, c.srcURL, id.OffsetInFile(), id.OffsetInFile(), adjustedChunkSize,
			&pageblob.UploadPagesFromURLOptions{
				CPKInfo:                 c.jptm.CpkInfo(),
				CPKScopeInfo:            c.jptm.CpkScopeInfo(),
				CopySourceAuthorization: token,
			})
		if err != nil {
			c.jptm.FailActiveS2SCopy("Uploading page from URL", err)
			return
		}
	})
}

// isolate the logic to fetch page ranges for a page blob, and check whether a given range has data
// for two purposes:
//	1. capture the necessary info to do so, so that fetchPages can be invoked anywhere
//  2. open to extending the logic, which could be re-used for both download and s2s scenarios
type pageRangeOptimizer struct {
	srcPageBlobClient *pageblob.Client
	ctx               context.Context
	srcPageList       *pageblob.PageList // nil if src is not a page blob, or it was not possible to get a response
}

func newPageRangeOptimizer(srcPageBlobClient *pageblob.Client, ctx context.Context) *pageRangeOptimizer {
	return &pageRangeOptimizer{srcPageBlobClient: srcPageBlobClient, ctx: ctx}
}

// withNoRetryForBlob returns a context that contains a marker to say we don't want any retries to happen
// Is only implemented for blob pipelines at present
func withNoRetryForBlob(ctx context.Context) context.Context {
	return runtime.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: 1})
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
	pager := p.srcPageBlobClient.NewGetPageRangesPager(nil)

	for pager.More() {
		pageList, err := pager.NextPage(limitedContext)
		if err == nil {
			if p.srcPageList == nil {
				p.srcPageList = &pageList.PageList
			} else {
				p.srcPageList.PageRange = append(p.srcPageList.PageRange, pageList.PageRange...)
				p.srcPageList.ClearRange = append(p.srcPageList.ClearRange, pageList.ClearRange...)
				p.srcPageList.NextMarker = pageList.NextMarker
			}
		}
	}
}

// check whether a particular given range is worth transferring, i.e. whether there's data at the source
func (p *pageRangeOptimizer) doesRangeContainData(givenRange pageblob.PageRange) bool {
	// if we have no page list stored, then assume there's data everywhere
	// (this is particularly important when we are using this code not just for performance, but also
	// for correctness - as we do when using on the destination of a managed disk upload)
	if p.srcPageList == nil {
		return true
	}

	// note that the page list is ordered in increasing order (in terms of position)
	for _, srcRange := range p.srcPageList.PageRange {
		if *givenRange.End < *srcRange.Start {
			// case 1: due to the nature of the list (it's sorted), if we've reached such a srcRange
			// we've checked all the appropriate srcRange already and haven't found any overlapping srcRange
			// given range:		|   |
			// source range:			|   |
			return false
		} else if *srcRange.End < *givenRange.Start {
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
