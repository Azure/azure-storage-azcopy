// Copyright © Microsoft <wastore@microsoft.com>
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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strings"
)

// isolate the logic to fetch page ranges for a page blob, and check whether a given range has data
// for two purposes:
//  1. capture the necessary info to do so, so that fetchPages can be invoked anywhere
//  2. open to extending the logic, which could be reused for both download and s2s scenarios
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
	return policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
}

func (p *pageRangeOptimizer) fetchPages() {
	// don't fetch page blob list if optimizations are not desired,
	// the lack of page list indicates that there's data everywhere
	if !strings.EqualFold(common.GetEnvironmentVariable(
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
	pager := p.srcPageBlobClient.NewGetPageRangesPager(&pageblob.GetPageRangesOptions{})

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
		} else {
			// if at any point, we fail to get the page list, we just give up and assume there's data everywhere
			p.srcPageList = nil
			break
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
