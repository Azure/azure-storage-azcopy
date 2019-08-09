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

type urlToAppendBlobCopier struct {
	appendBlobSenderBase

	srcURL url.URL
}

func newURLToAppendBlobCopier(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
	senderBase, err := newAppendBlobSenderBase(jptm, destination, p, pacer, srcInfoProvider)
	if err != nil {
		return nil, err
	}

	srcURL, err := srcInfoProvider.PreSignedSourceURL()
	if err != nil {
		return nil, err
	}

	return &urlToAppendBlobCopier{
		appendBlobSenderBase: *senderBase,
		srcURL:               *srcURL}, nil
}

// Returns a chunk-func for blob copies
func (c *urlToAppendBlobCopier) GenerateCopyFunc(id common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	appendBlockFromURL := func() {
		c.jptm.LogChunkStatus(id, common.EWaitReason.S2SCopyOnWire())

		// Set the latest service version from sdk as service version in the context, to use AppendBlockFromURL API.
		ctxWithLatestServiceVersion := context.WithValue(c.jptm.Context(), ServiceAPIVersionOverride, azblob.ServiceVersion)

		if err := c.pacer.RequestTrafficAllocation(c.jptm.Context(), adjustedChunkSize); err != nil {
			c.jptm.FailActiveUpload("Pacing block", err)
		}
		_, err := c.destAppendBlobURL.AppendBlockFromURL(ctxWithLatestServiceVersion, c.srcURL, id.OffsetInFile(), adjustedChunkSize,
			azblob.AppendBlobAccessConditions{
				AppendPositionAccessConditions: azblob.AppendPositionAccessConditions{IfAppendPositionEqual: id.OffsetInFile()},
			}, azblob.ModifiedAccessConditions{}, nil)
		if err != nil {
			c.jptm.FailActiveS2SCopy("Appending block from URL", err)
			return
		}
	}

	return c.generateAppendBlockToRemoteFunc(id, appendBlockFromURL)
}

// GetDestinationLength gets the destination length.
func (c *urlToAppendBlobCopier) GetDestinationLength() (int64, error) {
	properties, err := c.destAppendBlobURL.GetProperties(c.jptm.Context(), azblob.BlobAccessConditions{})
	if err != nil {
		return -1, err
	}

	return properties.ContentLength(), nil
}
