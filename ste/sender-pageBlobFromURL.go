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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/pacer"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type urlToPageBlobCopier struct {
	pageBlobSenderBase

	srcURL                   string
	sourcePageRangeOptimizer *pageRangeOptimizer // nil if src is not a page blob
	addFileRequestIntent     bool
}

func newURLToPageBlobCopier(jptm IJobPartTransferMgr, destination string, pacer pacer.Interface, srcInfoProvider IRemoteSourceInfoProvider) (s2sCopier, error) {
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

			// capture the necessary info so that we can perform optimizations later
			// This is strictly an optimization, and not a necessity. We ignore
			// any errors here.
			s, err := jptm.SrcServiceClient().BlobServiceClient()
			if err != nil {
				return nil, err
			}

			pbClient := s.NewContainerClient(jptm.Info().SrcContainer).NewPageBlobClient(jptm.Info().SrcFilePath)

			if jptm.Info().VersionID != "" {
				pbClient, err = pbClient.WithVersionID(jptm.Info().VersionID)
				if err != nil {
					return nil, err
				}
			} else if jptm.Info().SnapshotID != "" {
				pbClient, err = pbClient.WithSnapshot(jptm.Info().SnapshotID)
				if err != nil {
					return nil, err
				}
			}

			pageRangeOptimizer = newPageRangeOptimizer(
				pbClient, jptm.Context())

		}
	}

	senderBase, err := newPageBlobSenderBase(jptm, destination, pacer, srcInfoProvider, destBlobTier)
	if err != nil {
		return nil, err
	}

	// Check if source is File and using OAuth (no SAS)
	intentBool := false
	if _, ok := srcInfoProvider.(*fileSourceInfoProvider); ok {
		sUrl, _ := file.ParseURL(srcURL)
		intentBool = sUrl.SAS.Signature() == ""
	}
	return &urlToPageBlobCopier{
		pageBlobSenderBase:       *senderBase,
		srcURL:                   srcURL,
		sourcePageRangeOptimizer: pageRangeOptimizer,
		addFileRequestIntent:     intentBool}, nil
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
		<-c.pacer.InitiateUnpaceable(adjustedChunkSize, c.jptm.Context())
		token, err := c.jptm.GetS2SSourceTokenCredential(c.jptm.Context())
		if err != nil {
			c.jptm.FailActiveS2SCopy("Getting source token credential", err)
			return
		}
		options := &pageblob.UploadPagesFromURLOptions{
			CPKInfo:                 c.jptm.CpkInfo(),
			CPKScopeInfo:            c.jptm.CpkScopeInfo(),
			CopySourceAuthorization: token,
		}

		// Informs SDK to add xms-file-request-intent header
		if c.addFileRequestIntent {
			intentBackup := blob.FileRequestIntentTypeBackup
			options.FileRequestIntent = &intentBackup
		}
		_, err = c.destPageBlobClient.UploadPagesFromURL(enrichedContext, c.srcURL, id.OffsetInFile(), id.OffsetInFile(),
			adjustedChunkSize, options)
		if err != nil {
			c.jptm.FailActiveS2SCopy("Uploading page from URL", err)
			return
		}
	})
}
