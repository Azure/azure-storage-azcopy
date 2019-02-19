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
	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

// Creates the right kind of URL to blob copier, based on the blob type of the source
func newURLToBlobCopier(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer, sip ISourceInfoProvider) (ISenderBase, error) {
	srcInfoProvider := sip.(IRemoteSourceInfoProvider) // "downcast" to the type we know it really has

	targetBlobType := azblob.BlobBlockBlob // By default use block blob as destination type

	if blobSrcInfoProvider, ok := srcInfoProvider.(IBlobSourceInfoProvider); ok {
		targetBlobType = blobSrcInfoProvider.BlobType()
	}

	blobTypeOverride := jptm.BlobTypeOverride() // BlobTypeOverride is copy info specified by user

	if blobTypeOverride != common.EBlobType.None() {
		if blobTypeOverride.ToAzBlobType() != targetBlobType {
			targetBlobType = blobTypeOverride.ToAzBlobType()
		}
		if jptm.ShouldLog(pipeline.LogInfo) { // To save fmt.Sprintf
			jptm.LogTransferInfo(
				pipeline.LogInfo,
				srcInfoProvider.RawSource(),
				destination,
				fmt.Sprintf("BlobType has been explictly set to %q for destination blob.", blobTypeOverride))
		}
	}

	if jptm.ShouldLog(pipeline.LogDebug) { // To save fmt.Sprintf, debug level verbose log
		jptm.LogTransferInfo(
			pipeline.LogDebug,
			srcInfoProvider.RawSource(),
			destination,
			fmt.Sprintf("BlobType %q is set for destination blob.", targetBlobType))
	}

	switch targetBlobType {
	case azblob.BlobBlockBlob:
		return newURLToBlockBlobCopier(jptm, destination, p, pacer, srcInfoProvider)
	case azblob.BlobAppendBlob:
		return newURLToAppendBlobCopier(jptm, destination, p, pacer, srcInfoProvider)
	case azblob.BlobPageBlob:
		return newURLToPageBlobCopier(jptm, destination, p, pacer, srcInfoProvider)
	default:
		if jptm.ShouldLog(pipeline.LogDebug) { // To save fmt.Sprintf
			jptm.LogTransferInfo(
				pipeline.LogDebug,
				srcInfoProvider.RawSource(),
				destination,
				fmt.Sprintf("BlobType %q is used for destination blob by default.", azblob.BlobBlockBlob))
		}
		return newURLToBlockBlobCopier(jptm, destination, p, pacer, srcInfoProvider)
	}
}
