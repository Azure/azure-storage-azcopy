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
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

// upload related
const UploadMaxTries = 20
const UploadTryTimeout = time.Minute * 15
const UploadRetryDelay = time.Second * 1
const UploadMaxRetryDelay = time.Second * 60

var ADLSFlushThreshold uint32 = 7500 // The # of blocks to flush at a time-- Implemented only for CI.

// download related
const MaxRetryPerDownloadBody = 5

// TODO: consider to unify the retry options.
const DownloadTryTimeout = time.Minute * 15
const DownloadRetryDelay = time.Second * 1
const DownloadMaxRetryDelay = time.Second * 60

// pacer related
const PacerTimeToWaitInMs = 50

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// These types are define the STE Coordinator
type newJobXfer func(jptm IJobPartTransferMgr, pipeline pipeline.Pipeline, pacer pacer)

// same as newJobXfer, but with an extra parameter
type newJobXferWithDownloaderFactory = func(jptm IJobPartTransferMgr, pipeline pipeline.Pipeline, pacer pacer, df downloaderFactory)
type newJobXferWithSenderFactory = func(jptm IJobPartTransferMgr, pipeline pipeline.Pipeline, pacer pacer, sf senderFactory, sipf sourceInfoProviderFactory)

// Takes a multi-purpose download function, and makes it ready to user with a specific type of downloader
func parameterizeDownload(targetFunction newJobXferWithDownloaderFactory, df downloaderFactory) newJobXfer {
	return func(jptm IJobPartTransferMgr, pipeline pipeline.Pipeline, pacer pacer) {
		targetFunction(jptm, pipeline, pacer, df)
	}
}

// Takes a multi-purpose send function, and makes it ready to use with a specific type of sender
func parameterizeSend(targetFunction newJobXferWithSenderFactory, sf senderFactory, sipf sourceInfoProviderFactory) newJobXfer {
	return func(jptm IJobPartTransferMgr, pipeline pipeline.Pipeline, pacer pacer) {
		targetFunction(jptm, pipeline, pacer, sf, sipf)
	}
}

// the xfer factory is generated based on the type of source and destination
func computeJobXfer(fromTo common.FromTo, blobType common.BlobType) newJobXfer {
	switch fromTo {
	case common.EFromTo.BlobLocal(): // download from Azure Blob to local file system
		return parameterizeDownload(remoteToLocal, newBlobDownloader)
	case common.EFromTo.LocalBlob(): // upload from local file system to Azure blob
		switch blobType {
		case common.EBlobType.None(),
			common.EBlobType.BlockBlob():
			return parameterizeSend(anyToRemote, newBlockBlobUploader, newLocalSourceInfoProvider)
		case common.EBlobType.PageBlob():
			return parameterizeSend(anyToRemote, newPageBlobUploader, newLocalSourceInfoProvider)
		case common.EBlobType.AppendBlob():
			return parameterizeSend(anyToRemote, newAppendBlobUploader, newLocalSourceInfoProvider)
		}
	case common.EFromTo.BlobTrash():
		return DeleteBlobPrologue
	case common.EFromTo.FileLocal(): // download from Azure File to local file system
		return parameterizeDownload(remoteToLocal, newAzureFilesDownloader)
	case common.EFromTo.LocalFile(): // upload from local file system to Azure File
		return parameterizeSend(anyToRemote, newAzureFilesUploader, newLocalSourceInfoProvider)
	case common.EFromTo.FileTrash():
		return DeleteFilePrologue
	case common.EFromTo.LocalBlobFS():
		return parameterizeSend(anyToRemote, newBlobFSUploader, newLocalSourceInfoProvider)
	case common.EFromTo.BlobFSLocal():
		return parameterizeDownload(remoteToLocal, newBlobFSDownloader)
	case common.EFromTo.BlobBlob():
		return parameterizeSend(anyToRemote, newURLToBlobCopier, newBlobSourceInfoProvider)
	case common.EFromTo.FileBlob():
		return parameterizeSend(anyToRemote, newURLToBlobCopier, newFileSourceInfoProvider)
	case common.EFromTo.S3Blob():
		return parameterizeSend(anyToRemote, newURLToBlobCopier, newS3SourceInfoProvider)
	}
	panic(fmt.Errorf("Unrecognized from-to: %q", fromTo.String()))
}
