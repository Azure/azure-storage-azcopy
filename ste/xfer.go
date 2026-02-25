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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/pacer"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// upload related
const UploadMaxTries = 20
const UploadRetryDelay = time.Second * 1
const UploadMaxRetryDelay = time.Second * 60

var UploadTryTimeout = time.Minute * 15
var ADLSFlushThreshold uint32 = 7500 // The # of blocks to flush at a time-- Implemented only for CI.

// download related
const MaxRetryPerDownloadBody = 5

// TODO: consider to unify the retry options.
const DownloadTryTimeout = time.Minute * 15
const DownloadRetryDelay = time.Second * 1
const DownloadMaxRetryDelay = time.Second * 60

// pacer related
const PacerTimeToWaitInMs = 50

// CPK logging related.
// Sync.Once is used so we only log a CPK error once and prevent gumming up stdout
var cpkAccessFailureLogGLCM sync.Once

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// These types are define the STE Coordinator
type newJobXfer func(jptm IJobPartTransferMgr, pacer pacer.Interface)

// same as newJobXfer, but with an extra parameter
type newJobXferWithDownloaderFactory = func(jptm IJobPartTransferMgr, pacer pacer.Interface, df downloaderFactory)
type newJobXferWithSenderFactory = func(jptm IJobPartTransferMgr, pacer pacer.Interface, sf senderFactory, sipf sourceInfoProviderFactory)

// Takes a multi-purpose download function, and makes it ready to user with a specific type of downloader
func parameterizeDownload(targetFunction newJobXferWithDownloaderFactory, df downloaderFactory) newJobXfer {
	return func(jptm IJobPartTransferMgr, pacer pacer.Interface) {
		targetFunction(jptm, pacer, df)
	}
}

// Takes a multi-purpose send function, and makes it ready to use with a specific type of sender
func parameterizeSend(targetFunction newJobXferWithSenderFactory, sf senderFactory, sipf sourceInfoProviderFactory) newJobXfer {
	return func(jptm IJobPartTransferMgr, pacer pacer.Interface) {
		targetFunction(jptm, pacer, sf, sipf)
	}
}

// the xfer factory is generated based on the type of source and destination
func computeJobXfer(fromTo common.FromTo, blobType common.BlobType) newJobXfer {

	//local helper functions

	getDownloader := func(sourceType common.Location) downloaderFactory {
		switch sourceType {
		case common.ELocation.Blob():
			return newBlobDownloader
		case common.ELocation.File(), common.ELocation.FileNFS():
			return newAzureFilesDownloader
		case common.ELocation.BlobFS():
			return newBlobFSDownloader
		default:
			panic("unexpected source type")
		}
	}

	getSenderFactory := func(fromTo common.FromTo) senderFactory {
		isFromRemote := fromTo.From().IsRemote()
		if isFromRemote {
			// sending from remote = doing an S2S copy
			switch fromTo.To() {
			case common.ELocation.Blob(),
				common.ELocation.S3(), common.ELocation.GCP():
				return newURLToBlobCopier
			case common.ELocation.File(), common.ELocation.FileNFS():
				return newURLToAzureFileCopier
			case common.ELocation.BlobFS():
				return newURLToBlobCopier
			default:
				panic("unexpected target location type")
			}
		} else {
			// we are uploading
			switch fromTo.To() {
			case common.ELocation.Blob():
				return newBlobUploader
			case common.ELocation.File(), common.ELocation.FileNFS():
				return newAzureFilesUploader
			case common.ELocation.BlobFS():
				return newBlobFSUploader
			default:
				panic("unexpected target location type")
			}
		}
	}

	getSipFactory := func(sourceType common.Location) sourceInfoProviderFactory {
		switch sourceType {
		case common.ELocation.Local():
			return newLocalSourceInfoProvider
		case common.ELocation.Benchmark():
			return newBenchmarkSourceInfoProvider
		case common.ELocation.Blob():
			return newBlobSourceInfoProvider
		case common.ELocation.File(), common.ELocation.FileNFS():
			return newFileSourceInfoProvider
		case common.ELocation.BlobFS():
			return newBlobSourceInfoProvider // Blob source info provider pulls info from blob and dfs
		case common.ELocation.S3():
			return newS3SourceInfoProvider
		case common.ELocation.GCP():
			return newGCPSourceInfoProvider
		default:
			panic("unexpected source type")
		}
	}

	// main computeJobXfer logic
	switch fromTo {
	case common.EFromTo.BlobTrash():
		return DeleteBlob
	case common.EFromTo.FileTrash():
		return DeleteFile
	case common.EFromTo.BlobFSTrash():
		return DeleteHNSResource
	case common.EFromTo.BlobNone(), common.EFromTo.BlobFSNone(), common.EFromTo.FileNone():
		return SetProperties
	default:
		if fromTo.IsDownload() {
			return parameterizeDownload(remoteToLocal, getDownloader(fromTo.From()))
		} else {
			return parameterizeSend(anyToRemote, getSenderFactory(fromTo), getSipFactory(fromTo.From()))
		}
	}
}

var inferExtensions = map[string]blob.BlobType{
	".vhd":  blob.BlobTypePageBlob,
	".vhdx": blob.BlobTypePageBlob,
}

// infers a blob type from the extension specified.
func inferBlobType(filename string, defaultBlobType blob.BlobType) blob.BlobType {
	if b, ok := inferExtensions[strings.ToLower(filepath.Ext(filename))]; ok {
		return b
	}

	return defaultBlobType
}

func init() {
	requestTryTimeout := common.GetEnvironmentVariable(common.EEnvironmentVariable.RequestTryTimeout())
	if requestTryTimeout != "" {
		timeout, err := time.ParseDuration(requestTryTimeout + "m")
		if err == nil {
			UploadTryTimeout = timeout
		}
	}
}
