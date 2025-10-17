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

package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func setPropertiesTransferProcessor(cca *CookedCopyCmdArgs, numOfTransfersPerPart int, fpo common.FolderPropertyOption, targetServiceClient *common.ServiceClient) *copyTransferProcessor {
	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:               cca.jobID,
		CommandString:       cca.commandString,
		FromTo:              cca.FromTo,
		Fpo:                 fpo,
		SymlinkHandlingType: common.ESymlinkHandlingType.Preserve(), // we want to set properties on symlink blobs
		SourceRoot:          cca.Source.CloneWithConsolidatedSeparators(),
		CredentialInfo:      cca.credentialInfo,
		ForceIfReadOnly:     cca.ForceIfReadOnly,
		SrcServiceClient:    targetServiceClient,

		// flags
		LogLevel: LogLevel,
		BlobAttributes: common.BlobTransferAttributes{
			BlockBlobTier:     cca.blockBlobTier,
			PageBlobTier:      cca.pageBlobTier,
			Metadata:          cca.metadata,
			BlobTagsString:    cca.blobTagsMap.ToString(),
			RehydratePriority: cca.rehydratePriority,
		},
		SetPropertiesFlags: cca.propertiesToTransfer,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: cca.trailingDot,
		},
		JobErrorHandler: glcm,
	}

	reportFirstPart := func(jobStarted bool) {
		if jobStarted {
			cca.waitUntilJobCompletion(false)
		}
	}
	reportFinalPart := func() { cca.isEnumerationComplete = true }

	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return newCopyTransferProcessor(copyJobTemplate, numOfTransfersPerPart, cca.Source, cca.Destination,
		reportFirstPart, reportFinalPart, false, cca.dryrunMode)
}
