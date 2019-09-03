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
	"github.com/Azure/azure-storage-azcopy/common"
)

// extract the right info from cooked arguments and instantiate a generic copy transfer processor from it
func newRemoveTransferProcessor(cca *cookedCopyCmdArgs, numOfTransfersPerPart int) *copyTransferProcessor {
	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:         cca.jobID,
		CommandString: cca.commandString,
		FromTo:        cca.fromTo,
		SourceRoot:    consolidatePathSeparators(cca.source),

		// authentication related
		CredentialInfo: cca.credentialInfo,
		SourceSAS:      cca.sourceSAS,

		// flags
		LogLevel: cca.logVerbosity,
	}

	reportFirstPart := func() { cca.waitUntilJobCompletion(false) }
	reportFinalPart := func() { cca.isEnumerationComplete = true }

	shouldEncodeSource := cca.fromTo.From().IsRemote()

	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return newCopyTransferProcessor(copyJobTemplate, numOfTransfersPerPart, cca.source, cca.destination,
		shouldEncodeSource, false, reportFirstPart, reportFinalPart)
}
