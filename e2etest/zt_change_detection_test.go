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

package e2etest

import (
	"testing"
)

// Purpose: Tests for detecting that source has been changed during transfer

// TestDetectFileChangedDuringTransfer tests that we can detect files changed during transfer, for all supported
// pairwise source-dest combinations.
// We test all pairs here because change detection depends on both the source info provider and the xfer-... code.
// The latter differs between upload and download.
func TestDetectFileChangedDuringTransfer(t *testing.T) {
	RunScenarios(t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllPairs(),
		eValidate.TransferStates(),
		nil,
		nil,
		params{
			recursive: true,
		},
		&hooks{
			beforeOpenFirstFile: func(h hookHelper) {
				// Re-create the source files (over top of what AzCopy has already scanned, but has not yet started to transfer)
				// This will give them new LMTs
				h.CreateFiles(h.GetTestFiles(), true)
			},
			// We don't use the beforeRunJob hook here, because that doesn't allow us to time the change exactly after scanning starts,
			// but before the file gets read.
		},
		testFiles{
			size:       "1k",
			shouldFail: []failure{{"filea", "File modified since transfer scheduled"}},
		},
	)
}
