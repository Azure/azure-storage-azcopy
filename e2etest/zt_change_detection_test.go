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
	"time"
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
			beforeRunJob: func(h hookHelper) {
				ft := h.FromTo()
				if ft.IsS2S() && h.Operation() == eOperation.Copy() {
					// in Copy, s2s change detection is not enabled by default.
					// (Whereas in Sync, it is is, so we don't need to, and cannot, set it.)
					h.GetModifiableParameters().s2sSourceChangeValidation = true
				}
			},
			beforeOpenFirstFile: func(h hookHelper) {
				// Re-create the source files (over top of what AzCopy has already scanned, but has not yet started to transfer)
				// This will give them new LMTs
				time.Sleep(2 * time.Second) // make sure the new LMTs really will be different
				h.CreateFiles(h.GetTestFiles(), true)
			},
		},
		testFiles{
			size:           "1k",
			shouldTransfer: []string{folder("")}, // the root folder should transfer between folder-aware locations
			shouldFail:     []failure{{"filea", "File modified since transfer scheduled"}},
		},
	)
}
