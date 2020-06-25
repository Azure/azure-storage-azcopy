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
			recursive:   true,
			capMbps:     1,    // go really slow, so that the transfer will last long enough for our other thread to change the file while its running
			blockSizeMB: 0.25, // small block size, so that the cap works better (since capMbps is coarse-grained when running S2S)
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// use separate Goroutine, so that job will start while our goroutine is still running
				go func() {
					// wait a moment, then re-create the source files (over top of what AzCopy will be  already trying to transfer)
					time.Sleep(5 * time.Second)
					h.CreateFiles(h.GetTestFiles(), true) // force the files to change
				}()
			},
		},
		testFiles{
			size:       "20M",
			shouldFail: []failure{{"filea", "File modified since transfer scheduled"}},
		},
	)
}
