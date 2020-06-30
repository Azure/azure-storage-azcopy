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
	"github.com/Azure/azure-storage-azcopy/common"
	"testing"
)

// Purpose: Other tests for enumeration of sources, NOT including filtering

func TestEnumeration_DirectoryStubsAreNotDownloaded(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.Specific(common.EFromTo.BlobLocal()), // TODO: does this apply to any other cases
		eValidate.TransferStates(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldIgnore: []interface{}{
				f("dir", withDirStubMetadata{}),
			},
			shouldTransfer: []interface{}{
				"filea",
				folder("dir"),
				"dir/fileb",
			},
		})
}
