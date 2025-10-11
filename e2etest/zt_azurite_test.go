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

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestAzurite_Local(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.LocalBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1"),
				folder("test"),
				folder("test/dir1"),
				folder("test/dir1/dir2"),
				f("test/file2"),
				f("test/dir1/file3"),
				f("test/dir1/dir2/file4"),
				folder("test/dir3"),
			},
		}, EAccountType.Azurite(), EAccountType.Azurite(), "")
}

// Note: S2S is not really supported by Azurite.

func TestAzurite_Remove(t *testing.T) {
	RunScenarios(t, eOperation.Remove(), eTestFromTo.Other(common.EFromTo.BlobTrash()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1"),
			folder("test"),
			folder("test/dir1"),
			folder("test/dir1/dir2"),
			f("test/file2"),
			f("test/dir1/file3"),
			f("test/dir1/dir2/file4"),
			folder("test/dir3"),
		},
	}, EAccountType.Azurite(), EAccountType.Azurite(), "")
}
