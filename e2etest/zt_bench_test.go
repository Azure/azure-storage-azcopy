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

// Purpose: Tests benchmark to validate that AzCopy doesn't crash. Note: This does not validate the benchmark results.

func TestBench_Upload(t *testing.T) {
	blobBench := TestFromTo{
		desc:      "BlobBench",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	// Note: The fromTo is not technically correct, but if you set it to LocalBlob, the runner will try to use the local path as the param
	RunScenarios(t, eOperation.Benchmark(), blobBench, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:   true,
		mode:        "Upload",
		fileCount:   50,
		sizePerFile: "128M",
	}, nil, testFiles{}, EAccountType.Standard(), EAccountType.Standard(), "")
}
