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

func TestBlobVersioning_ListOfVersions(t *testing.T) {
	RunScenarios(t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{},
		&hooks{},
		testFiles{
			defaultSize: "1k",
			objectTarget: objectTarget{
				objectName: "versioned",
				versions:   []uint{0, 2},
			},
			shouldTransfer: []any{
				f("versioned", with{
					blobVersions: 3, // create 3 unique versions to read from
				}),
			},
		},
		EAccountType.Standard(), EAccountType.Standard(), "")
}
