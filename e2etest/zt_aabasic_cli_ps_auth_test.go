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

// Purpose: Tests AZCLI and powershell auth tests

func TestBasic_AzCLIAuth(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive:     true,
		AutoLoginType: common.EAutoLoginType.AzCLI().String(),
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_AzCLIAuthLowerCase(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive:     true,
		AutoLoginType: "azCli", // the purpose of this test is to check whether our casing is working or not
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_PSAuth(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive:     true,
		AutoLoginType: common.EAutoLoginType.PsCred().String(),
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_PSAuthCamelCase(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive:     true,
		AutoLoginType: "PSCred", // ditto
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
