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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestTier_UploadCold(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.LocalBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		accessTier: to.Ptr(blob.AccessTierCold),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.Classic(), EAccountType.Standard(), "")
}

func TestTier_V2ToClassicAccount(t *testing.T) {

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:             true,
		s2sPreserveAccessTier: true,
		accessTier:            to.Ptr(blob.AccessTierHot),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.Classic(), EAccountType.Standard(), "")
}

func TestTier_V2ToClassicAccountNoPreserve(t *testing.T) {

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:             true,
		s2sPreserveAccessTier: false,
		accessTier:            to.Ptr(blob.AccessTierHot),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.Classic(), EAccountType.Standard(), "")
}

func TestTier_V2ToClassicAccountCool(t *testing.T) {

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:             true,
		s2sPreserveAccessTier: true,
		accessTier:            to.Ptr(blob.AccessTierCool),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.Classic(), EAccountType.Standard(), "")
}

func TestTier_V2ToClassicAccountNoPreserveCool(t *testing.T) {

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:             true,
		s2sPreserveAccessTier: false,
		accessTier:            to.Ptr(blob.AccessTierCool),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.Classic(), EAccountType.Standard(), "")
}

// Set Tier on Premium Block Blob account is a private preview feature that requires a whitelisted subscription
// and is only available in certain regions. The service version must be 2021-04-10 or above. To test this feature,
// uncomment TestTier_V2ToPremiumBBAccountCool and TestTier_V2ToPremiumBBAccountHot.
/*func TestTier_V2ToPremiumBBAccountCool(t *testing.T) {
	os.Setenv(common.EEnvironmentVariable.DefaultServiceApiVersion().Name, "2021-04-10")
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		accessTier: to.Ptr(blob.AccessTierCool),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.PremiumBlockBlob(), EAccountType.Standard(), "")
	os.Setenv(common.EEnvironmentVariable.DefaultServiceApiVersion().Name, "2020-10-02")
}

func TestTier_V2ToPremiumBBAccountHot(t *testing.T) {
	os.Setenv(common.EEnvironmentVariable.DefaultServiceApiVersion().Name, "2021-04-10")
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		accessTier: to.Ptr(blob.AccessTierHot),
	}, nil, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			f("filea"),
		},
	}, EAccountType.PremiumBlockBlob(), EAccountType.Standard(), "")
	os.Setenv(common.EEnvironmentVariable.DefaultServiceApiVersion().Name, "2020-10-02")
}*/
