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

package ste

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/stretchr/testify/assert"
)

func Test_BlobTierAllowed_TierNotPossible(t *testing.T) {
	a := assert.New(t)
	tierSetPossibleFail = true

	allowed := BlobTierAllowed(to.Ptr(blob.AccessTierHot))
	a.True(allowed)

}

func Test_BlobTierAllowed_Premium(t *testing.T) {
	a := assert.New(t)
	tierSetPossibleFail = false
	destAccountSKU = "Premium_LRS"
	destAccountKind = "StorageV2"

	allowed := BlobTierAllowed(to.Ptr(blob.AccessTierP4))
	a.True(allowed)

	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierP40))
	a.True(allowed)

	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierCool))
	a.False(allowed)

	destAccountKind = "Storage"
	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierHot))
	a.False(allowed)

	destAccountKind = "BlockBlobStorage"
	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierHot))
	a.True(allowed)
}

func Test_BlobTierAllowed(t *testing.T) {
	a := assert.New(t)
	tierSetPossibleFail = false
	destAccountSKU = "Standard"
	destAccountKind = "StorageV2"

	allowed := BlobTierAllowed(to.Ptr(blob.AccessTierArchive))
	a.True(allowed)

	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierCool))
	a.True(allowed)

	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierCold))
	a.True(allowed)

	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierHot))
	a.True(allowed)

	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierP4))
	a.False(allowed)

	destAccountKind = "Storage"
	allowed = BlobTierAllowed(to.Ptr(blob.AccessTierHot))
	a.False(allowed)
}
