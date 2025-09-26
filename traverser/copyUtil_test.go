// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package traverser

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestUrlIsContainerOrBlob(t *testing.T) {
	a := assert.New(t)
	util := CopyHandlerUtil{}

	testUrl := "https://fakeaccount.core.windows.net/container/dir1"
	isContainer := util.urlIsContainerOrVirtualDirectory(testUrl)
	a.False(isContainer)

	testUrl = "https://fakeaccount.core.windows.net/container/dir1/dir2"
	isContainer = util.urlIsContainerOrVirtualDirectory(testUrl)
	a.False(isContainer)

	testUrl = "https://fakeaccount.core.windows.net/container/"
	isContainer = util.urlIsContainerOrVirtualDirectory(testUrl)
	a.True(isContainer)

	testUrl = "https://fakeaccount.core.windows.net/container"
	isContainer = util.urlIsContainerOrVirtualDirectory(testUrl)
	a.True(isContainer)

	// root container
	testUrl = "https://fakeaccount.core.windows.net/"
	isContainer = util.urlIsContainerOrVirtualDirectory(testUrl)
	a.True(isContainer)
}

func TestIPIsContainerOrBlob(t *testing.T) {
	a := assert.New(t)
	util := CopyHandlerUtil{}

	testIP := "https://127.0.0.1:8256/account/container"
	testURL := "https://fakeaccount.core.windows.net/account/container"
	isContainerIP := util.urlIsContainerOrVirtualDirectory(testIP)
	isContainerURL := util.urlIsContainerOrVirtualDirectory(testURL)
	a.True(isContainerIP)   // IP endpoints contain the account in the path, making the container the second entry
	a.False(isContainerURL) // URL endpoints do not contain the account in the path, making the container the first entry.

	testIP = "https://127.0.0.1:8256/account/container/folder"
	testURL = "https://fakeaccount.core.windows.net/account/container/folder"
	isContainerIP = util.urlIsContainerOrVirtualDirectory(testIP)
	isContainerURL = util.urlIsContainerOrVirtualDirectory(testURL)
	a.False(isContainerIP)  // IP endpoints contain the account in the path, making the container the second entry
	a.False(isContainerURL) // URL endpoints do not contain the account in the path, making the container the first entry.

	testIP = "https://127.0.0.1:8256/account/container/folder/"
	testURL = "https://fakeaccount.core.windows.net/account/container/folder/"
	isContainerIP = util.urlIsContainerOrVirtualDirectory(testIP)
	isContainerURL = util.urlIsContainerOrVirtualDirectory(testURL)
	a.True(isContainerIP)  // IP endpoints contain the account in the path, making the container the second entry
	a.True(isContainerURL) // URL endpoints do not contain the account in the path, making the container the first entry.
	// The behaviour isn't too different from here.
}

func TestDoesBlobRepresentAFolder(t *testing.T) {
	a := assert.New(t)
	util := CopyHandlerUtil{}

	// Test case 1: metadata is empty
	metadata := make(common.Metadata)
	ok := util.doesBlobRepresentAFolder(metadata)
	a.False(ok)

	// Test case 2: metadata contains exact key
	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("true")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.True(ok)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("True")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.True(ok)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("false")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.False(ok)

	metadata = make(common.Metadata)
	metadata["hdi_isfolder"] = to.Ptr("other_value")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.False(ok)

	// Test case 3: metadata contains key with different case
	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("true")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.True(ok)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("True")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.True(ok)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("false")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.False(ok)

	metadata = make(common.Metadata)
	metadata["Hdi_isfolder"] = to.Ptr("other_value")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.False(ok)

	// Test case 4: metadata is not empty and does not contain key
	metadata = make(common.Metadata)
	metadata["other_key"] = to.Ptr("value")
	ok = util.doesBlobRepresentAFolder(metadata)
	a.False(ok)
}
