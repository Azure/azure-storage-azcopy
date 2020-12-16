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

func TestTags_SetTagsSingleBlob(t *testing.T) {
	blobTagsStr := "foo=bar&blah=bazz"
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
			blobTags:  blobTagsStr,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				//folder("", ),
				f("file1.txt", with{blobTags: blobTagsStr}),
			},
		})
}

func TestTags_SetTagsSpecialCharactersSingleBlob(t *testing.T) {
	blobTagsStr := "bla_bla=foo%2b-foo&bla%2fbla%2f2=bar"
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
			blobTags:  blobTagsStr,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				//folder("", ),
				f("file1.txt", with{blobTags: blobTagsStr}),
			},
		})
}

func TestTags_SetTagsMultipleBlobs(t *testing.T) {
	blobTagsStr := "foo=bar&blah=bazz"
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
			blobTags:  blobTagsStr,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				folder("fdlr1"),
				f("file1.txt", with{blobTags: blobTagsStr}),
				f("fdlr1/file2.txt", with{blobTags: blobTagsStr}),
			},
		})
}

func TestTags_PreserveTagsSingleBlob(t *testing.T) {
	blobTagsStr := "foo/-foo=bar:bar&baz=blah&YeAr=2020"
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				//folder("", ),
				f("file1.txt", with{blobTags: blobTagsStr}),
			},
		})
}

func TestTags_PreserveTagsSpecialCharactersSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				//folder("", ),
				f("file1.txt", with{blobTags: "foo/-foo=bar:bar&baz=blah&YeAr=2020"}),
			},
		})
}

func TestTags_PreserveTagsMultipleBlobs(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				folder("fdlr1"),
				f("file1.txt", with{blobTags: "foo=bar&baz=blah&YeAr=2020"}),
				f("fdlr1/file2.txt", with{blobTags: "temp123=321pmet&zab=halb&rAeY=0202"}),
			},
		})
}
