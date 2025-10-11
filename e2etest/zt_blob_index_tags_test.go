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

// ================================  Copy: Setting Tags ========================================================
func TestTags_SetTagsSingleBlob(t *testing.T) {
	blobTagsStr := "foo=bar&blah=bazz"
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive: true,
			blobTags:  blobTagsStr,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				f("file1.txt", with{blobTags: blobTagsStr}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTags_SetTagsSpecialCharactersSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive: true,
			blobTags:  "bla_bla=foo%2b-foo&bla%2fbla%2f2=bar",
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				// folder("", ),
				f("file1.txt", with{blobTags: "bla_bla=foo+-foo&bla/bla/2=bar"}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTags_SetTagsMultipleBlobs(t *testing.T) {
	blobTagsStr := "foo=bar&blah=bazz"
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive: true,
			blobTags:  blobTagsStr,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				folder("fdlr1"),
				f("file1.txt", with{blobTags: blobTagsStr}),
				f("fdlr1/file2.txt", with{blobTags: blobTagsStr}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// ================================  Copy: Preserve Tags ========================================================
func TestTags_PreserveTagsSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				// folder("", ),
				f("file1.txt", with{blobTags: "foo/-foo=bar:bar&baz=blah&YeAr=2020"}),
				f("file2.txt", with{blobTags: "very long string with 127 characters to check the maximum limit of key very long string with 127 characters to check the maxi=very long string with 250 characters to check the maximum limit of val very long string with 250 characters to check the maximum limit of val very long string with 250 characters to check the maximum limit of val very long string with 250 character"}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTags_PreserveTagsSpecialCharactersSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				// folder("", ),
				f("file1.txt", with{blobTags: "foo/-foo=bar:bar&baz=blah&YeAr=2020"}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTags_PreserveTagsMultipleBlobs(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				folder("fdlr1"),
				f("file1.txt", with{blobTags: "foo=bar&baz=blah&YeAr=2020"}),
				f("fdlr1/file2.txt", with{blobTags: "temp123=321pmet&zab=halb&rAeY=0202"}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTags_PreserveTagsSpecialCharactersMultipleBlobs(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				folder("fdlr1"),
				f("file1.txt", with{blobTags: "bla_bla=foo+-foo&bla/ :bla/2=bar"}),
				f("fdlr1/file2.txt", with{blobTags: "foo/-foo=bar:bar&baz=blah&YeAr=2020"}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// ================================  Sync: Preserve Tags ========================================================
func TestTags_PreserveTagsSpecialCharactersDuringSync(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:           true,
			s2sPreserveBlobTags: true,
		}, nil, testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				folder("fdlr1"),
				f("file1.txt", with{blobTags: "bla_bla=foo+-foo&bla/ :bla/2=bar"}),
				f("fdlr1/file2.txt", with{blobTags: "very long string with 127 characters to check the maximum limit of key very long string with 127 characters to check the maxi=very long string with 250 characters to check the maximum limit of val very long string with 250 characters to check the maximum limit of val very long string with 250 characters to check the maximum limit of val very long string with 250 character"}),
				folder("fdlr2"),
				f("fdlr2/file2.cpp", with{blobTags: "123+234-345=321+432-543"}),
				f("fdlr2/file2.exe", with{blobTags: "++--..//::=____"}),
				f("fdlr2/file2.pdf", with{blobTags: "a=b&c=d&e=f&g=h&i=j&a1=b1&c1=d1&e1=f1&g1=h1&i1=j1"}),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}
