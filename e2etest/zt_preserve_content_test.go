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

// Purpose: Tests for preserving the content of transferred files. (Including use of MD5 hashes to allow error detection)

// TODO: include decopression
// TODO; inpclude account-to-account copy

func TestContent_AtBlobStorage(t *testing.T) {

	RunScenarios(
		t,
		eOperation.Copy(), // Sync doesn't support the command-line metadata flag
		eTestFromTo.Other(common.EFromTo.LocalBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "4M",
			shouldTransfer: []interface{}{
				folder(""), // root folder
				f("filea"),
			},
		})
}

func TestContent_AtFileShare(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(), // Sync doesn't support the command-line metadata flag
		eTestFromTo.Other(common.EFromTo.LocalFile()),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "4M",
			shouldTransfer: []interface{}{
				folder(""), // root folder
				folder("folder1"),
				f("folder1/filea"),
			},
		})
}

func TestContent_BlobToBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(), // Sync doesn't support the command-line metadata flag
		// eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "8M",
			shouldTransfer: []interface{}{
				folder(""), // root folder
				f("filea"),
			},
		})
}

//func TestChange_ValidateFileContentAtRemote(t *testing.T) {
//	RunScenarios(
//		t,
//		eOperation.Copy(),
//		eTestFromTo.AllUploads(),
//		eValidate.Auto(),
//		params{
//			recursive: true,
//		},
//		nil,
//		testFiles{
//			defaultSize: "1K",
//			shouldTransfer: []interface{}{
//				"file1",
//				"folder1/file2",
//				"folder1/file3",
//			},
//		})
//}
//
//func TestChange_ValidateFileContentAtLocal(t *testing.T) {
//
//}
//
//func TestChange_ValidateFileContentAfterS2STransfer(t *testing.T) {
//
//}
