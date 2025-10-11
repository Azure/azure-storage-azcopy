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

var fileExtensions = []string{".exe", ".cpp", ".java", ".py", ".go", ".mp3", ".mp4", ".pdf", ".gzip", ".txt", ".dat", ".bat", ".xlsx"}

func TestHeader_SourceLocal(t *testing.T) {
	extensionsMap := GetContentTypeMap(fileExtensions)
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.LocalBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1M",
		shouldTransfer: []interface{}{
			// folder("", ),
			f("file1.mp3", with{contentType: extensionsMap[".mp3"]}),
			f("file2.pdf", with{contentType: extensionsMap[".pdf"]}),
			f("file3.exe", with{contentType: extensionsMap[".exe"]}),
			f("file4.txt", with{contentType: extensionsMap[".txt"]}),
			f("file5.mp4", with{contentType: extensionsMap[".mp4"]}),
			f("file6.xlsx", with{contentType: extensionsMap[".xlsx"]}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestHeader_SourceLocalEmptyFiles(t *testing.T) {
	extensionsMap := GetContentTypeMap(fileExtensions)
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.LocalBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "0K",
		shouldTransfer: []interface{}{
			// folder("", ),
			f("file1.mp3", with{contentType: extensionsMap[".mp3"]}),
			f("file2.pdf", with{contentType: extensionsMap[".pdf"]}),
			f("file3.exe", with{contentType: extensionsMap[".exe"]}),
			f("file4.txt", with{contentType: extensionsMap[".txt"]}),
			f("file3.mp4", with{contentType: extensionsMap[".mp4"]}),
			f("file5.xlsx", with{contentType: extensionsMap[".xlsx"]}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestHeader_AllS2S(t *testing.T) {
	extensionsMap := GetContentTypeMap(fileExtensions)
	RunScenarios(t, eOperation.Copy(), eTestFromTo.AllS2S(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			// folder("", ),
			f("file1.mp3", with{contentType: extensionsMap[".mp3"]}),
			f("file2.pdf", with{contentType: extensionsMap[".pdf"]}),
			f("file3.exe", with{contentType: extensionsMap[".exe"]}),
			f("file4.txt", with{contentType: extensionsMap[".txt"]}),
			f("file3.mp4", with{contentType: extensionsMap[".mp4"]}),
			f("file5.xlsx", with{contentType: extensionsMap[".xlsx"]}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// TODO: AutoPlusContent is not thread-safe. Look into that.
func TestHeader_SourceBlobEmptyBlob(t *testing.T) {
	extensionsMap := GetContentTypeMap(fileExtensions)
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "0K",
		shouldTransfer: []interface{}{
			// folder("", ),
			f("file1.mp3", with{contentType: extensionsMap[".mp3"]}),
			f("file2.pdf", with{contentType: extensionsMap[".pdf"]}),
			f("file3.exe", with{contentType: extensionsMap[".exe"]}),
			f("file4.txt", with{contentType: extensionsMap[".txt"]}),
			f("file3.mp4", with{contentType: extensionsMap[".mp4"]}),
			f("file5.xlsx", with{contentType: extensionsMap[".xlsx"]}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
