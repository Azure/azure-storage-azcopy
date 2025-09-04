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

// Upload, Download, S2S transfer of folders/files with special characters. Required for avoiding regression.
func TestNaming_ShareFileFoldersSpecialChar(t *testing.T) {
	files := []string{"file1.txt", "fi,le2.pdf", "fil%e3.mp3", "file 4.jpg", "file;a5.csv", "file_a6.cpp", "file+a7.mp4"}
	folders := []string{";", ";;", "%", "_", "+", "test%folder1", "test+folder2", "test,folder3", "test folder4", "test_folder5", "test;folder6"}
	transfers := make([]interface{}, 0)
	transfers = append(transfers, folder(""))
	for i := 0; i < len(folders); i++ {
		transfers = append(transfers, folder(folders[i]))
		for j := 0; j < len(files); j++ {
			transfers = append(transfers, f(folders[i]+"/"+files[j]))
		}
	}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileSMBFileSMB(), common.EFromTo.FileSMBLocal(), common.EFromTo.LocalFileSMB()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize:    "1K",
		shouldTransfer: transfers,
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
