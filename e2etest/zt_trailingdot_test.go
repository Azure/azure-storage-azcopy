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
	"context"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-file-go/azfile"
	"runtime"
	"testing"
)

func TestTrailingDot_Local(t *testing.T) {
	// Windows does not support trailing dot files, so we cannot test File->Local or Local->File
	if runtime.GOOS == "windows" {
		return
	}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileLocal(), common.EFromTo.LocalFile()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file"),
				f("file."),
				folder("directory."),
				f("directory./file."),
				f("directory./file"),
				folder("directory"),
				f("directory/file."),
				f("directory/file"),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTrailingDot_LocalManual(t *testing.T) {
	// Windows does not support trailing dot files, so we cannot test File->Local or Local->File
	if runtime.GOOS == "windows" {
		return
	}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileLocal(), common.EFromTo.LocalFile()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
			trailingDot: common.ETrailingDotOption.Enable(),
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file"),
				f("file."),
				folder("directory."),
				f("directory./file."),
				f("directory./file"),
				folder("directory"),
				f("directory/file."),
				f("directory/file"),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTrailingDot_Min(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
			trailingDot: common.ETrailingDotOption.Enable(),
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file."),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTrailingDot_Disabled(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
			trailingDot: common.ETrailingDotOption.Disable(),
		}, &hooks{
			afterValidation: func(h hookHelper) {
				shareURL := h.GetDestination().(*resourceAzureFileShare).shareURL
				l, err := shareURL.NewRootDirectoryURL().ListFilesAndDirectoriesSegment(context.Background(), azfile.Marker{}, azfile.ListFilesAndDirectoriesOptions{})
				if err != nil {
					panic(err)
				}
				if len(l.FileItems) != 1 {
					panic("expected 1 file named `file`")
				}
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file"),
				f("file."),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTrailingDot_S2S(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file"),
				f("file."),
				folder("directory."),
				f("directory./file."),
				f("directory./file"),
				folder("directory"),
				f("directory/file."),
				f("directory/file"),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTrailingDot_Remove(t *testing.T) {
	RunScenarios(t, eOperation.Remove(), eTestFromTo.Other(common.EFromTo.FileTrash()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:          true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file"),
			f("file."),
			folder("directory."),
			f("directory./file."),
			f("directory./file"),
			folder("directory"),
			f("directory/file."),
			f("directory/file"),


		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}