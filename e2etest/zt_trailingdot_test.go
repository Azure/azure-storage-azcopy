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
	"runtime"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
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
			recursive:   true,
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
			recursive:   true,
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
			recursive:   true,
			trailingDot: common.ETrailingDotOption.Disable(),
		}, &hooks{
			afterValidation: func(h hookHelper) {
				shareURL := h.GetDestination().(*resourceAzureFileShare).shareClient
				l, err := shareURL.NewRootDirectoryClient().NewListFilesAndDirectoriesPager(nil).NextPage(context.Background())
				if err != nil {
					panic(err)
				}
				if len(l.Segment.Files) != 1 {
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

func TestTrailingDot_FileFile(t *testing.T) {
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

// This is testing that we do not pass the x-ms-source-allow-trailing-dot when the source is not File.
func TestTrailingDot_BlobFile(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				f("file."),
			},
			objectTarget: objectTarget{objectName: "file."},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// This is testing that we do not pass the x-ms-source-allow-trailing-dot when the source is not File.
func TestTrailingDot_BlobFileHNS(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				f("file."),
			},
			objectTarget: objectTarget{objectName: "file."},
		}, EAccountType.Standard(), EAccountType.HierarchicalNamespaceEnabled(), "")
}

// This is testing that we skip trailing dot files from File to Blob.
func TestTrailingDot_FileBlob(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.FileBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("normalfile"),
				folder("normaldirectory"),
				f("normaldirectory/normalfile"),
			},
			shouldSkip: []interface{}{
				f("trailingdotfile."),
				folder("trailingdotdirectory."),
				f("trailingdotdirectory./trailingdotfile."),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// This is testing that we skip trailing dot files from File to BlobFS.
func TestTrailingDot_FileBlobHNS(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.FileBlobFS()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("normalfile"),
				folder("normaldirectory"),
				f("normaldirectory/normalfile"),
			},
			shouldSkip: []interface{}{
				f("trailingdotfile."),
				folder("trailingdotdirectory."),
				f("trailingdotdirectory./trailingdotfile."),
			},
		}, EAccountType.HierarchicalNamespaceEnabled(), EAccountType.Standard(), "")
}

func TestTrailingDot_FileLocalWindows(t *testing.T) {
	// Windows does not support trailing dot files, so we should skip trailing dot files
	if runtime.GOOS != "windows" {
		return
	}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive: true,
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("normalfile"),
				folder("normaldirectory"),
				f("normaldirectory/normalfile"),
			},
			shouldSkip: []interface{}{
				f("trailingdotfile."),
				folder("trailingdotdirectory."),
				f("trailingdotdirectory./trailingdotfile."),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// TODO : Enable when the test suite supports testing AzCopy runs that we expect to fail
//func TestTrailingDot_FileLocalWindowsError(t *testing.T) {
//	// Windows does not support trailing dot files, so we should error out on trailing dot file
//	if runtime.GOOS != "windows" {
//		return
//	}
//	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly,
//		params{
//			recursive: true,
//		}, nil,
//		testFiles{
//			defaultSize: "1K",
//			shouldFail: []interface{}{
//				f("trailingdotfile."),
//			},
//			objectTarget: objectTarget{objectName: "trailingdotfile.",
//		}, EAccountType.Standard(), EAccountType.Standard(), "")
//}

// This is testing we still skip the trailing dot paths when the trailing dot option is set to Disable.
func TestTrailingDot_FileLocalWindowsDisable(t *testing.T) {
	// Windows does not support trailing dot files, so we should skip trailing dot files
	if runtime.GOOS != "windows" {
		return
	}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly,
		params{
			recursive:   true,
			trailingDot: common.ETrailingDotOption.Disable(),
		}, nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("normalfile"),
				folder("normaldirectory"),
				f("normaldirectory/normalfile"),
			},
			shouldSkip: []interface{}{
				f("trailingdotfile."),
				folder("trailingdotdirectory."),
				f("trailingdotdirectory./trailingdotfile."),
			},
		}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestTrailingDot_Remove(t *testing.T) {
	RunScenarios(t, eOperation.Remove(), eTestFromTo.Other(common.EFromTo.FileTrash()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
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
