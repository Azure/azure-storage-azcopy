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
)

// ================================  Copy And Sync: Upload, Download, and S2S  =========================================
func TestBasic_CopyUploadSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllUploads(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyUploadEmptyBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllUploads(),
		eValidate.Auto(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "0K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyUploadLargeBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllUploads(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		&hooks{
			beforeTestRun: func(h hookHelper) {
				h.SkipTest()
			},
		},
		testFiles{
			defaultSize: "1G",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyDownloadSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllDownloads(),
		eValidate.Auto(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyDownloadEmptyBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllDownloads(),
		eValidate.Auto(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "0K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyDownloadLargeBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllDownloads(),
		eValidate.Auto(),
		params{
			recursive: true,
		},
		&hooks{
			beforeTestRun: func(h hookHelper) {
				h.SkipTest()
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyS2SSingleBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllS2S(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyS2SEmptyBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllS2S(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "0K",
			shouldTransfer: []interface{}{
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyS2SLargeBlob(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllS2S(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		&hooks{
			beforeTestRun: func(h hookHelper) {
				h.SkipTest()
			},
		},
		testFiles{
			defaultSize: "1G",
			shouldTransfer: []interface{}{
				f("file1.txt"),
			},
		})
}

func TestBasic_CopyUploadDir(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllUploads(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1"),
				f("file2"),
				folder("folder1"),
				folder("folder2"),
				f("folder1/file1"),
				f("folder1/file2"),
				f("folder2/file3"),
			},
		})
}

func TestBasic_CopyDownloadDir(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllDownloads(),
		eValidate.Auto(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1"),
				f("file2"),
				folder("folder1"),
				folder("folder2"),
				f("folder1/file1"),
				f("folder1/file2"),
				f("folder2/file3"),
			},
		})
}

func TestBasic_CopyS2SDir(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.AllS2S(),
		eValidate.AutoPlusContent(),
		params{
			recursive: true,
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				f("file1"),
				f("file2"),
				folder("folder1"),
				folder("folder2"),
				f("folder1/file1"),
				f("folder1/file2"),
				f("folder2/file3"),
			},
		})
}

// ================================  Remove: File, Folder, and Container  ==============================================
func TestBasic_CopyRemoveFile(t *testing.T) {

	RunScenarios(
		t,
		eOperation.Remove(),
		eTestFromTo.AllRemove(),
		eValidate.Auto(),
		params{
			relativeSourcePath: "file2.txt",
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				"file1.txt",
			},
			shouldIgnore: []interface{}{
				"file2.txt",
			},
		})
}

func TestBasic_CopyRemoveLargeFile(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Remove(),
		eTestFromTo.AllRemove(),
		eValidate.Auto(),
		params{
			relativeSourcePath: "file2.txt",
		},
		&hooks{
			beforeTestRun: func(h hookHelper) {
				h.SkipTest()
			},
		},
		testFiles{
			defaultSize: "1G",
			shouldTransfer: []interface{}{
				"file1.txt",
			},
			shouldIgnore: []interface{}{
				"file2.txt",
			},
		})
}

func TestBasic_CopyRemoveFolder(t *testing.T) {

	RunScenarios(
		t,
		eOperation.Remove(),
		eTestFromTo.AllRemove(),
		eValidate.Auto(),
		params{
			recursive:          true,
			relativeSourcePath: "folder2/",
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				"file1.txt",
				"folder1/file11.txt",
				"folder1/file12.txt",
			},
			shouldIgnore: []interface{}{
				"folder2/file21.txt",
				"folder2/file22.txt",
			},
		})
}

func TestBasic_CopyRemoveContainer(t *testing.T) {

	RunScenarios(
		t,
		eOperation.Remove(),
		eTestFromTo.AllRemove(),
		eValidate.Auto(),
		params{
			recursive:          true,
			relativeSourcePath: "",
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				"file1.txt",
				"folder1/file11.txt",
				"folder1/file12.txt",
			},
		})
}
