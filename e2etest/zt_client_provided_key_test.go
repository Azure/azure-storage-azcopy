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

// Scenarios to consider for copy
// 1. Local -> Blob (Upload)
// 2. File -> Blob, Blob <-> Blob (S2S)
// 3. Blob -> Local (Download)

// Similarly, scenarios to consider for sync
// 1. Local <-> Blob (Upload)
// 2. File <-> Blob (S2S)
// 3. Blob <-> Local (Download)

// Scenarios to consider for remove
// 1. Blob <-> Trash (Delete)

func TestClient_ProvidedScopeUpload(t *testing.T) {
	cpkByName := "blobgokeytestscope"
	verifyOnlyProps := verifyOnly{with{cpkByName: cpkByName}}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.LocalFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt", verifyOnlyProps),
			f("file2.txt", verifyOnlyProps),
			folder("folder1"),
			folder("folder2"),
			f("folder1/file3", verifyOnlyProps),
			f("folder1/file4", verifyOnlyProps),
			f("folder2/file5", verifyOnlyProps),
			f("file6", verifyOnlyProps),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedScopeUploadSingleFile(t *testing.T) {
	cpkByName := "blobgokeytestscope"
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.LocalBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1", verifyOnly{with{cpkByName: cpkByName}}),
		},
		objectTarget: objectTarget{objectName: "file1"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedScopeS2S(t *testing.T) {
	cpkByName := "blobgokeytestscope"
	verifyOnlyProps := verifyOnly{with{cpkByName: cpkByName}}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt", verifyOnlyProps),
			f("file2.txt", verifyOnlyProps),
			folder("folder1"),
			f("folder1/file3", verifyOnlyProps),
			f("folder1/file4", verifyOnlyProps),
			f("folder2/file5", verifyOnlyProps),
			f("file6", verifyOnlyProps),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedScopeDownload(t *testing.T) {
	cpkByName := "blobgokeytestscope"
	verifyOnlyProps := verifyOnly{with{cpkByName: cpkByName}}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.BlobLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1", verifyOnlyProps),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedScopeDownloadSingleFile(t *testing.T) {
	cpkByName := "blobgokeytestscope"
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1", with{cpkByName: cpkByName}),
		},
		objectTarget: objectTarget{objectName: "file1"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedScopeDelete(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "BlobRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	cpkByName := "blobgokeytestscope"
	RunScenarios(t, eOperation.Remove(), blobRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			h.CreateFile(f("file1.txt", with{cpkByName: cpkByName}), true)
			h.CreateFile(f("file2.txt", with{cpkByName: cpkByName}), true)
			h.CreateFile(f("folder1/file3", with{cpkByName: cpkByName}), true)
			h.CreateFile(f("folder1/file4", with{cpkByName: cpkByName}), true)
			h.CreateFile(f("folder2/file5", with{cpkByName: cpkByName}), true)
			h.CreateFile(f("file6", with{cpkByName: cpkByName}), true)
		},
	}, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
			f("file2.txt"),
			folder("folder1"),
			folder("folder2"),
			f("folder1/file3"),
			f("folder1/file4"),
			f("folder2/file5"),
			f("file6"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedScopeDeleteSingleFile(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "BlobRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	cpkByName := "blobgokeytestscope"
	RunScenarios(t, eOperation.Remove(), blobRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		cpkByName: cpkByName,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			h.CreateFile(f("file1", with{cpkByName: cpkByName}), true)
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			f("file1"),
		},
		objectTarget: objectTarget{objectName: "file1"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyUpload(t *testing.T) {
	verifyOnlyProps := verifyOnly{with{cpkByValue: true}}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt", verifyOnlyProps),
			f("file2.txt", verifyOnlyProps),
			folder("folder1"),
			folder("folder2"),
			f("folder1/file3", verifyOnlyProps),
			f("folder1/file4", verifyOnlyProps),
			f("folder2/file5", verifyOnlyProps),
			f("file6", verifyOnlyProps),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyUploadSingleFile(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.LocalBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1", verifyOnly{with{cpkByValue: true}}),
		},
		objectTarget: objectTarget{objectName: "file1"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyS2S(t *testing.T) {
	verifyOnlyProps := verifyOnly{with{cpkByValue: true}}
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.FileBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt", verifyOnlyProps),
			f("file2.txt", verifyOnlyProps),
			folder("folder1"),
			f("folder1/file3", verifyOnlyProps),
			f("folder1/file4", verifyOnlyProps),
			f("folder2/file5", verifyOnlyProps),
			f("file6", verifyOnlyProps),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyDownload(t *testing.T) {
	verifyOnlyProps := verifyOnly{with{cpkByValue: true}}
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1", verifyOnlyProps),
			folder("dir"),
			f("dir/file2", verifyOnlyProps),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyDownloadSingleFile(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, nil, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1", with{cpkByValue: true}),
		},
		objectTarget: objectTarget{objectName: "file1"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyDelete(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "BlobRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	RunScenarios(t, eOperation.Remove(), blobRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			h.CreateFile(f("file1", with{cpkByValue: true}), true)
			h.CreateFile(folder("dir", with{cpkByValue: true}), true)
			h.CreateFile(f("dir/file2", with{cpkByValue: true}), true)
		},
	}, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1"),
			folder("dir"),
			f("dir/file2"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestClient_ProvidedKeyDeleteSingleFile(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "BlobRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	RunScenarios(t, eOperation.Remove(), blobRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:  true,
		cpkByValue: true,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			h.CreateFile(f("file1", with{cpkByValue: true}), true)
		},
	}, testFiles{
		defaultSize: "100K",
		shouldTransfer: []interface{}{
			f("file1"),
		},
		objectTarget: objectTarget{objectName: "file1"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
