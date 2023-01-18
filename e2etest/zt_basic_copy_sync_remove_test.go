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
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ================================  Copy And Sync: Upload, Download, and S2S  =========================================
func TestBasic_CopyUploadSingleBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllUploads(), eValidate.AutoPlusContent(), anonymousAuthOnly, allCredentialTypes, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyUploadEmptyBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllUploads(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "0K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyUploadLargeBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllUploads(), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			h.SkipTest()
		},
	}, testFiles{
		defaultSize: "1G",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyDownloadSingleBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllDownloads(), eValidate.Auto(), allCredentialTypes, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyDownloadEmptyBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllDownloads(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "0K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyDownloadLargeBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllDownloads(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			h.SkipTest()
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyS2SSingleBlob(t *testing.T) {
	// AllCredentialTypes on both sides allows us to test OAuth-OAuth
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllS2S(), eValidate.AutoPlusContent(), allCredentialTypes, allCredentialTypes, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyS2SEmptyBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllS2S(), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "0K",
		shouldTransfer: []interface{}{
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyS2SLargeBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllS2S(), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			h.SkipTest()
		},
	}, testFiles{
		defaultSize: "1G",
		shouldTransfer: []interface{}{
			f("file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyUploadDir(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllUploads(), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
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
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyDownloadDir(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllDownloads(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
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
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyS2SDir(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.AllS2S(), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
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
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

// ================================  Remove: File, Folder, and Container  ==============================================
func TestBasic_CopyRemoveFile(t *testing.T) {
	RunScenarios(t, eOperation.Remove(), eTestFromTo.AllRemove(), eValidate.Auto(), allCredentialTypes, anonymousAuthOnly, params{
		relativeSourcePath: "file2.txt",
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"file1.txt",
		},
		shouldIgnore: []interface{}{
			"file2.txt",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyRemoveLargeFile(t *testing.T) {
	RunScenarios(t, eOperation.Remove(), eTestFromTo.AllRemove(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		relativeSourcePath: "file2.txt",
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			h.SkipTest()
		},
	}, testFiles{
		defaultSize: "1G",
		shouldTransfer: []interface{}{
			"file1.txt",
		},
		shouldIgnore: []interface{}{
			"file2.txt",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyRemoveFolder(t *testing.T) {

	RunScenarios(t, eOperation.Remove(), eTestFromTo.AllRemove(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:          true,
		relativeSourcePath: "folder2/",
	}, nil, testFiles{
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
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyRemoveContainer(t *testing.T) {

	RunScenarios(t, eOperation.Remove(), eTestFromTo.AllRemove(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:          true,
		relativeSourcePath: "",
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"file1.txt",
			"folder1/file11.txt",
			"folder1/file12.txt",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyToWrongBlobType(t *testing.T) {
	btypes := []common.BlobType{
		common.EBlobType.BlockBlob(),
		common.EBlobType.PageBlob(),
		common.EBlobType.AppendBlob(),
	}

	for _, dst := range btypes {
		for _, src := range btypes {
			if dst == src {
				continue
			}

			RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob(), common.EFromTo.LocalBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
				recursive:              true,
				blobType:               src.String(),
				stripTopDir:            true,
				disableParallelTesting: true, // todo: why do append blobs _specifically_ fail when this is done in parallel?
			}, &hooks{
				beforeRunJob: func(h hookHelper) {
					h.CreateFile(f("test.txt", with{blobType: dst}), false)
				},
				afterValidation: func(h hookHelper) {
					props := h.GetDestination().getAllProperties(h.GetAsserter())
					bprops, ok := props["test.txt"]
					h.GetAsserter().Assert(ok, equals(), true)
					if ok {
						h.GetAsserter().Assert(bprops.blobType, equals(), dst)
					}
				},
			}, testFiles{
				defaultSize: "1k",

				shouldFail: []interface{}{
					f("test.txt", with{blobType: src}),
				},
			}, EAccountType.Standard(), EAccountType.Standard(), src.String()+"-"+dst.String())
		}
	}
}

func TestBasic_CopyWithShareRoot(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(), // Sync already shares the root by default.
		eTestFromTo.AllUploads(),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:        true,
			invertedAsSubdir: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			destTarget:  "newName",

			shouldTransfer: []interface{}{
				folder(""),
				f("asdf.txt"),
				folder("a"),
				f("a/asdf.txt"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestBasic_SyncLMTSwitch_PreferServiceLMT(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			preserveSMBInfo: false,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// re-create dotransfer on the destination before the source to allow an overwrite.
				// create the files endpoint with an LMT in the future.
				fromTo := h.FromTo()
				if fromTo.To() == common.ELocation.File() {
					// if we're ignoring the SMB LMT, then the service LMT will still indicate the file is old, rather than new.
					h.CreateFile(f("dotransfer", with{lastWriteTime: time.Now().Add(time.Second * 60)}), false)
				} else {
					h.CreateFile(f("dotransfer"), false)
				}
				time.Sleep(time.Second * 5)
				if fromTo.From() == common.ELocation.File() {
					// if we're ignoring the SMB LMT, then the service LMT will indicate the destination is older, not newer.
					h.CreateFile(f("dotransfer", with{lastWriteTime: time.Now().Add(-time.Second * 60)}), true)
				} else {
					h.CreateFile(f("dotransfer"), true)
				}
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("dotransfer"),
			},
			shouldSkip: []interface{}{
				f("donottransfer"), // "real"/service LMT should be out of date
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
		)
}

func TestBasic_SyncLMTSwitch_PreferSMBLMT(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			preserveSMBInfo: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				/*
				In a typical scenario, the source is written before the destination.
				This way, the destination is always skipped in the case of overwrite on Sync.

				In this case, because we distinctly DO NOT want to test the service LMT, we'll create the destination before the source.
				But, we'll create those files with an SMB LMT that would lead to a skipped file.
				 */

				newTestFiles := testFiles{
					defaultSize: "1K",
					shouldTransfer: []interface{}{
						folder(""),
						f("do overwrite"),
					},
					shouldSkip: []interface{}{
						f("do not overwrite"),
					},
				}

				// create do not overwrite in the future, so that it does not get overwritten
				h.CreateFile(f("do not overwrite", with{lastWriteTime: time.Now().Add(time.Second * 60)}), false)
				// create do overwrite in the past, so that it does get overwritten
				h.CreateFile(f("do overwrite", with{lastWriteTime: time.Now().Add(-time.Second * 60)}), false)
				time.Sleep(time.Second * 5)
				h.CreateFiles(newTestFiles, true, true, false)
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}
