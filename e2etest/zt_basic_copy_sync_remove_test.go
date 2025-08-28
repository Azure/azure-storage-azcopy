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
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
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

func TestBasic_CopyUploadLargeAppendBlob(t *testing.T) {
	dst := common.EBlobType.AppendBlob()

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob(), common.EFromTo.LocalBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		blobType:  dst.String(),
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			bprops := &objectProperties{}
			for key, _ := range props {
				// we try to match the test.txt substring because local test files have randomizing prefix to file names
				if strings.Contains(key, "test.txt") {
					bprops = props[key]
				}
			}
			h.GetAsserter().Assert(bprops.blobType, equals(), dst)
		},
	}, testFiles{
		defaultSize: "101M",

		shouldTransfer: []interface{}{
			f("test.txt", with{blobType: dst}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_CopyUploadLargeAppendBlobBlockSizeFlag(t *testing.T) {
	dst := common.EBlobType.AppendBlob()

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob(), common.EFromTo.LocalBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:   true,
		blobType:    dst.String(),
		blockSizeMB: 100, // 100 MB
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			bprops := &objectProperties{}
			for key, _ := range props {
				// we try to match the test.txt substring because local test files have randomizing prefix to file names
				if strings.Contains(key, "test.txt") {
					bprops = props[key]
				}
			}
			h.GetAsserter().Assert(bprops.blobType, equals(), dst)
		},
	}, testFiles{
		defaultSize: "101M",

		shouldTransfer: []interface{}{
			f("test.txt", with{blobType: dst}),
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

func TestBasic_CopyDownloadSingleBlobEmptyDir(t *testing.T) {
	// Only Windows fails to rename if there is an empty dir name in the path
	if runtime.GOOS != "windows" {
		return
	}
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobLocal()), eValidate.Auto(), allCredentialTypes, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
		},
		shouldFail: []interface{}{
			f("dir1//dir3/file1.txt"),
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

func TestBasic_CopyRemoveFileHNS(t *testing.T) {
	bfsRemove := TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(), // blobfs isn't technically supported; todo: support it properly rather than jank through Blob
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}

	RunScenarios(t, eOperation.Remove(), bfsRemove, eValidate.Auto(), allCredentialTypes, anonymousAuthOnly, params{}, nil, testFiles{
		objectTarget: objectTarget{objectName: "file1.txt"},
		defaultSize:  "1K",
		shouldTransfer: []interface{}{
			"file1.txt",
		},
	},
		EAccountType.Standard(),                     // dest is OK to ignore
		EAccountType.HierarchicalNamespaceEnabled(), // mark source as HNS
		"")
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

func TestBasic_CopyRemoveFolderHNS(t *testing.T) {
	bfsRemove := TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.BlobFS(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}

	RunScenarios(t, eOperation.Remove(), bfsRemove, eValidate.Auto(), allCredentialTypes, anonymousAuthOnly,
		params{
			recursive: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFiles(testFiles{
					defaultSize: "1K",
					shouldTransfer: []interface{}{
						folder("foo"),
						"foo/bar.txt",
						folder("foo/bar"),
						"foo/bar/baz.txt",
					},
				}, true, false, false)
			},
			afterValidation: func(h hookHelper) {
				a := h.GetAsserter()
				s := h.(*scenario)
				container := s.state.source.(*resourceBlobContainer)

				props := container.getAllProperties(a)

				_, ok := props["foo"]
				a.Assert(ok, equals(), false)
				_, ok = props["foo/bar.txt"]
				a.Assert(ok, equals(), false)
				_, ok = props["foo/bar/baz.txt"]
				a.Assert(ok, equals(), false)
			},
		},
		testFiles{
			objectTarget: objectTarget{objectName: "foo"},
			defaultSize:  "1K",
			shouldTransfer: []interface{}{
				folder(""), // really only should target root
			},
		},
		EAccountType.Standard(),                     // dest is OK to ignore
		EAccountType.HierarchicalNamespaceEnabled(), // mark source as HNS
		"")
}

func TestBasic_CopyRemoveContainer(t *testing.T) {
	allButBfsRemove := TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(), // If you have a container-level SAS and a HNS account, you can't delete the container. HNS should not be included here.
			common.ELocation.FileSMB(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}

	RunScenarios(t, eOperation.Remove(), allButBfsRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
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

func TestBasic_CopyRemoveContainerHNS(t *testing.T) {
	bfsRemove := TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.BlobFS(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}

	RunScenarios(t, eOperation.Remove(), bfsRemove, eValidate.Auto(), oAuthOnly, oAuthOnly, // do it over OAuth because our SAS tokens don't have appropriate perms (because they're FS-level?)
		params{
			recursive: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFiles(testFiles{
					defaultSize: "1K",
					shouldTransfer: []interface{}{
						folder("foo"),
						"foo/bar.txt",
						folder("foo/bar"),
						"foo/bar/baz.txt",
					},
				}, true, false, false)
			},
			afterValidation: func(h hookHelper) {
				a := h.GetAsserter()
				s := h.(*scenario)
				r := s.state.source.(*resourceBlobContainer)
				urlParts, err := blob.ParseURL(r.containerClient.URL())
				a.Assert(err, equals(), nil)
				fsURL := TestResourceFactory{}.GetDatalakeServiceURL(r.accountType).NewFileSystemClient(urlParts.ContainerName).NewDirectoryClient("/")

				_, err = fsURL.GetAccessControl(ctx, nil)
				a.Assert(err, notEquals(), nil)
				a.Assert(datalakeerror.HasCode(err, "FilesystemNotFound"), equals(), true)
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""), // really only should target root
			},
		},
		EAccountType.Standard(),                     // dest is OK to ignore
		EAccountType.HierarchicalNamespaceEnabled(), // mark source as HNS
		"")
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

// TestBasic_HashBasedSync_Folders validates that folders appropriately use LMT when hash based sync is enabled
func TestBasic_HashBasedSync_Folders(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.FileFile(), common.EFromTo.FileLocal()), // test both dest and source comparators
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:       true,
			compareHash:     common.ESyncHashType.MD5(),
			hashStorageMode: common.EHashStorageMode.HiddenFiles(),
		},
		&hooks{
			beforeRunJob: func(h hookHelper) { // set up source to overwrite dest
				newFiles := testFiles{
					defaultSize: "1K",
					shouldTransfer: []interface{}{
						folder(""),
						folder("overwrite me"),
						folder("not duplicate"),
					},
					shouldSkip: []interface{}{
						folder("do not overwrite me"),
					},
				}

				h.SetTestFiles(newFiles)

				target := newFiles.shouldTransfer[1].(*testObject) // overwrite me

				h.CreateFile(target, false) // create destination before source to prefer overwrite
				time.Sleep(5 * time.Second)
				h.CreateFile(target, true)
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				folder("not duplicate"),
			},
			shouldSkip: []interface{}{
				folder("do not overwrite me"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestBasic_HashBasedSync_S2S(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.BlobBlob()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:   true,
			compareHash: common.ESyncHashType.MD5(),
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFile(f("overwriteme.txt"), false) // will have a different hash, and get overwritten.

				existingBody := []byte("foobar")
				existingObject := f("skipme-exists.txt")
				existingObject.body = existingBody

				h.CreateFile(existingObject, true)
				h.CreateFile(existingObject, false)
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("asdf.txt"),
				f("overwriteme.txt"), // create at destination with different hash
			},
			shouldSkip: []interface{}{
				f("skipme-exists.txt"), // create at destination
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestBasic_HashBasedSync_UploadDownload(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.BlobLocal(), common.EFromTo.FileLocal()), // no need to run every endpoint again
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:       true,
			compareHash:     common.ESyncHashType.MD5(),
			hashStorageMode: common.EHashStorageMode.HiddenFiles(),
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFile(f("overwriteme.txt"), false) // will have a different hash, and get overwritten.

				existingBody := []byte("foobar")
				existingObject := f("skipme-exists.txt")
				existingObject.body = existingBody

				h.CreateFile(existingObject, true)
				h.CreateFile(existingObject, false)
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("asdf.txt"),
				f("overwriteme.txt"), // create at destination with different hash
			},
			shouldSkip: []interface{}{
				f("skipme-exists.txt"), // create at destination
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

// TestBasic_HashBasedSync_StorageModeOSSpecific validates AzCopy's ability to save and via the same adapter, read hash data from os-specific types
func TestBasic_HashBasedSync_StorageModeOSSpecific(t *testing.T) {
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		tmpDir, err := os.MkdirTemp("", "xattrtest*")
		if err != nil {
			t.Log("Failed to create xattr test dir:", err)
			t.FailNow()
		}

		fileName := filepath.Join(tmpDir, "asdf.txt")
		f, err := os.Create(fileName)
		if err != nil {
			t.Log("Failed to create xattr test file:", err)
			t.FailNow()
		}
		err = f.Close()
		if err != nil {
			t.Log("Failed to close xattr test file:", err)
			t.FailNow()
		}

		xAttrAdapter, _ := common.NewHashDataAdapter("", tmpDir, common.HashStorageMode(11)) // same as xattr; no errors on Linux
		err = xAttrAdapter.SetHashData("asdf.txt", &common.SyncHashData{Mode: common.ESyncHashType.MD5(), Data: "test", LMT: time.Now()})
		if errors.Is(err, syscall.Errno(0x5f)) { // == ENOTSUP
			t.Skip("XAttr not supported")
			return
		}
	}

	body := []byte("foobar")
	fileSum := md5.Sum(body)
	textFile := f("asdf.txt", with{contentMD5: fileSum[:]})
	textFile.body = body

	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.BlobLocal(), common.EFromTo.FileLocal()), // no need to run every endpoint again
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:       true,
			compareHash:     common.ESyncHashType.MD5(),
			hashStorageMode: common.HashStorageMode(11),
		},
		&hooks{
			afterValidation: func(h hookHelper) {
				fromTo := h.FromTo()
				a := h.GetAsserter()

				// get which location has the local traverser
				var localLocation *resourceLocal
				sen := h.(*scenario)
				if fromTo.IsUpload() {
					localLocation = sen.state.source.(*resourceLocal)
				} else {
					localLocation = sen.state.dest.(*resourceLocal)
				}

				// Ensure we got what we're looking for
				a.Assert(localLocation, notEquals(), nil)

				// create the hash adapter
				dataPath := localLocation.dirPath
				hashAdapter, err := common.NewHashDataAdapter("", dataPath, common.EHashStorageMode.Default())
				if err != nil || hashAdapter == nil {
					a.Error(fmt.Sprintf("Could not create hash adapter: %s", err))
					return
				}
				a.Assert(hashAdapter.GetMode(), equals(), common.HashStorageMode(11)) // 1 is currently either XAttr or ADS; both are the intent of this test.

				hashData, err := hashAdapter.GetHashData("asdf.txt")
				if err != nil || hashData == nil {
					a.Error(fmt.Sprintf("Could not read hash data: %s", err))
					return
				}

				a.Assert(hashData.Mode, equals(), common.ESyncHashType.MD5())
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				textFile,
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

// TestBasic_HashBasedSync_HashDir validates AzCopy's ability to save and via the same adapter, read hash data from an alternate directory
func TestBasic_HashBasedSync_HashDir(t *testing.T) {
	body := []byte("foobar")
	fileSum := md5.Sum(body)
	textFile := f("asdf.txt", with{contentMD5: fileSum[:]})
	textFile.body = body

	hashStorageDir, err := os.MkdirTemp("", "hashdir*")
	if err != nil {
		t.Fatal("failed to create temp dir:", err)
	}

	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.LocalFile(), common.EFromTo.BlobLocal(), common.EFromTo.FileLocal()), // no need to run every endpoint again
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:       true,
			compareHash:     common.ESyncHashType.MD5(),
			hashStorageMode: common.EHashStorageMode.HiddenFiles(), // must target hidden files
			hashStorageDir:  hashStorageDir,
		},
		&hooks{
			afterValidation: func(h hookHelper) {
				fromTo := h.FromTo()
				a := h.GetAsserter()

				// get which location has the local traverser
				var localLocation *resourceLocal
				sen := h.(*scenario)
				if fromTo.IsUpload() {
					localLocation = sen.state.source.(*resourceLocal)
				} else {
					localLocation = sen.state.dest.(*resourceLocal)
				}

				// Ensure we got what we're looking for
				a.Assert(localLocation, notEquals(), nil)

				// create the hash adapter
				dataPath := localLocation.dirPath
				hashAdapter, err := common.NewHashDataAdapter(hashStorageDir, dataPath, common.EHashStorageMode.HiddenFiles())
				if err != nil || hashAdapter == nil {
					a.Error(fmt.Sprintf("Could not create hash adapter: %s", err))
					return
				}
				a.Assert(hashAdapter.GetMode(), equals(), common.EHashStorageMode.HiddenFiles())

				hashData, err := hashAdapter.GetHashData("asdf.txt")
				if err != nil || hashData == nil {
					a.Error(fmt.Sprintf("Could not read hash data: %s", err))
					return
				}

				a.Assert(hashData.Mode, equals(), common.ESyncHashType.MD5())

				// Ensure the hash file actually exists in the right place
				hashFile := filepath.Join(hashStorageDir, ".asdf.txt"+common.AzCopyHashDataStream)
				_, err = os.Stat(hashFile)
				a.AssertNoErr(err)

				isHidden := osScenarioHelper{}.IsFileHidden(a, hashFile)
				assert.True(t, isHidden, "The metadata file should be hidden")

				originalFile := filepath.Join(dataPath, "asdf.txt")
				isHidden = osScenarioHelper{}.IsFileHidden(a, originalFile)
				assert.True(t, !isHidden, "The original file should not be hidden")
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				textFile,
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestBasic_OverwriteHNSDirWithChildren(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobFSBlobFS()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:              true,
			preserveSMBPermissions: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFiles(
					testFiles{
						defaultSize: "1K",
						shouldSkip: []interface{}{
							folder("overwrite"), //create folder to overwrite, with no perms so it can be correctly detected later.
							f("overwrite/a"),    // place file under folder to re-create conditions
						},
					},
					false, // create dest
					false, // do not set test files
					false, // create only shouldSkip here
				)
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				// overwrite with an ACL to ensure overwrite worked
				folder("overwrite", with{adlsPermissionsACL: "user::rwx,group::rwx,other::-w-"}),
			},
		},
		EAccountType.HierarchicalNamespaceEnabled(),
		EAccountType.HierarchicalNamespaceEnabled(),
		"",
	)
}

func TestBasic_SyncLMTSwitch_PreferServiceLMT(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.FileFile()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			preserveSMBInfo: to.Ptr(false),
			preserveInfo:    to.Ptr(false),
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// re-create dotransfer on the destination before the source to allow an overwrite.
				// create the files endpoint with an LMT in the future.
				fromTo := h.FromTo()
				if fromTo.To() == common.ELocation.FileSMB() {
					// if we're ignoring the SMB LMT, then the service LMT will still indicate the file is old, rather than new.
					h.CreateFile(f("dotransfer", with{lastWriteTime: time.Now().Add(time.Second * 60)}), false)
				} else {
					h.CreateFile(f("dotransfer"), false)
				}
				time.Sleep(time.Second * 5)
				if fromTo.From() == common.ELocation.FileSMB() {
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
		eTestFromTo.Other(common.EFromTo.FileFile()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			// enforce for Linux/MacOS tests
			preserveSMBInfo: to.Ptr(true),
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

func TestBasic_SyncRemoveFolders(t *testing.T) {
	destExisting := testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder("asdf"), // validate the folder is deleted
			f("asdf/a"),
		},
	}

	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.FileLocal(), common.EFromTo.LocalFile()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:         true,
			deleteDestination: common.EDeleteDestination.True(),
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFiles(destExisting, false, false, true)
			},
			afterValidation: func(h hookHelper) {
				c := h.GetAsserter()

				objects := h.GetDestination().getAllProperties(c)
				_, ok := objects["asdf"]
				c.Assert(ok, equals(), false, "asdf should not exist")
				_, ok = objects["asdf/a"]
				c.Assert(ok, equals(), false, "asdf/a should not exist")
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("a"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestBasic_SyncRemoveFoldersHNS(t *testing.T) {
	destExisting := testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder("asdf"), // validate the folder is deleted
			f("asdf/a"),
		},
	}

	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.BlobFSBlobFS()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:         true,
			deleteDestination: common.EDeleteDestination.True(),
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				h.CreateFiles(destExisting, false, false, true)
			},
			afterValidation: func(h hookHelper) {
				c := h.GetAsserter()

				objects := h.GetDestination().getAllProperties(c)
				_, ok := objects["asdf"]
				c.Assert(ok, equals(), false, "asdf should not exist")
				_, ok = objects["asdf/a"]
				c.Assert(ok, equals(), false, "asdf/a should not exist")
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("a"),
			},
		},
		EAccountType.HierarchicalNamespaceEnabled(),
		EAccountType.HierarchicalNamespaceEnabled(),
		"",
	)
}

func TestCopySync_DeleteDestinationFileFlag(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.BlobBlob(), common.EFromTo.LocalBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:             true,
		deleteDestinationFile: true,
	},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				blobClient := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient("filea")
				// initial stage block, with block id incompatible with us
				_, err := blobClient.StageBlock(ctx, base64.StdEncoding.EncodeToString([]byte("foobar")), streaming.NopCloser(strings.NewReader(blockBlobDefaultData)), nil)
				if err != nil {
					t.Errorf("error staging block %s", err)
				}

				// make sure there is an uncommitted block
				resp, err := blobClient.GetBlockList(ctx, blockblob.BlockListTypeUncommitted, nil)
				if err != nil {
					t.Errorf("error staging block %s", err)
				}

				if len(resp.UncommittedBlocks) < 1 {
					t.Error("there should be an uncommitted block")
				}
			},
		},
		testFiles{
			defaultSize: "100M",
			shouldTransfer: []interface{}{
				f("filea"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestBasic_PutBlobSizeSingleShot(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:     true,
		putBlobSizeMB: 256, // 256 MB
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			for key, _ := range props {
				// we try to match the test.txt substring because local test files have randomizing prefix to file names
				if strings.Contains(key, "test.txt") {
					client := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient(key)
					list, err := client.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
					if err != nil {
						t.Errorf("error getting block list %s", err)
					}
					if len(list.CommittedBlocks) != 0 {
						t.Errorf("expected 0 committed blocks, got %d", len(list.CommittedBlocks))
					}
					if len(list.UncommittedBlocks) != 0 {
						t.Errorf("expected 0 uncommitted blocks, got %d", len(list.UncommittedBlocks))
					}
				}
			}
		},
	}, testFiles{
		defaultSize: "101M",

		shouldTransfer: []interface{}{
			folder(""),
			f("test.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_PutBlobSizeMultiPart(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:     true,
		putBlobSizeMB: 50, // 50 MB
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			for key, _ := range props {
				// we try to match the test.txt substring because local test files have randomizing prefix to file names
				if strings.Contains(key, "test.txt") {
					client := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient(key)
					list, err := client.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
					if err != nil {
						t.Errorf("error getting block list %s", err)
					}
					// default block size is 8mb
					if len(list.CommittedBlocks) != 13 {
						t.Errorf("expected 13 committed blocks, got %d", len(list.CommittedBlocks))
					}
					if len(list.UncommittedBlocks) != 0 {
						t.Errorf("expected 0 uncommitted blocks, got %d", len(list.UncommittedBlocks))
					}
				}
			}
		},
	}, testFiles{
		defaultSize: "101M",

		shouldTransfer: []interface{}{
			folder(""),
			f("test.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_MaxSingleChunkUpload(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:   true,
		blockSizeMB: 300, // 300 MB, bigger than the default of 8 MB
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			for blob, _ := range props {
				if strings.Contains(blob, "filea") {
					blobClient := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient(blob)

					// attempt to "get blocks" but we actually won't get blocks because the blob is uploaded using put blob, not put block
					resp, err := blobClient.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
					h.GetAsserter().Assert(err, equals(), nil)
					h.GetAsserter().Assert(len(resp.CommittedBlocks), equals(), 0)
					h.GetAsserter().Assert(len(resp.UncommittedBlocks), equals(), 0)
				}
			}
		},
	}, testFiles{
		defaultSize: "100M",

		shouldTransfer: []interface{}{
			f("filea"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_MaxSingleChunkUploadNoFlag(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		// 8 MB would be the default block size
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			for blob, _ := range props {
				if strings.Contains(blob, "filea") {
					blobClient := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient(blob)

					// attempt to "get blocks" but we actually won't get blocks because the blob is uploaded using put blob, not put block
					resp, err := blobClient.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
					h.GetAsserter().Assert(err, equals(), nil)
					h.GetAsserter().Assert(len(resp.CommittedBlocks), equals(), 0)
					h.GetAsserter().Assert(len(resp.UncommittedBlocks), equals(), 0)
				}
			}
		},
	}, testFiles{
		defaultSize: "8M",

		shouldTransfer: []interface{}{
			f("filea"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_MaxMultiChunkUpload(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:   true,
		blockSizeMB: 50, // 50 MB
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			for blob, _ := range props {
				if strings.Contains(blob, "filea") {
					blobClient := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient(blob)

					// attempt to "get blocks" and get 2 committed blocks
					resp, err := blobClient.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
					h.GetAsserter().Assert(err, equals(), nil)
					h.GetAsserter().Assert(len(resp.CommittedBlocks), equals(), 2)
					h.GetAsserter().Assert(len(resp.UncommittedBlocks), equals(), 0)
				}
			}
		},
	}, testFiles{
		defaultSize: "100M",

		shouldTransfer: []interface{}{
			f("filea"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_MaxMultiChunkUploadNoFlag(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		// 8 MB would be the default block size
	}, &hooks{
		afterValidation: func(h hookHelper) {
			props := h.GetDestination().getAllProperties(h.GetAsserter())
			h.GetAsserter().Assert(len(props), equals(), 1)
			for blob, _ := range props {
				if strings.Contains(blob, "filea") {
					blobClient := h.GetDestination().(*resourceBlobContainer).containerClient.NewBlockBlobClient(blob)

					// attempt to "get blocks" and get 13 committed blocks
					resp, err := blobClient.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
					h.GetAsserter().Assert(err, equals(), nil)
					h.GetAsserter().Assert(len(resp.CommittedBlocks), equals(), 13)
					h.GetAsserter().Assert(len(resp.UncommittedBlocks), equals(), 0)
				}
			}
		},
	}, testFiles{
		defaultSize: "100M",

		shouldTransfer: []interface{}{
			f("filea"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
