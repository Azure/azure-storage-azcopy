package e2etest

import (
	"bytes"
	"encoding/base64"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type SyncTestSuite struct{}

func init() {
	suiteManager.RegisterSuite(&SyncTestSuite{})
}

func (s *SyncTestSuite) Scenario_TestSyncHashStorageModes(a *ScenarioVariationManager) {
	// First, pick out our hash storage mode.
	// Mode "11" is always XAttr or AlternateDataStreams.
	hashStorageMode := ResolveVariation(a, []common.HashStorageMode{
		common.EHashStorageMode.HiddenFiles(), // OS-agnostic behavior
		common.HashStorageMode(11),            // XAttr (linux; if available), ADS (windows; if available)
	})

	customDirVariation := "UseCustomDir"
	useCustomLocalHashDir := "NoCustomDir"
	if hashStorageMode == common.EHashStorageMode.HiddenFiles() { // Custom hash dir is only available on HiddenFiles
		a.InsertVariationSeparator("_")
		useCustomLocalHashDir = ResolveVariation(a, []string{customDirVariation, "NoCustomDir"})
	}

	a.InsertVariationSeparator("|")

	// TODO: If you want to test XAttr support on Linux or Mac, remove me! ADO does not support XAttr!
	if hashStorageMode == 11 && (runtime.GOOS != "windows") {
		a.InvalidateScenario()
		return
	}

	// A local source is required to use any hash storage mode.
	source := NewLocalContainer(a)
	dupeBodyPath := "underfolder/donottransfer" // A directory is used to validate that the hidden files cache creates *all* subdirectories.
	dupeBody := NewRandomObjectContentContainer(512)
	resourceSpec := ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"newobject":      ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
			"shouldtransfer": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
			dupeBodyPath:     ResourceDefinitionObject{Body: dupeBody}, // note: at this moment, this is *not* a great test, because we lack plan file validation. todo WI#26418256
		},
	}
	CreateResource[ContainerResourceManager](a, source, resourceSpec)

	// We'll use Blob and Files as a target for the destination.
	md5 := dupeBody.MD5()
	dest := CreateResource[ContainerResourceManager](a,
		GetRootResource(a, ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.File()})),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				// Object to overwrite
				"shouldtransfer": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
				// Object to avoid overwriting
				dupeBodyPath: ResourceDefinitionObject{Body: dupeBody, ObjectProperties: ObjectProperties{HTTPHeaders: contentHeaders{contentMD5: md5[:]}}},
			},
		},
	)

	// Make local files overwritten at a much later date than storage to validate we're doing hash-based tx
	if !a.Dryrun() {
		err := filepath.WalkDir(source.URI(), func(path string, d fs.DirEntry, err error) error {
			err = os.Chtimes(path, time.Time{}, time.Now().Add(time.Hour*24))
			return err
		})

		a.NoError("Tried to set times", err)
	}

	var customDir *string
	if useCustomLocalHashDir == customDirVariation {
		f := NewLocalContainer(a)
		customDir = pointerTo(f.URI())

		if !a.Dryrun() {
			a.Cleanup(func(a ScenarioAsserter) {
				// Should be created by AzCopy, but, won't get tracked by the framework, because it's never actually created.
				f.Delete(a)
			})
		}
	}

	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{source, dest},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			CompareHash:          PtrOf(common.ESyncHashType.MD5()),
			LocalHashStorageMode: &hashStorageMode,
			LocalHashDir:         customDir,
		},
	})

	ValidateResource[ContainerResourceManager](a, dest, resourceSpec, true)

	// Finally, validate that we're actually storing the hash correctly.
	// For this, we'll only validate the single hash we expected to conflict, because we already have the hash data for that.
	if a.Dryrun() {
		return // Don't do this if we're dryrunning, since we can't validate this at this time.
	}

	if customDir != nil {
		_, err := os.Stat(*customDir)
		a.NoError("AzCopy must create the hash directory", err)
	}

	adapter, err := common.NewHashDataAdapter(DerefOrZero(customDir), source.URI(), hashStorageMode)
	a.NoError("create hash storage adapter", err)
	a.Assert("create hash storage adapter with correct mode", Equal{}, adapter.GetMode(), hashStorageMode)

	data, err := adapter.GetHashData(dupeBodyPath)
	a.NoError("Poll hash data", err)
	a.Assert("Data must not be nil", Not{IsNil{}}, data)
	a.Assert("Data must match target hash mode", Equal{}, data.Mode, common.ESyncHashType.MD5()) // for now, we only have MD5. In the future, CRC64 may be available.

	fi, err := os.Stat(filepath.Join(source.URI(), dupeBodyPath))
	a.NoError("Stat file at source", err)
	a.Assert("LMTs must match between hash data and file", Equal{}, data.LMT.Equal(fi.ModTime()), true)

	a.Assert("hashes must match", Equal{}, data.Data, base64.StdEncoding.EncodeToString(md5[:]))
}

func (s *SyncTestSuite) Scenario_TestSyncRemoveDestination(svm *ScenarioVariationManager) {
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})
	dstLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})

	if srcLoc == common.ELocation.Local() && srcLoc == dstLoc {
		svm.InvalidateScenario()
		return
	}

	srcRes := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, srcLoc, GetResourceOptions{
		PreferredAccount: common.Iff(srcLoc == common.ELocation.BlobFS(), pointerTo(PrimaryHNSAcct), nil),
	}), ResourceDefinitionContainer{})
	dstRes := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, dstLoc, GetResourceOptions{
		PreferredAccount: common.Iff(dstLoc == common.ELocation.BlobFS(), pointerTo(PrimaryHNSAcct), nil),
	}), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"deleteme.txt":      ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
			"also/deleteme.txt": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
		},
	})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{srcRes, dstRes},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			DeleteDestination: pointerTo(true),
		},
	})

	ValidateResource[ContainerResourceManager](svm, dstRes, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"deleteme.txt":      ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)},
			"also/deleteme.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)},
		},
	}, false)
}

// Scenario_TestSyncDeleteDestinationIfNecessary tests that sync is
// - capable of deleting blobs of the wrong type
func (s *SyncTestSuite) Scenario_TestSyncDeleteDestinationIfNecessary(svm *ScenarioVariationManager) {
	dstLoc := ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.BlobFS()})
	dstRes := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, dstLoc, GetResourceOptions{
			PreferredAccount: common.Iff(dstLoc == common.ELocation.Blob(),
				pointerTo(PrimaryStandardAcct), //
				pointerTo(PrimaryHNSAcct),
			),
		}),
		ResourceDefinitionContainer{})

	overwriteName := "copyme.txt"
	ignoreName := "ignore.txt"

	if !svm.Dryrun() { // We're working directly with raw clients, so, we need to be careful.
		buf := streaming.NopCloser(bytes.NewReader([]byte("foo")))

		switch dstRes.Location() {
		case common.ELocation.Blob(): // In this case, we want to submit a block ID with a different length.
			ctClient := dstRes.(*BlobContainerResourceManager).internalClient
			blobClient := ctClient.NewBlockBlobClient(overwriteName)

			_, err := blobClient.StageBlock(ctx, base64.StdEncoding.EncodeToString([]byte("foobar")), buf, nil)
			svm.Assert("stage block error", IsNil{}, err)
		case common.ELocation.BlobFS(): // In this case, we want to upload a blob via DFS.
			ctClient := dstRes.(*BlobFSFileSystemResourceManager).internalClient
			pathClient := ctClient.NewFileClient(overwriteName)

			_, err := pathClient.Create(ctx, nil)
			svm.Assert("Create error", IsNil{}, err)
			err = pathClient.UploadStream(ctx, buf, nil)
			svm.Assert("Upload stream error", IsNil{}, err)
		}

		// Sleep so it's in the past.
		time.Sleep(time.Second * 10)
	}

	srcData := NewRandomObjectContentContainer(1024)
	srcRes := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			overwriteName: ResourceDefinitionObject{Body: srcData},
			ignoreName:    ResourceDefinitionObject{Body: srcData},
		},
	})

	dstData := NewRandomObjectContentContainer(1024)
	if !svm.Dryrun() {
		time.Sleep(time.Second * 10) // Make sure this file is newer

		CreateResource[ObjectResourceManager](svm, dstRes, ResourceDefinitionObject{
			ObjectName: &ignoreName,
			Body:       dstData,
		})
	}

	stdout, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{srcRes, dstRes},
		Flags: SyncFlags{
			DeleteIfNecessary: pointerTo(true),
		},
	})

	ValidatePlanFiles(svm, stdout, ExpectedPlanFile{
		Objects: map[PlanFilePath]PlanFileObject{
			PlanFilePath{"/" + overwriteName, "/" + overwriteName}: {
				ShouldBePresent: pointerTo(true),
			},
			PlanFilePath{"/" + ignoreName, "/" + ignoreName}: {
				ShouldBePresent: pointerTo(false),
			},
		},
	})

	ValidateResource(svm, dstRes, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			overwriteName: ResourceDefinitionObject{
				Body: srcData, // Validate we overwrote this one
			},
			ignoreName: ResourceDefinitionObject{
				Body: dstData, // Validate we did not overwrite this one
			},
		},
	}, true)
}
