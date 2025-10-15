package e2etest

import (
	"bytes"
	"encoding/base64"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
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
			a.Cleanup(func(a Asserter) {
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

	ValidateResource[ContainerResourceManager](a, dest, resourceSpec, ValidateResourceOptions{
		validateObjectContent: true,
	})

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
	if data != nil {
		a.Assert("Data must match target hash mode", Equal{}, data.Mode, common.ESyncHashType.MD5()) // for now, we only have MD5. In the future, CRC64 may be available.

		fi, err := os.Stat(filepath.Join(source.URI(), dupeBodyPath))
		a.NoError("Stat file at source", err)
		a.Assert("LMTs must match between hash data and file", Equal{}, data.LMT.Equal(fi.ModTime()), true)

		a.Assert("hashes must match", Equal{}, data.Data, base64.StdEncoding.EncodeToString(md5[:]))
	}
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
		Verb: AzCopyVerbSync,
		Targets: []ResourceManager{
			srcRes,
			dstRes,
		},
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
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})
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
			ctClient := dstRes.(*BlobContainerResourceManager).InternalClient
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
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}

// Note : For local sources, the hash is computed by a hashProcessor created in zc_traverser_local, so there is no way
// for local sources to have no source hash. As such these tests only cover remote sources.
func (s *SyncTestSuite) Scenario_TestSyncHashTypeSourceHash(svm *ScenarioVariationManager) {

	// There are 4 cases to consider, this test will cover all of them
	// 1. Has hash and is equal -> skip
	// 2. Has hash and is not equal -> overwrite
	// 3. Has no hash and src LMT after dest LMT -> overwrite
	// 4. Has no hash and src LMT before dest LMT -> skip

	// Create dest
	hashEqualBody := NewRandomObjectContentContainer(512)
	hashNotEqualBody := NewRandomObjectContentContainer(512)
	noHashDestSrc := NewRandomObjectContentContainer(512)
	noHashSrcDest := NewRandomObjectContentContainer(512)

	zeroBody := NewZeroObjectContentContainer(512)

	dest := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.Local()})),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"hashequal":     ResourceDefinitionObject{Body: hashEqualBody},
				"hashnotequal":  ResourceDefinitionObject{Body: zeroBody},
				"nohashdestsrc": ResourceDefinitionObject{Body: noHashDestSrc},
				"nohashsrcdest": ResourceDefinitionObject{Body: zeroBody},
			},
		},
	)

	time.Sleep(time.Second * 10) // Make sure source is newer

	srcObjs := ObjectResourceMappingFlat{
		"hashequal":     ResourceDefinitionObject{Body: hashEqualBody},
		"hashnotequal":  ResourceDefinitionObject{Body: hashNotEqualBody},
		"nohashdestsrc": ResourceDefinitionObject{Body: noHashDestSrc},
		"nohashsrcdest": ResourceDefinitionObject{Body: noHashSrcDest},
	}

	src := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.Blob()),
		ResourceDefinitionContainer{
			Objects: srcObjs,
		},
	)

	// Need to manually unset the md5
	src.GetObject(svm, "nohashdestsrc", common.EEntityType.File()).SetHTTPHeaders(svm, contentHeaders{contentMD5: nil})
	src.GetObject(svm, "nohashsrcdest", common.EEntityType.File()).SetHTTPHeaders(svm, contentHeaders{contentMD5: nil})

	time.Sleep(time.Second * 10) // Make sure destination is newer

	// Re-create nohashsrcdest so the src LMT is before dest LMT
	dest.GetObject(svm, "nohashsrcdest", common.EEntityType.File()).Create(svm, noHashSrcDest, ObjectProperties{})

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    AzCopyVerbSync,
			Targets: []ResourceManager{src, dest},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				CompareHash:          pointerTo(common.ESyncHashType.MD5()),
				LocalHashStorageMode: pointerTo(common.EHashStorageMode.HiddenFiles()), // This is OS agnostic (ADO does not support xattr so Linux test fails without this).
			},
		})

	// All source, dest should match
	ValidateResource[ContainerResourceManager](svm, dest, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})

	// Only non skipped paths should be in plan file
	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		Objects: map[PlanFilePath]PlanFileObject{
			PlanFilePath{SrcPath: "/hashnotequal", DstPath: "/hashnotequal"}: {
				Properties: ObjectProperties{},
			},
			PlanFilePath{SrcPath: "/nohashdestsrc", DstPath: "/nohashdestsrc"}: {
				Properties: ObjectProperties{},
			},
		},
	})
}

// Note : For local destinations, the hash is computed by a hashProcessor created in zc_traverser_local, so there is no way
// for local destinations to have no source hash. As such these tests only cover remote destinations.
func (s *SyncTestSuite) Scenario_TestSyncHashTypeDestinationHash(svm *ScenarioVariationManager) {

	// There are 4 cases to consider, this test will cover all of them
	// 1. Has hash and is equal -> skip
	// 2. Has hash and is not equal -> overwrite
	// 3. Has no hash and src LMT after dest LMT -> overwrite
	// 4. Has no hash and src LMT before dest LMT -> overwrite

	// Create dest
	hashEqualBody := NewRandomObjectContentContainer(512)
	hashNotEqualBody := NewRandomObjectContentContainer(512)
	noHashDestSrc := NewRandomObjectContentContainer(512)
	noHashSrcDest := NewRandomObjectContentContainer(512)

	zeroBody := NewZeroObjectContentContainer(512)

	dest := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.Blob()),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"hashequal":     ResourceDefinitionObject{Body: hashEqualBody},
				"hashnotequal":  ResourceDefinitionObject{Body: zeroBody},
				"nohashdestsrc": ResourceDefinitionObject{Body: zeroBody},
				"nohashsrcdest": ResourceDefinitionObject{Body: zeroBody},
			},
		},
	)

	time.Sleep(time.Second * 10) // Make sure source is newer

	srcObjs := ObjectResourceMappingFlat{
		"hashequal":     ResourceDefinitionObject{Body: hashEqualBody},
		"hashnotequal":  ResourceDefinitionObject{Body: hashNotEqualBody},
		"nohashdestsrc": ResourceDefinitionObject{Body: noHashDestSrc},
		"nohashsrcdest": ResourceDefinitionObject{Body: noHashSrcDest},
	}

	src := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.Local()})),
		ResourceDefinitionContainer{
			Objects: srcObjs,
		},
	)

	// Need to manually unset the md5
	dest.GetObject(svm, "nohashdestsrc", common.EEntityType.File()).SetHTTPHeaders(svm, contentHeaders{contentMD5: nil})
	dest.GetObject(svm, "nohashsrcdest", common.EEntityType.File()).SetHTTPHeaders(svm, contentHeaders{contentMD5: nil})

	time.Sleep(time.Second * 10) // Make sure destination is newer

	// Re-create nohashsrcdest so the src LMT is before dest LMT
	dest.GetObject(svm, "nohashsrcdest", common.EEntityType.File()).Create(svm, zeroBody, ObjectProperties{})

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    AzCopyVerbSync,
			Targets: []ResourceManager{src, dest},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				CompareHash: pointerTo(common.ESyncHashType.MD5()),
			},
		})

	// All source, dest should match
	ValidateResource[ContainerResourceManager](svm, dest, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})

	// Only non skipped paths should be in plan file
	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		Objects: map[PlanFilePath]PlanFileObject{
			PlanFilePath{SrcPath: "/hashnotequal", DstPath: "/hashnotequal"}: {
				Properties: ObjectProperties{},
			},
			PlanFilePath{SrcPath: "/nohashdestsrc", DstPath: "/nohashdestsrc"}: {
				Properties: ObjectProperties{},
			},
			PlanFilePath{SrcPath: "/nohashsrcdest", DstPath: "/nohashsrcdest"}: {
				Properties: ObjectProperties{},
			},
		},
	})
}

func (s *SyncTestSuite) Scenario_TestSyncCreateResources(a *ScenarioVariationManager) {
	// Set up the scenario
	a.InsertVariationSeparator("Blob->")
	srcLoc := common.ELocation.Blob()
	// We cannot test it for File because if the file share does not exists at the destination, azcopy will fail the azcopy job.
	// The user will have to manually create the destination file share.
	dstLoc := ResolveVariation(a, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.BlobFS()})
	a.InsertVariationSeparator("|Create:")

	const (
		CreateContainer = "Container"
		CreateFolder    = "Folder"
		CreateObject    = "Object"
	)

	resourceType := ResolveVariation(a, []string{CreateContainer, CreateFolder, CreateObject})

	// Select source map
	srcMap := map[string]ObjectResourceMappingFlat{
		CreateContainer: {
			"foo": ResourceDefinitionObject{},
		},
		CreateFolder: {
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
			},
			"foo/bar": ResourceDefinitionObject{},
		},
		CreateObject: {
			"foo": ResourceDefinitionObject{},
		},
	}[resourceType]

	// Create resources and targets
	src := CreateResource(a, GetRootResource(a, srcLoc), ResourceDefinitionContainer{
		Objects: srcMap,
	})
	srcTarget := map[string]ResourceManager{
		CreateContainer: src,
		CreateFolder:    src.GetObject(a, "foo", common.EEntityType.Folder()),
		CreateObject:    src.GetObject(a, "foo", common.EEntityType.File()),
	}[resourceType]

	var dstTarget ResourceManager
	var dst ContainerResourceManager
	if dstLoc == common.ELocation.Local() {
		dst = GetRootResource(a, dstLoc).(ContainerResourceManager) // No need to grab a container
	} else {
		dst = GetRootResource(a, dstLoc).(ServiceResourceManager).GetContainer(uuid.NewString())
	}

	if resourceType != CreateContainer {
		dst.Create(a, ContainerProperties{})
	}

	dstTarget = map[string]ResourceManager{
		CreateContainer: dst,
		CreateFolder:    dst.GetObject(a, "foo", common.EEntityType.File()), // Intentionally don't end with a trailing slash, so Sync has to pick that up for us.
		CreateObject:    dst.GetObject(a, "foo", common.EEntityType.File()),
	}[resourceType]

	// Run the test for realsies.
	RunAzCopy(a, AzCopyCommand{
		Verb: AzCopyVerbSync,
		Targets: []ResourceManager{
			srcTarget,
			AzCopyTarget{
				ResourceManager: dstTarget,
				AuthType:        EExplicitCredentialType.SASToken(),
				Opts: CreateAzCopyTargetOptions{
					SASTokenOptions: GenericAccountSignatureValues{
						Permissions: (&blobsas.AccountPermissions{
							Read:   true,
							Write:  true,
							Delete: true,
							List:   true,
							Add:    true,
							Create: true,
						}).String(),
						ResourceTypes: (&blobsas.AccountResourceTypes{
							Service:   true,
							Container: true,
							Object:    true,
						}).String(),
					},
				},
			},
		},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:             pointerTo(true),
				IncludeDirectoryStubs: pointerTo(true),
			},
			IncludeRoot: pointerTo(true),
		},
	})

	ValidateResource(a, dst, ResourceDefinitionContainer{
		Objects: srcMap,
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})
}

// Scenario_TestS2SBlobFSIncludeRootACLS validates root ACLs are included in transfer and
// preserved on the destination
func (s *SyncTestSuite) Scenario_TestS2SBlobFSIncludeRootACLS(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	// Use BlobFS (HNS) for ACLs we can validate
	dstContainer := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.BlobFS()),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				// Destination root with a different ACL to observe the change after sync
				"root": ResourceDefinitionObject{
					ObjectProperties: ObjectProperties{
						EntityType: common.EEntityType.Folder(),
						BlobFSProperties: BlobFSProperties{
							ACL: pointerTo("user::rwx,group::---,other::---"),
						},
					},
				},
				"root/file.txt": ResourceDefinitionObject{
					Body: body,
					ObjectProperties: ObjectProperties{
						EntityType: common.EEntityType.File(),
					},
				},
			},
		},
	)
	if !svm.Dryrun() {
		// Make sure LMT is in the past
		time.Sleep(time.Second * 5)
	}

	srcContainer := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.BlobFS()),
		ResourceDefinitionContainer{})
	rootDir := "root"

	srcObjs := make(ObjectResourceMappingFlat)
	srcObjResMan := make(map[string]ObjectResourceManager)
	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder(),
			BlobFSProperties: BlobFSProperties{
				// grant all for group to make it observably different
				ACL: pointerTo("user::rwx,group::rwx,other::---"),
			},
		},
	}
	srcObjResMan[rootDir] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[rootDir] = obj

	// create one file under root so sync enumerates content
	fileName := rootDir + "/file.txt"
	fileObj := ResourceDefinitionObject{
		ObjectName: pointerTo(fileName),
		Body:       body,
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	}
	srcObjResMan[fileName] = CreateResource[ObjectResourceManager](svm, srcContainer, fileObj)
	srcObjs[fileName] = fileObj

	includeRoot := NamedResolveVariation(svm, map[string]bool{
		"|includeRoot=true":  true,
		"|includeRoot=false": false,
	})
	if !includeRoot {
		delete(srcObjs, rootDir)
	}
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbSync,
		Targets: []ResourceManager{ // Sync the directory to the directory
			TryApplySpecificAuthType(srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()),
				EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}), // Need OAuth for full permissions to modify ACLs
			TryApplySpecificAuthType(dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder()),
				EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
		},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:           pointerTo(true),
				PreservePermissions: pointerTo(true),
			},
			IncludeRoot: pointerTo(includeRoot),
		},
	})

	// Validate that the destination root folder picked up the source ACL
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})

}

// Scenario_TestFileLocalIncludeRootCreationTime checks that the creation time of the source is overwritten on the destination
// when --include-root is set.
func (s *SyncTestSuite) Scenario_TestFileLocalIncludeRootCreationTime(svm *ScenarioVariationManager) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	currTime := time.Now()
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	dst := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.Local()})),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				// Destination root with a newer creation time to observe after sync
				"root": ResourceDefinitionObject{
					ObjectProperties: ObjectProperties{
						EntityType: common.EEntityType.Folder(),
						FileProperties: FileProperties{
							FileCreationTime: pointerTo(currTime.Add(time.Second * 10)),
						},
					},
				},
				"root/file.txt": ResourceDefinitionObject{Body: body},
			},
		},
	)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	src := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.Local()})),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				// Root directory with a specific creation time we will validate later
				"root": ResourceDefinitionObject{
					ObjectProperties: ObjectProperties{
						EntityType: common.EEntityType.Folder(),
						FileProperties: FileProperties{
							FileCreationTime: pointerTo(currTime),
						},
					},
				},
				// Include at least one file under root so the sync enumerates content
				"root/file.txt": ResourceDefinitionObject{Body: body},
			},
		},
	)

	// Dont test Local->Local
	if src.Location().IsLocal() && dst.Location().IsLocal() {
		svm.InvalidateScenario()
	}

	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbSync,
		Targets: []ResourceManager{
			// Sync the directory to the directory
			src.GetObject(svm, "root", common.EEntityType.Folder()),
			dst.GetObject(svm, "root", common.EEntityType.Folder()),
		},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:           pointerTo(true),
				PreservePermissions: pointerTo(true),
				PreserveInfo:        pointerTo(true),
			},
			IncludeRoot: pointerTo(true),
		},
	})

	// Validate that the destination root folder picked up the source creation time
	ValidateResource[ContainerResourceManager](svm, dst, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"root": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
					FileProperties: FileProperties{
						FileCreationTime: pointerTo(currTime),
					},
				},
			},
			"root/file.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(true)},
		},
	}, ValidateResourceOptions{
		validateObjectContent: true,
		preserveInfo:          true,
	})
}

// Scenario_TestFileLocalIncludeRootMetadata tests include-root with Metadata
// validates that metadata property on a root directory is overwritten when
// syncing s2s and uploading  with --include-root
func (s *SyncTestSuite) Scenario_TestFileLocalIncludeRootMetadata(svm *ScenarioVariationManager) {
	// Scale up from service to object
	rootDir := "root"
	includeRoot := ResolveVariation(svm, []bool{true, false})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.FileNFS()})),
		ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				rootDir: ResourceDefinitionObject{
					ObjectProperties: ObjectProperties{
						EntityType: common.EEntityType.Folder(),
						Metadata:   map[string]*string{"Author": pointerTo("Wendi")}, // Different metadata to source
					},
				},
			},
		})

	if !svm.Dryrun() {
		// Make sure the LMT is in the past
		time.Sleep(time.Second * 10)
	}

	srcObjs := make(ObjectResourceMappingFlat)
	obj := ResourceDefinitionObject{ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder(),
			Metadata: map[string]*string{"Author": pointerTo("Wonw")}}} //Different metadata to destination
	srcObjs[rootDir] = obj
	name := rootDir + "/test1.txt"
	obj = ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
	srcObjs[name] = obj

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm,
		[]common.Location{common.ELocation.Local(), common.ELocation.File(), common.ELocation.FileNFS()})), ResourceDefinitionContainer{})
	for _, obj := range srcObjs {
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	}

	sasOpts := GenericAccountSignatureValues{}

	// Dont test Local->Local
	if srcContainer.Location().IsLocal() && dstContainer.Location().IsLocal() {
		svm.InvalidateScenario()
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()),
					EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
						SASTokenOptions: sasOpts,
					}),
				TryApplySpecificAuthType(dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder()),
					EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
						SASTokenOptions: sasOpts,
					}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:    pointerTo(true),
					PreserveInfo: pointerTo(true),
				},
				IncludeRoot: pointerTo(includeRoot),
			},
		})

	// root directory should not be included in transfer when preserve-root-properties is not set
	if !includeRoot {
		delete(srcObjs, rootDir)
	}

	if !svm.Dryrun() {
		destMap := dstContainer.ListObjects(svm, "", true)
		props := destMap["root"]
		// Validate the Author metadata was updated on the destination
		svm.Assert(DerefOrZero(props.Metadata["Author"]), Equal{}, "Wonw")
	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
		preserveInfo:          true,
	})
}
