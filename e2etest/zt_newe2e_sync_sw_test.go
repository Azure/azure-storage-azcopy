package e2etest

import (
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	//"github.com/google/uuid"
)

type SWSyncTestSuite struct{}

func init() {
	suiteManager.RegisterSuite(&SWSyncTestSuite{})
}

func (s *SWSyncTestSuite) Scenario_TestSyncRemoveDestination(svm *ScenarioVariationManager) {
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local()})
	dstLoc := ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})

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
				Recursive:             pointerTo(false),
				IncludeDirectoryStubs: pointerTo(true),
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

func (s *SWSyncTestSuite) Scenario_TestSyncCreateResources(a *ScenarioVariationManager) {
	// Set up the scenario
	a.InsertVariationSeparator("Local->")
	srcLoc := common.ELocation.Local()
	dstLoc := ResolveVariation(a, []common.Location{common.ELocation.File()})
	a.InsertVariationSeparator("|Create:")

	const (
		CreateContainer = "Container"
		CreateFolder    = "Folder"
		CreateObject    = "Object"
	)

	resourceType := ResolveVariation(a, []string{CreateFolder, CreateObject, CreateContainer})

	// Select source map
	srcMap := map[string]ObjectResourceMappingFlat{
		CreateContainer: {
			"foo": ResourceDefinitionObject{},
		},
		CreateFolder: {
			"foo/": ResourceDefinitionObject{
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
				Recursive:             pointerTo(false),
				IncludeDirectoryStubs: pointerTo(true),
			},
			IncludeRoot: pointerTo(true),
		},
	})

	ValidateResource(a, dst, ResourceDefinitionContainer{
		Objects: srcMap,
	}, false)
}
