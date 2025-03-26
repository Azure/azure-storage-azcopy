package e2etest

import (
	"time"

	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
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
	dstLoc := ResolveVariation(a, []common.Location{common.ELocation.File(), common.ELocation.Blob()})
	a.InsertVariationSeparator("|Create:")

	const (
		CreateFolder    = "Folder"
		CreateContainer = "Container"
	)

	resourceType := ResolveVariation(a, []string{CreateFolder})

	// Select source map
	srcMap := map[string]ObjectResourceMappingFlat{
		CreateFolder: {
			"fooNew": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
			},
			"fooNew/bar": ResourceDefinitionObject{},
		},
	}[resourceType]

	// Create resources and targets
	src := CreateResource(a, GetRootResource(a, srcLoc), ResourceDefinitionContainer{
		Objects: srcMap,
	})
	srcTarget := map[string]ResourceManager{
		CreateFolder: src.GetObject(a, "fooNew", common.EEntityType.Folder()),
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
		CreateFolder: dst.GetObject(a, "fooNew", common.EEntityType.File()), // Intentionally don't end with a trailing slash, so Sync has to pick that up for us.
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

	srcMapValidation := map[string]ObjectResourceMappingFlat{
		CreateFolder: {
			"fooNew/bar": ResourceDefinitionObject{},
		},
	}[resourceType]
	ValidateResource(a, dst, ResourceDefinitionContainer{
		Objects: srcMapValidation,
	}, false)
}

func (s *SWSyncTestSuite) Scenario_TestSyncCreateResourceObject(a *ScenarioVariationManager) {
	// Set up the scenario
	a.InsertVariationSeparator("Local->")
	srcLoc := common.ELocation.Local()
	dstLoc := ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.File()})
	a.InsertVariationSeparator("|Create:")

	const (
		CreateContainer = "Container"
		CreateFolder    = "Folder"
		CreateObject    = "Object"
	)

	resourceType := ResolveVariation(a, []string{CreateContainer, CreateObject})

	// Select source map
	srcMap := map[string]ObjectResourceMappingFlat{
		CreateContainer: {
			"foo": ResourceDefinitionObject{},
		},
		CreateObject: {
			"foobar": ResourceDefinitionObject{},
		},
	}[resourceType]

	// Create resources and targets
	src := CreateResource(a, GetRootResource(a, srcLoc), ResourceDefinitionContainer{
		Objects: srcMap,
	})
	srcTarget := map[string]ResourceManager{
		CreateContainer: src,
		//CreateFolder: src.GetObject(a, "foo", common.EEntityType.Folder()),
		CreateObject: src.GetObject(a, "foobar", common.EEntityType.File()),
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
		CreateObject:    dst.GetObject(a, "foobar", common.EEntityType.File()),
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

	srcMapValidation := map[string]ObjectResourceMappingFlat{
		CreateContainer: {
			"foo": ResourceDefinitionObject{},
		},
		CreateObject: {
			"foobar": ResourceDefinitionObject{},
		},
	}[resourceType]
	ValidateResource(a, dst, ResourceDefinitionContainer{
		Objects: srcMapValidation,
	}, false)
}

func (s *SWSyncTestSuite) Scenario_SingleFile(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync})
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	body := NewRandomObjectContentContainer(SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	// no local->local
	if srcObj.Location().IsLocal() && dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(false),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		Objects: map[PlanFilePath]PlanFileObject{
			PlanFilePath{SrcPath: "", DstPath: ""}: {
				Properties: ObjectProperties{},
			},
		},
	})
}

/*
func (s *SWSyncTestSuite) Scenario_MultiFileUploadDownload(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Resolve variation early so name makes sense
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local()})
	// Scale up from service to object
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})

	// Scale up from service to object
	srcDef := ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"abc":    ResourceDefinitionObject{Body: NewRandomObjectContentContainer(SizeFromString("10K"))},
			"def":    ResourceDefinitionObject{Body: NewRandomObjectContentContainer(SizeFromString("10K"))},
			"foobar": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(SizeFromString("10K"))},
		},
	}
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, srcLoc), srcDef)

	// no s2s, no local->local
	if srcContainer.Location().IsRemote() == dstContainer.Location().IsRemote() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(false),
				},

				AsSubdir: nil, // defaults true
			},
		})

	fromTo := common.FromToValue(srcContainer.Location(), dstContainer.Location())

	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		// todo: service level resource to object mapping
		Objects: GeneratePlanFileObjectsFromMapping(ObjectResourceMappingOverlay{
			Base: srcDef.Objects,
			Overlay: common.Iff(fromTo.AreBothFolderAware() && azCopyVerb == AzCopyVerbCopy, // If we're running copy and in a folder aware , we need to include the root
				ObjectResourceMappingFlat{"": {ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}},
				nil),
		}, GeneratePlanFileObjectsOptions{
			DestPathProcessor: common.Iff(asSubdir, ParentDirDestPathProcessor(srcContainer.ContainerName()), nil),
		}),
	})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: common.Iff[ObjectResourceMapping](asSubdir, ObjectResourceMappingParentFolder{srcContainer.ContainerName(), srcDef.Objects}, srcDef.Objects),
	}, true)
}
*/
