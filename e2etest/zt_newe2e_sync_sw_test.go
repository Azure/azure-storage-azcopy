//go:build smslidingwindow
// +build smslidingwindow

package e2etest

import (
	"runtime"
	"strconv"
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
	dstLoc := ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})

	if srcLoc == common.ELocation.Local() && srcLoc == dstLoc {
		svm.InvalidateScenario()
		return
	}

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

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
			DeleteDestination: pointerTo(deleteDestination),
		},
	})

	ValidateResource[ContainerResourceManager](svm, dstRes, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"deleteme.txt":      ResourceDefinitionObject{ObjectShouldExist: pointerTo(!deleteDestination)},
			"also/deleteme.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(!deleteDestination)},
		},
	}, false)
}

func (s *SWSyncTestSuite) Scenario_TestSyncCreateResources(a *ScenarioVariationManager) {
	// Set up the scenario
	a.InsertVariationSeparator("Local->")
	srcLoc := common.ELocation.Local()
	dstLoc := ResolveVariation(a, []common.Location{common.ELocation.File(), common.ELocation.Blob(), common.ELocation.BlobFS()})
	a.InsertVariationSeparator("|Create:")

	const (
		CreateFolder    = "Folder"
		CreateContainer = "Container"
	)

	resourceType := ResolveVariation(a, []string{CreateFolder})

	a.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(a, []bool{true, false}) // Add variation for DeleteDestination flag

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
				Recursive: pointerTo(false),
			},
			DeleteDestination: pointerTo(deleteDestination),
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
	dstLoc := ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})
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
				Recursive: pointerTo(false),
			},
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
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
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

func (s *SWSyncTestSuite) Scenario_MultiFileUpload(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local()})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

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

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(false),
				},
				DeleteDestination: pointerTo(deleteDestination),
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
			DestPathProcessor: nil,
		}),
	})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcDef.Objects,
	}, true)
}

func (s *SWSyncTestSuite) Scenario_MultiFileUpload_NoChange(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Resolve variation early so name makes sense
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local()})
	// Scale up from service to object
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})
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

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

	sasOpts := GenericAccountSignatureValues{}
	var copySyncFlag CopySyncCommonFlags
	if runtime.GOOS == "linux" {
		copySyncFlag = CopySyncCommonFlags{
			Recursive:               pointerTo(false),
			PreservePOSIXProperties: pointerTo(runtime.GOOS == "linux"),
		}
	} else {
		copySyncFlag = CopySyncCommonFlags{
			Recursive: pointerTo(false),
		}
	}
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
			Flags: SyncFlags{
				CopySyncCommonFlags: copySyncFlag,
				DeleteDestination:   pointerTo(deleteDestination),
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
			DestPathProcessor: nil,
		}),
	})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcDef.Objects,
	}, true)

	//Retrigger Sync with no change in dataset
	RunAzCopy(
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
			Flags: SyncFlags{
				CopySyncCommonFlags: copySyncFlag,
				DeleteDestination:   pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcDef.Objects,
	}, true)
}

// Sync entire directory with subdirectories and files
// Add new files to the source and validate that they are copied to the destination
func (s *SWSyncTestSuite) Scenario_NewFileAdditionAtSource_UploadContainer(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag
	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	srcContainerNew := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	//Create New source where files are renamed
	//srcObjsNew := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := 6; i < 10; i++ {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainerNew, obj)
		}
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainerNew, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(deleteDestination),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

}

func (s *SWSyncTestSuite) Scenario_RenameOfFileAtSource(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.Blob(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(deleteDestination),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	//sleep for 10 seconds to make sure the LMT is in the past
	//time.Sleep(60 * time.Second)
	srcContainerNew := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	//Create New source where files are renamed
	srcObjsNew := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjsNew[dir] = obj
		}
		for i := range 1 {
			name := dir + "/testnew" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjsNew[name] = obj
		}

		for i := 2; i < 5; i++ {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjsNew[name] = obj
		}
	}

	for _, obj := range srcObjsNew {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainerNew, obj)
		}
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainerNew, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(deleteDestination),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjsNew,
	}, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"dir_file_copy_test/test0.txt":                   ResourceDefinitionObject{ObjectShouldExist: pointerTo(!deleteDestination)},
			"dir_file_copy_test/sub_dir_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(!deleteDestination)},
		},
	}, false)
}

// Its failing for the files when deletedestination is false
func (s *SWSyncTestSuite) Scenario_RenameOfFolderAtSource(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.Blob(), common.Elocation.BlobFS()})), ResourceDefinitionContainer{})

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(false),
				},
				DeleteDestination: pointerTo(deleteDestination),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"dir_file_copy_test/sub_dir_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(true)},
		},
	}, false)

	srcContainerNew := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	//Change the sub directory name from dir_file_copy_test/sub_dir_copy_test to dir_file_copy_test/sub_dir_copy_test_new
	dirsToCreateNew := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test_new"}

	//Create New source where files are renamed
	srcObjsNew := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreateNew {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjsNew[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjsNew[name] = obj
		}
	}

	for _, obj := range srcObjsNew {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainerNew, obj)
		}
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainerNew, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(deleteDestination),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjsNew,
	}, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"dir_file_copy_test/sub_dir_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(!deleteDestination)},
		},
	}, false)
}

// this scenario is not working for Azurefiles and blobfs
func (s *SWSyncTestSuite) Scenario_DeleteFileAndCreateFolderWithSameName(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	srcContainerNew := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	//Change the sub directory name from dir_file_copy_test/sub_dir_copy_test to dir_file_copy_test/sub_dir_copy_test_new
	dirsToCreateNew := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test", "dir_file_copy_test/test0.txt"}

	//Create New source where files are renamed
	srcObjsNew := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreateNew {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjsNew[dir] = obj
		}
		for i := 1; i < 5; i++ {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjsNew[name] = obj
		}
	}

	for _, obj := range srcObjsNew {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainerNew, obj)
		}
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainerNew, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjsNew,
	}, true)

	if dstContainer.Location() == common.ELocation.Blob() {
		ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"dir_file_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(true)},
			},
		}, true)
	} else {
		ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"dir_file_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)},
			},
		}, false)
	}
}

func (s *SWSyncTestSuite) Scenario_DeleteFolderAndCreateFileWithSameName(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
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
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	srcContainerNew := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	//Change the sub directory name from dir_file_copy_test/sub_dir_copy_test to dir_file_copy_test/sub_dir_copy_test_new
	dirsToCreateNew := []string{"dir_file_copy_test"}

	//Create New source where files are renamed
	srcObjsNew := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreateNew {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjsNew[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
			srcObjsNew[name] = obj
		}
	}
	name := "dir_file_copy_test/sub_dir_copy_test.txt"
	obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
	srcObjsNew[name] = obj

	for _, obj := range srcObjsNew {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainerNew, obj)
		}
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainerNew, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(false),
					IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjsNew,
	}, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"dir_file_copy_test/sub_dir_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)},
		},
	}, false)
}
