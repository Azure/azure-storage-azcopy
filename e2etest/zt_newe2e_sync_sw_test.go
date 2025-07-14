//go:build smslidingwindow
// +build smslidingwindow

package e2etest

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var UseSyncOrchestrator = true
var SyncThrottlingTestMode = true

type SWSyncTestSuite struct{}

func init() {
	suiteManager.RegisterSuite(&SWSyncTestSuite{})
}

// Helper function to create consistent file content
func createConsistentFileBodies(count int, size string) map[int]ObjectContentContainer {
	fileBodies := make(map[int]ObjectContentContainer)
	for i := 0; i < count; i++ {
		fileBodies[i] = NewRandomObjectContentContainer(SizeFromString(size))
	}
	return fileBodies
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
		Environment: &AzCopyEnvironment{
			SyncThrottling: pointerTo(true), // Enable throttling for this test
		},
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag
	// Scale up from service to object
	srcDef := ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"abc":    ResourceDefinitionObject{Body: NewRandomObjectContentContainer(SizeFromString("10K")), ObjectShouldExist: pointerTo(true)},
			"def":    ResourceDefinitionObject{Body: NewRandomObjectContentContainer(SizeFromString("10K")), ObjectShouldExist: pointerTo(true)},
			"foobar": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(SizeFromString("10K")), ObjectShouldExist: pointerTo(true)},
		},
	}
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, srcLoc), srcDef)

	// no s2s, no local->local
	if srcContainer.Location().IsRemote() == dstContainer.Location().IsRemote() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}
	copySyncFlag := CopySyncCommonFlags{
		Recursive: pointerTo(false),
	}
	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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

	// Create consistent file bodies that can be reused
	fileBodies := createConsistentFileBodies(5, "1K")

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.Blob(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create consistent file bodies that can be reused
	fileBodies := createConsistentFileBodies(5, "1K")

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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

func (s *SWSyncTestSuite) Scenario_DeleteFileAndCreateFolderWithSameName(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.BlobFS(), common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	//Create consistent file bodies that can be reused
	fileBodies := createConsistentFileBodies(5, "1K")

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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

	if dstContainer.Location() == common.ELocation.Blob() || dstContainer.Location() == common.ELocation.BlobFS() {
		ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				// The original file test0.txt should be replaced by the folder
				"dir_file_copy_test/test0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(true)},
				// Files inside the new folder should exist
				"dir_file_copy_test/test0.txt/inside0.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(true)},
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
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	svm.InsertVariationSeparator("_DeleteDestination_")
	deleteDestination := ResolveVariation(svm, []bool{true, false}) // Add variation for DeleteDestination flag

	// Create consistent file bodies that can be reused
	fileBodies := createConsistentFileBodies(5, "1K")

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: fileBodies[i]}
			srcObjsNew[name] = obj
		}
	}

	//deleted folder sub_dir_copy_test and creating file sub_dir_copy_test
	name := "dir_file_copy_test/sub_dir_copy_test"
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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

func (s *SWSyncTestSuite) Scenario_TestFollowLinks(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcBody := NewRandomObjectContentContainer(1024)

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar",
				},
			},
			"fooNew": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar",
				},
			},
			"bar": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	})

	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})

	_, _ = RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy, // sync doesn't support symlinks at this time
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
			Targets: []ResourceManager{
				source, dest,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				FollowSymlinks: pointerTo(true),
				AsSubdir:       pointerTo(false),
			},
		})
	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				Body: srcBody,
			},
			"fooNew": ResourceDefinitionObject{
				Body: srcBody,
			},
			"bar": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	}, false)
}
func (s *SWSyncTestSuite) Scenario_TestFollowLinksFolder(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcBody := NewRandomObjectContentContainer(1024)

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar/",
				},
			},
			"fooNew": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar/",
				},
			},
			"bar/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
			},
			"bar/file.txt": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	})

	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	_, _ = RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync, // sync doesn't support symlinks at this time
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
			Targets: []ResourceManager{
				source, dest,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(false),
				},
			},
		})
	//get the container which is created by the azcopy command inside dest
	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo/file.txt": ResourceDefinitionObject{
				Body: srcBody,
			},
			"fooNew/file.txt": ResourceDefinitionObject{
				Body: srcBody,
			},
			"bar/file.txt": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	}, true)
}

func (s *SWSyncTestSuite) Scenario_FileMetadataModTimeChange(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync})
	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})
	body := NewRandomObjectContentContainer(SizeFromString("10K"))

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.File(),
				},
				Body: body,
			},
		},
	})
	filePath := source.URI() + "/foo"
	stats, _ := os.Stat(filePath)
	fileInfo, _ := stats.(os.FileInfo)
	var modTime time.Time
	if fileInfo != nil {
		modTime = fileInfo.ModTime()
	}
	modTimeUnix := modTime.UTC().Unix()
	mtimeStr := strconv.FormatInt(modTimeUnix, 10)
	metadata := common.Metadata{
		"Modtime": pointerTo(mtimeStr),
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
			Targets: []ResourceManager{
				TryApplySpecificAuthType(source, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dest, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:               pointerTo(false),
					PreservePOSIXProperties: pointerTo(true),
					IncludeDirectoryStubs:   pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})
	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				Body: body,
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.File(),
					Metadata:   metadata,
				},
			},
		},
	}, true)

	//update the modtime of the file and perfom sync. Ensure that modtime is update in the destination
	newModTime := time.Now().UTC().Add(+1 * time.Hour).Unix()
	newModTimeUnix := time.Unix(newModTime, 0)
	os.Chtimes(filePath, newModTimeUnix, newModTimeUnix)
	mtimeNewStr := strconv.FormatInt(newModTime, 10)
	Newmetadata := common.Metadata{
		"Modtime": pointerTo(mtimeNewStr),
	}
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
			Targets: []ResourceManager{
				TryApplySpecificAuthType(source, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dest, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:               pointerTo(false),
					PreservePOSIXProperties: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})
	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				Body: body,
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.File(),
					Metadata:   Newmetadata,
				},
			},
		},
	}, true)

}

func (s *SWSyncTestSuite) Scenario_FolderMetadataModTimeChange(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync})
	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.BlobFS()})), ResourceDefinitionContainer{})
	body := NewRandomObjectContentContainer(SizeFromString("10K"))

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
			},
			"foo/file.txt": ResourceDefinitionObject{
				Body: body,
			},
		},
	})
	folderPath := source.URI() + "/foo"
	stats, _ := os.Stat(folderPath)
	folderInfo, _ := stats.(os.FileInfo)
	var modTime time.Time
	if folderInfo != nil {
		modTime = folderInfo.ModTime()
	}
	modTimeUnix := modTime.UTC().Unix()
	mtimeStr := strconv.FormatInt(modTimeUnix, 10)
	metadata := common.Metadata{
		"Modtime": pointerTo(mtimeStr),
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
			Targets: []ResourceManager{
				TryApplySpecificAuthType(source, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dest, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:               pointerTo(false),
					PreservePOSIXProperties: pointerTo(true),
					IncludeDirectoryStubs:   pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})
	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
					Metadata:   metadata,
				},
			},
		},
	}, true)

	//update the modtime of the folder and perfom sync. Ensure that modtime is update in the destination
	newModTime := time.Now().UTC().Add(+1 * time.Hour).Unix()
	newModTimeUnix := time.Unix(newModTime, 0)
	os.Chtimes(folderPath, newModTimeUnix, newModTimeUnix)
	mtimeNewStr := strconv.FormatInt(newModTime, 10)
	Newmetadata := common.Metadata{
		"Modtime": pointerTo(mtimeNewStr),
	}
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
			Targets: []ResourceManager{
				TryApplySpecificAuthType(source, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dest, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:               pointerTo(false),
					PreservePOSIXProperties: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})
	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
					Metadata:   Newmetadata,
				},
			},
		},
	}, true)

}

func (s *SWSyncTestSuite) Scenario_AddNonEmptyFolder(svm *ScenarioVariationManager) {
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
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
					//IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	//Add new diretory dir_file_copy_test/new_folder
	dirsToCreateNew := []string{"dir_file_copy_test/new_folder"}

	for _, dir := range dirsToCreateNew {
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

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
					//IncludeDirectoryStubs: pointerTo(true),
				},
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

}

func (s *SWSyncTestSuite) Scenario_DeleteNonEmptyFolder(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
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
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)

	//Delete diretory dir_file_copy_test/sub_dir_copy_test
	dirsToCreateNew := []string{"dir_file_copy_test"}
	folderPath := srcContainer.URI() + "dir_file_copy_test/sub_dir_copy_test"
	//construct srcObjsNew with only dir_file_copy_test
	srcObjsNew := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreateNew {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjsNew[dir] = obj

		}
		for i := range 5 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
			srcObjsNew[name] = obj
		}
	}

	os.Remove(folderPath)
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				SyncThrottling: pointerTo(true), // Enable throttling for this test
			},
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
				DeleteDestination: pointerTo(true),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjsNew,
	}, true)

}
