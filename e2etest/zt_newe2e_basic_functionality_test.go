package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strconv"
	"time"
)

func init() {
	suiteManager.RegisterSuite(&BasicFunctionalitySuite{})
}

type BasicFunctionalitySuite struct{}

func (s *BasicFunctionalitySuite) Scenario_SingleFile(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	// no local->local
	if srcObj.Location().IsLocal() && dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	stdout, _ := RunAzCopy(
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
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	// Validate that the network stats were updated
	ValidateStatsReturned(svm, stdout)
}

func (s *BasicFunctionalitySuite) Scenario_EntireDirectory_S2SContainer(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 10 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	if azCopyVerb == AzCopyVerbSync {
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
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
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *BasicFunctionalitySuite) Scenario_EntireDirectory_UploadContainer(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 10 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
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
			Verb: AzCopyVerbCopy,
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
					Recursive: pointerTo(true),
				},
				AsSubdir: pointerTo(false),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *BasicFunctionalitySuite) Scenario_EntireDirectory_DownloadContainer(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 10 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
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
			Verb: AzCopyVerbCopy,
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
					Recursive: pointerTo(true),
				},
				AsSubdir: pointerTo(false),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *BasicFunctionalitySuite) Scenario_SingleFileUploadDownload_EmptySAS(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())

	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionObject{})

	// no local <-> local
	if srcObj.Location().IsLocal() == dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
				AzCopyTarget{dstObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: true,
		})

	// Validate that the stdout contains the missing sas message
	ValidateErrorOutput(svm, stdout, "Please authenticate using Microsoft Entra ID (https://aka.ms/AzCopy/AuthZ), use AzCopy login, or append a SAS token to your Azure URL.")
}

func (s *BasicFunctionalitySuite) Scenario_Sync_EmptySASErrorCodes(svm *ScenarioVariationManager) {
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())

	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionObject{})

	// no local <-> local
	if srcObj.Location().IsLocal() == dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
				AzCopyTarget{dstObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: true,
		})

	// Validate that the stdout contains these error URLs
	ValidateContainsError(svm, stdout, []string{"https://aka.ms/AzCopyError/NoAuthenticationInformation", "https://aka.ms/AzCopyError/ResourceNotFound"})
}

func (s *BasicFunctionalitySuite) Scenario_Copy_EmptySASErrorCodes(svm *ScenarioVariationManager) {
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())

	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionObject{})

	if srcObj.Location().IsLocal() == dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
				AzCopyTarget{dstObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: true,
		})

	// Validate that the stdout contains these error URLs
	ValidateContainsError(svm, stdout, []string{"https://aka.ms/AzCopyError/NoAuthenticationInformation", "https://aka.ms/AzCopyError/ResourceNotFound"})
}
