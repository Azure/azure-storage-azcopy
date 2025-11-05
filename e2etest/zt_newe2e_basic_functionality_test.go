package e2etest

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&BasicFunctionalitySuite{})
}

type BasicFunctionalitySuite struct{}

func (s *BasicFunctionalitySuite) Scenario_SingleFile(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
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
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, srcLoc), ResourceDefinitionObject{
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
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})

	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		Objects: map[PlanFilePath]PlanFileObject{
			PlanFilePath{SrcPath: "", DstPath: ""}: {
				Properties: ObjectProperties{},
			},
		},
	})
}

func (s *BasicFunctionalitySuite) Scenario_MultiFileUploadDownload(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Resolve variation early so name makes sense
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})
	// Scale up from service to object
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

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

	var asSubdir bool
	if azCopyVerb == AzCopyVerbCopy {
		svm.InsertVariationSeparator("-Subdir:")
		asSubdir = ResolveVariation(svm, []bool{true, false})
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
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},

				AsSubdir: common.Iff(azCopyVerb == AzCopyVerbCopy, &asSubdir, nil), // defaults true
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
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
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
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
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
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
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
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
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
	ValidateMessageOutput(svm, stdout, "Please authenticate using Microsoft Entra ID (https://aka.ms/AzCopy/AuthZ), use AzCopy login, or append a SAS token to your Azure URL.", true)
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

// Test of Copy to UnsafeDestinations (Windows Local dest)
func (s *BasicFunctionalitySuite) Scenario_CopyUnSafeDest(svm *ScenarioVariationManager) {
	azCopyVerb := AzCopyVerbCopy

	fileName := "file."
	body := NewRandomObjectContentContainer(SizeFromString("10K"))

	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm,
		GetRootResource(svm, common.ELocation.File()), // Only source is File - Windows Local & Blob doesn't support T.D
		ResourceDefinitionObject{
			ObjectName: pointerTo(fileName),
			Body:       body,
		})

	dstObj := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm,
			ResolveVariation(svm, []common.Location{common.ELocation.File(),
				common.ELocation.Local()})),
		ResourceDefinitionContainer{}).GetObject(svm, "file", common.EEntityType.File())

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.SASToken(), svm,
					CreateAzCopyTargetOptions{
						SASTokenOptions: sasOpts,
					}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm,
					CreateAzCopyTargetOptions{
						SASTokenOptions: sasOpts,
					}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:   pointerTo(true),
					TrailingDot: to.Ptr(common.ETrailingDotOption.AllowToUnsafeDestination()), // Allow download to unsafe Local destination
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj,
		ResourceDefinitionObject{
			Body: body,
		}, ValidateResourceOptions{
			validateObjectContent: false,
		})
}

func (s *BasicFunctionalitySuite) Scenario_TagsPermission(svm *ScenarioVariationManager) {
	objectType := ResolveVariation(svm, []common.EntityType{common.EEntityType.File(), common.EEntityType.Folder(), common.EEntityType.Symlink()})
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})

	// Local resource manager doesn't have symlink abilities yet, and the same codepath is hit.
	if objectType == common.EEntityType.Symlink() && srcLoc == common.ELocation.Local() {
		svm.InvalidateScenario()
		return
	}

	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, srcLoc), ResourceDefinitionObject{
		Body: common.Iff(objectType == common.EEntityType.File(), NewZeroObjectContentContainer(1024*1024*5), nil),
		ObjectProperties: ObjectProperties{
			EntityType: objectType,
		},
	})
	dstCt := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	svm.InsertVariationSeparator("Blob")

	multiBlock := "single-block"
	var blobType common.BlobType
	if objectType == common.EEntityType.File() {
		svm.InsertVariationSeparator("-")
		blobType = ResolveVariation(svm, []common.BlobType{common.EBlobType.BlockBlob(), common.EBlobType.PageBlob(), common.EBlobType.AppendBlob()})

		if blobType == common.EBlobType.BlockBlob() {
			svm.InsertVariationSeparator("-")
			multiBlock = ResolveVariation(svm, []string{"single-block", "multi-block"})
		}
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				srcObj,
				AzCopyTarget{dstCt, EExplicitCredentialType.SASToken(), CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: dstCt.ContainerName(),
						Permissions: PtrOf(blobsas.ContainerPermissions{
							Read:   true,
							Add:    true,
							Create: true,
							Write:  true,
							Tag:    false,
						}).String(),
					},
				}},
			},
			Flags: CopyFlags{
				BlobTags: common.Metadata{
					"foo":   PtrOf("bar"),
					"alpha": PtrOf("beta"),
				},
				CopySyncCommonFlags: CopySyncCommonFlags{
					BlockSizeMB: common.Iff(objectType == common.EEntityType.File() && multiBlock != "single-block",
						PtrOf(0.5),
						nil),
					Recursive:             pointerTo(true),
					IncludeDirectoryStubs: pointerTo(true),
				},
				BlobType:         &blobType,
				PreserveSymlinks: pointerTo(true),
			},
			ShouldFail: true,
		},
	)

	ValidateMessageOutput(svm, stdOut, "Authorization failed during an attempt to set tags, please ensure you have the appropriate Tags permission", true)
}

// This scenario tests that if the AZCOPY_CONCURRENCY_VALUE is set azcopy should honor this.
func (s *BasicFunctionalitySuite) Scenario_ConcurrencyValueSet(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Resolve variation early so name makes sense
	srcLoc := ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})
	// Scale up from service to object
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionContainer{})

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

	var asSubdir bool
	if azCopyVerb == AzCopyVerbCopy {
		svm.InsertVariationSeparator("-Subdir:")
		asSubdir = ResolveVariation(svm, []bool{true, false})
	}

	stdOut, _ := RunAzCopy(
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

				AsSubdir: common.Iff(azCopyVerb == AzCopyVerbCopy, &asSubdir, nil), // defaults true
			},
			Environment: &AzCopyEnvironment{
				AzcopyConcurrencyValue: pointerTo("64"),
			},
		})

	ValidateMessageOutput(svm, stdOut, "concurrent connections", false)
}

// Scenario_CheckVersion test version info is only printed explicitly when --check-version is used.
func (*BasicFunctionalitySuite) Scenario_CheckVersion(svm *ScenarioVariationManager) {
	if strings.Contains(common.AzcopyVersion, "preview") {
		svm.Skip("Check version does not print output for preview versions.")
	}
	// The flag usage is `azcopy --check-version` without sub-commands.
	// So, no need to pass azcopy verb
	stdout, _ := RunAzCopy(svm, AzCopyCommand{
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				GlobalFlags: GlobalFlags{
					CheckVersion: pointerTo(true),
					OutputType:   pointerTo(cmd.EOutputFormat.Text()),
				},
			},
		},
	})

	versionCheckOpts := []*regexp.Regexp{
		regexp.MustCompile("INFO: azcopy.* .*: A newer version .* is available to download"),
		regexp.MustCompile("INFO: Current AzCopy version *.*.* is up to date"),
	}

	if !svm.Dryrun() {
		foundVersionInOutput := func() bool { // Check if either of the version output is in the string
			matched := false
			for _, line := range stdout.RawStdout() { // If there's another warning or info message
				for _, regex := range versionCheckOpts {
					if regex.MatchString(line) {
						matched = true
						return matched
					}
				}
			}
			return matched
		}
		versionAssertion := assert.New(svm.t)
		versionAssertion.True(foundVersionInOutput())
	}

	ValidateMessageOutput(svm, stdout, "version", true) // loose check
}

// Scenario_CheckVersion test version info is not printed when the flag is not used.
func (*BasicFunctionalitySuite) Scenario_DisabledCheckVersion(svm *ScenarioVariationManager) {
	// The flag usage is `azcopy --check-version` without sub-commands.
	// So, no need to create src and dest
	stdout, _ := RunAzCopy(svm, AzCopyCommand{
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				GlobalFlags: GlobalFlags{
					// CheckVersion: pointerTo(false), default is False
					OutputType: pointerTo(cmd.EOutputFormat.Text()),
				},
			},
		},
	})

	versionCheckOpts := []*regexp.Regexp{
		regexp.MustCompile("INFO: azcopy.* .*: A newer version .* is available to download"),
		regexp.MustCompile("INFO: Current AzCopy version *.*.* is up to date"),
	}

	if !svm.Dryrun() {
		foundVersionInOutput := func() bool { // Check if either of the version output is in the string
			matched := false
			for _, line := range stdout.RawStdout() { // If there's another warning or info message
				for _, regex := range versionCheckOpts {
					if regex.MatchString(line) {
						matched = true
						return matched
					}
				}
			}
			return matched
		}
		versionAssertion := assert.New(svm.t)
		versionAssertion.False(foundVersionInOutput())
	}

	ValidateMessageOutput(svm, stdout, "version", false)
}

// Scenario_SkipVersionCheckDisabledBackCompat validates we are backwards compatible with skip-version-check now deprecated.
func (*BasicFunctionalitySuite) Scenario_SkipVersionCheckDisabledBackCompat(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})),
		ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
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
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})),
		ResourceDefinitionObject{
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
					GlobalFlags: GlobalFlags{
						SkipVersionCheck: pointerTo(false),
					},
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})

	ValidateDoesNotContainError(svm, stdOut, []string{"unknown flag: --skip-version-check"})

}

func (s *BasicFunctionalitySuite) Scenario_JobResume(svm *ScenarioVariationManager) {
	// Create a service resource manager
	svc := GetRootResource(svm, common.ELocation.Blob()).(ServiceResourceManager)
	// Pick a never-before-used name (unix nano to avoid collisions)
	dstName := "missing" + strconv.FormatInt(time.Now().UnixNano(), 36)
	// Just get a handle; no network call that creates the container
	dstContainer := svc.GetContainer(dstName)

	body := NewRandomObjectContentContainer(SizeFromString("10M"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local()})), ResourceDefinitionObject{
		Body: body,
	})

	// no local->local
	if srcObj.Location().IsLocal() && dstContainer.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	// Create a single AzCopyEnvironment and reuse it for both runs so logs/plans land in the same /<scenario>/<000> folder.
	envCtx := FetchAzCopyEnvironmentContext(svm)
	env := &AzCopyEnvironment{
		InheritEnvironment: map[string]bool{"*": true},
	}

	// Build the temp path for that environment and set explicit plan/log locations
	envTmpPath := envCtx.GetEnvTempPath(env) // -> .../<scenario>/<000>
	env.JobPlanLocation = pointerTo(filepath.Join(envTmpPath, PlanSubdir))
	env.LogLocation = pointerTo(filepath.Join(envTmpPath, LogSubdir))

	// Ensure directories exist so azcopy can write to them
	_ = os.MkdirAll(filepath.Join(envTmpPath, PlanSubdir), 0o777)
	_ = os.MkdirAll(filepath.Join(envTmpPath, LogSubdir), 0o777)

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(srcObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
		ShouldFail:  true,
		Environment: env,
	})

	// Assertions to check if above copy job failed with final status Failed
	if !svm.Dryrun() {
		firstParsed, ok := stdOut.(*AzCopyParsedCopySyncRemoveStdout)
		svm.Assert("could not parse first job stdout", Equal{}, ok, true)
		svm.Assert("first run did not show failed status", Equal{}, firstParsed.FinalStatus.JobStatus, common.EJobStatus.Failed())
		svm.Assert("first run failed transfers not equal to 1", Equal{}, firstParsed.FinalStatus.TransfersFailed, uint32(1))
	}

	// Create a ContainerResourceManager which will create a container for the azcopy job resume command to succeed
	dstContainer = CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionContainer{ContainerName: pointerTo(dstName)})

	// Find the Job ID for the above azcopy copy job
	var jobId string
	if !svm.Dryrun() {
		if parsedOut, ok := stdOut.(*AzCopyParsedCopySyncRemoveStdout); ok {
			if parsedOut.InitMsg.JobID != "" {
				jobId = parsedOut.InitMsg.JobID
			}
		}
	}

	resStdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsResume,
			PositionalArgs: []string{jobId},
			Environment:    env,
		})

	// Assertions to check if JobResume succeeded
	if !svm.Dryrun() {
		resumeParsed, rok := resStdOut.(*AzCopyParsedCopySyncRemoveStdout)
		svm.Assert("could not parse resume stdout", Equal{}, rok, true)
		svm.Assert("resume did not lead to job completion", Equal{}, resumeParsed.FinalStatus.JobStatus, common.EJobStatus.Completed())
		svm.Assert("resume completed transfers not equal to 1", Equal{}, resumeParsed.FinalStatus.TransfersCompleted, uint32(1))
	}
}
