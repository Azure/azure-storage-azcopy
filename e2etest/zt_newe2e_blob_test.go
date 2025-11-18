package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&BlobTestSuite{})
}

type BlobTestSuite struct{}

func (s *BlobTestSuite) Scenario_UploadBlockBlobs(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_10_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 10 {
		name := "dir_10_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObject, dstContainer.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
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

func (s *BlobTestSuite) Scenario_UploadPageBlob(svm *ScenarioVariationManager) {
	fileName := "test_page_blob_1mb.vHd"
	body := NewRandomObjectContentContainer(common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:   pointerTo(true),
				BlockSizeMB: pointerTo(4.0),
			},
			BlobType: pointerTo(common.EBlobType.PageBlob()),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(blob.BlobTypePageBlob),
			},
		},
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}

func (s *BlobTestSuite) Scenario_SetPageBlobTier(svm *ScenarioVariationManager) {
	fileName := "test_page_blob.vHd"
	body := NewRandomObjectContentContainer(common.KiloByte)
	tier := ResolveVariation(svm, []common.PageBlobTier{common.EPageBlobTier.P10(), common.EPageBlobTier.P20(), common.EPageBlobTier.P30(), common.EPageBlobTier.P4(), common.EPageBlobTier.P40(), common.EPageBlobTier.P50()})

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob(), GetResourceOptions{PreferredAccount: pointerTo(PremiumPageBlobAcct)}), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			BlobType:     pointerTo(common.EBlobType.PageBlob()),
			PageBlobTier: pointerTo(tier),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type:                pointerTo(blob.BlobTypePageBlob),
				BlockBlobAccessTier: pointerTo(tier.ToAccessTierType()),
			},
		},
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}

func (s *BlobTestSuite) Scenario_UploadBlob(svm *ScenarioVariationManager) {
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				srcObj,
				TryApplySpecificAuthType(dstObj, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
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
}

func (s *BlobTestSuite) Scenario_DownloadBlob(svm *ScenarioVariationManager) {
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
				dstObj,
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
}

func (s *BlobTestSuite) Scenario_DownloadBlobRecursive(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_5_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 5 {
		name := "dir_5_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObject, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
				dstContainer,
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

/*
TODO wonw: passes as a container
*/
func (s *BlobTestSuite) Scenario_DownloadBlobNoNameDirectory(svm *ScenarioVariationManager) {
	noNameBlobDirectory := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{
		ObjectName: pointerTo(""),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder()},
		ObjectShouldExist: pointerTo(true),
		Body:              NewRandomObjectContentContainer(SizeFromString("1K")),
	}) //todo should i change this to CRM with GetObject?

	localFolder := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	localObjs := make(ObjectResourceMappingFlat)
	obj := ResourceDefinitionObject{ObjectName: pointerTo("file.txt"), Body: NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()}}
	CreateResource[ObjectResourceManager](svm, localFolder, obj)
	localObjs["file.txt"] = obj

	// This test needs a two-step process. Upload the no-name dir (since creating it will fail), download to test
	stdOut1, _ := RunAzCopy(
		svm, AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				localFolder, noNameBlobDirectory,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					GlobalFlags: GlobalFlags{
						OutputType: pointerTo(cmd.EOutputFormat.Text()),
					},
				},
			},
			ShouldFail: true,
		})

	stdOut, _ := RunAzCopy(
		svm, AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				localFolder, noNameBlobDirectory,
			},
			Flags: CopyFlags{
				ListOfFiles: []string{"file.txt"},
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					GlobalFlags: GlobalFlags{
						OutputType: pointerTo(cmd.EOutputFormat.Text()),
					},
				},
			},
			ShouldFail: true,
		})
	if !svm.Dryrun() {
		svm.t.Log(stdOut)
	}

	// Validate that the stdout contains the missing sas message
	ValidateMessageOutput(svm, stdOut, "The specified file was not found.", true)
}

/*
TODO wonw: passes as a container
func (s *BlobTestSuite) Scenario_DownloadBlobNoNameDirectory(svm *ScenarioVariationManager) {
	noNameBlobDirectory := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{
		ObjectName: pointerTo(""),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder()},
		ObjectShouldExist: pointerTo(true),
		Body:              NewRandomObjectContentContainer(SizeFromString("1K")),
	}) //todo should i change this to CRM with GetObject?

	dstFolder := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	dstFolder.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{EntityType: common.EEntityType.File()})

	srcObjs := make(ObjectResourceMappingFlat)
	obj := ResourceDefinitionObject{ObjectName: pointerTo("file.txt"), Body: NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()}}
	CreateResource[ObjectResourceManager](svm, noNameBlobDirectory, obj)
	srcObjs["file.txt"] = obj

	stdOut, _ := RunAzCopy(
		svm, AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				srcNoNameDirectory, dstFolder,
			},
			Flags: CopyFlags{
				ListOfFiles: []string{"file.txt"},
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					GlobalFlags: GlobalFlags{
						OutputType: pointerTo(cmd.EOutputFormat.Text()),
					},
				},
			},
			ShouldFail: true,
		})
	if !svm.Dryrun() {
		svm.t.Log(stdOut)
	}

	// Validate that the stdout contains the missing sas message
	ValidateMessageOutput(svm, stdOut, "The specified file was not found.", true)
}
*/
