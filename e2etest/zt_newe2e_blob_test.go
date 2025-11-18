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
Scenario_DownloadBlobNoNameDirectory validates we report errors when
downloading from a blobURL containing an unnamed directory and a wildcard pattern. I.e '//*'

E.g https:/acct.blob/container//* AzCopy and the AzBlob SDK URL parser normalizes the path
in occurrences - stripTrailingWildCardOnRemoteSources(), Traverser.SplitResourceString()

Which causes the path to not be found. This tests we do not fail silently in that case.
*/
func (s *BlobTestSuite) Scenario_DownloadBlobNoNameDirectory(svm *ScenarioVariationManager) {
	// Test uses a two-step upload, download to replicate the leading-slash blob scenario without tripping the SDK’s empty-name validation

	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	blobContainer := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.Blob(), GetResourceOptions{PreferredAccount: pointerTo(PrimaryStandardAcct)}),
		ResourceDefinitionContainer{},
	)

	srcLocal := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcFile := CreateResource[ObjectResourceManager](svm, srcLocal, ResourceDefinitionObject{
		ObjectName: pointerTo("image.png"),
		Body:       body,
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	dstLocal := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	// Upload to https:/acct/container//image.png
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			srcFile,
			TryApplySpecificAuthType(blobContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "//image.png"}),
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})
	ValidateResource[ObjectResourceManager](svm, blobContainer.GetObject(svm, "/image.png", common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, ValidateResourceOptions{validateObjectContent: false})

	// Download using list-of-files from https://container//
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(blobContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "//*"}),
			dstLocal,
		},
		Flags: CopyFlags{
			ListOfFiles: []string{"image.png"},
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
		ShouldFail: true,
	})
	ValidateMessageOutput(svm, stdOut, "The specified file was not found.", true)
}

// Scenario_DownloadBlobNamedDirectory validates list-of-files compatible with downloads from *named* virtual directories
func (s *BlobTestSuite) Scenario_DownloadBlobNamedDirectory(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	srcCont := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	namedBlobDirectory := CreateResource[ObjectResourceManager](svm, srcCont, ResourceDefinitionObject{
		ObjectName: pointerTo("dir"),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder()},
		ObjectShouldExist: pointerTo(true),
		Body:              body,
	})

	srcObjs := make(ObjectResourceMappingFlat)
	obj := ResourceDefinitionObject{
		ObjectName:       pointerTo("dir/file.txt"),
		Body:             body,
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()}}
	CreateResource[ObjectResourceManager](svm, srcCont, obj)
	srcObjs["dir/file.txt"] = obj

	dstFolder := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	RunAzCopy(
		svm, AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				namedBlobDirectory, dstFolder,
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
			ShouldFail: false,
		})
	ValidateResource[ObjectResourceManager](svm, dstFolder,
		ResourceDefinitionObject{ObjectName: pointerTo("file.txt")},
		ValidateResourceOptions{validateObjectContent: false})

}

// Scenario_DownloadBlobNoNameWithoutWildcardDirectory tests downloading blobs with leading slash paths (like "/image.png") and list-of-files
// work without using a wildcard
func (s *BlobTestSuite) Scenario_DownloadBlobNoNameWithoutWildcardDirectory(svm *ScenarioVariationManager) {
	// Test uses a two-step upload, download to replicate the leading-slash blob scenario without tripping the SDK’s empty-name validation

	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	blobContainer := CreateResource[ContainerResourceManager](svm,
		GetRootResource(svm, common.ELocation.Blob(), GetResourceOptions{PreferredAccount: pointerTo(PrimaryStandardAcct)}),
		ResourceDefinitionContainer{},
	)

	srcLocal := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcFile := CreateResource[ObjectResourceManager](svm, srcLocal, ResourceDefinitionObject{
		ObjectName: pointerTo("image.png"),
		Body:       body,
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	dstLocal := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	// Upload to https:/acct/container//image.png
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			srcFile,
			TryApplySpecificAuthType(blobContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "//image.png"}),
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})
	ValidateResource[ObjectResourceManager](svm, blobContainer.GetObject(svm, "/image.png", common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, ValidateResourceOptions{validateObjectContent: false})

	// Download using list-of-files from https://container//
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(blobContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "//"}),
			dstLocal,
		},
		Flags: CopyFlags{
			ListOfFiles: []string{"image.png"},
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
				GlobalFlags: GlobalFlags{
					OutputType: pointerTo(cmd.EOutputFormat.Text()),
				},
			},
		},
		ShouldFail: false,
	})
	ValidateResource[ObjectResourceManager](svm, dstLocal,
		ResourceDefinitionObject{Body: body},
		ValidateResourceOptions{validateObjectContent: false})
}

func (s *BlobTestSuite) Scenario_DownloadBlobObjNoNameDirectory(svm *ScenarioVariationManager) {
	// Test uses a two-step upload, download upload then download to replicate the leading-slash blob scenario without tripping the SDK’s empty-name validation
	blobContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	localSrc := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectName:       pointerTo("a.txt"),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			localSrc,
			TryApplySpecificAuthType(blobContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "//a.txt"}),
		},
		Flags: CopyFlags{CopySyncCommonFlags: CopySyncCommonFlags{Recursive: pointerTo(true)}},
	})

	dstLocal := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	blobObj := blobContainer.GetObject(svm, "/a.txt", common.EEntityType.File())
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			blobObj,
			dstLocal,
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
				FromTo:    pointerTo(common.EFromTo.BlobLocal()),
				GlobalFlags: GlobalFlags{
					OutputType: pointerTo(cmd.EOutputFormat.Text()),
				},
			},
		},
		ShouldFail: true,
	})
}
