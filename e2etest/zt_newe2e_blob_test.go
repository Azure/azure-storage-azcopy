package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strconv"
)

func init() {
	suiteManager.RegisterSuite(&BlobTestSuite{})
}

type BlobTestSuite struct{}

func (s *BasicFunctionalitySuite) Scenario_UploadBlockBlobs(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_10_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 10 {
		name := "dir_10_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
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
	}, true)
}

func (s *BasicFunctionalitySuite) Scenario_UploadPageBlob(svm *ScenarioVariationManager) {
	fileName := "test_page_blob_1mb.vHd"
	body := NewRandomObjectContentContainer(svm, common.MegaByte)

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
	}, true)
}

func (s *BasicFunctionalitySuite) Scenario_UploadBlob(svm *ScenarioVariationManager) {
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	body := NewRandomObjectContentContainer(svm, SizeFromString("1K"))
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
	}, true)
}

func (s *BasicFunctionalitySuite) Scenario_DownloadBlob(svm *ScenarioVariationManager) {
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	body := NewRandomObjectContentContainer(svm, SizeFromString("1K"))
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
	}, true)
}
