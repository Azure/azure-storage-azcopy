package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&S2SSuite{})
}

type S2SSuite struct{}

func (s *S2SSuite) Scenario_OverwriteSingleFile(svm *ScenarioVariationManager) {
	srcFileName := "test_1kb_copy.txt"
	dstFileName := "test_copy.txt"
	srcBody := NewRandomObjectContentContainer(svm, common.KiloByte)
	dstBody := NewRandomObjectContentContainer(svm, 2*common.KiloByte)

	// TODO : Add S3 to source
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, srcFileName, common.EEntityType.File())
	srcObj.Create(svm, srcBody, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, dstFileName, common.EEntityType.File())
	dstObj.Create(svm, dstBody, ObjectProperties{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			// Overwrite behavior is default
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, dstFileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: srcBody,
	}, true)
}

func (s *S2SSuite) Scenario_NonOverwriteSingleFile(svm *ScenarioVariationManager) {
	srcFileName := "test_1kb_copy.txt"
	dstFileName := "test_copy.txt"
	srcBody := NewRandomObjectContentContainer(svm, common.KiloByte)
	dstBody := NewRandomObjectContentContainer(svm, 2*common.KiloByte)

	// TODO : Add S3 to source
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, srcFileName, common.EEntityType.File())
	srcObj.Create(svm, srcBody, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, dstFileName, common.EEntityType.File())
	dstObj.Create(svm, dstBody, ObjectProperties{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			Overwrite: pointerTo(false),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, dstFileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: dstBody,
	}, true)
}

func (s *S2SSuite) Scenario_SingleFileCopyBlobTypeVariations(svm *ScenarioVariationManager) {
	srcBlobType := ResolveVariation(svm, []blob.BlobType{blob.BlobTypeBlockBlob, blob.BlobTypePageBlob, blob.BlobTypeAppendBlob})
	destBlobType := ResolveVariation(svm, []blob.BlobType{blob.BlobTypeBlockBlob, blob.BlobTypePageBlob, blob.BlobTypeAppendBlob})

	fileName := "test_512b_copy.txt"
	body := NewRandomObjectContentContainer(svm, 512)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{BlobProperties: BlobProperties{Type: pointerTo(srcBlobType)}})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			BlobType: pointerTo(common.FromBlobType(destBlobType)),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(destBlobType),
			},
		},
	}, true)
}
