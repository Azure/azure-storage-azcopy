package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&AutoDetectBlobTypeSuite{})
}

type AutoDetectBlobTypeSuite struct{}

func (s *AutoDetectBlobTypeSuite) Scenario_InferBlobTypeVhdWrongCase(svm *ScenarioVariationManager) {
	fileName := "myVHD.vHd"
	body := NewRandomObjectContentContainer(svm, 4*common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File()) // awkward capitalization to see if AzCopy catches it.

	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:   pointerTo(true),
				BlockSizeMB: pointerTo(4.0),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(blob.BlobTypePageBlob),
			},
		},
	}, true)
}

func (s *AutoDetectBlobTypeSuite) Scenario_InferBlobTypeFromFileToBlob(svm *ScenarioVariationManager) {
	fileName := "testS2SVHD.vhd"
	body := NewRandomObjectContentContainer(svm, 4*common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(blob.BlobTypePageBlob),
			},
		},
	}, true)
}

func (s *AutoDetectBlobTypeSuite) Scenario_DetectBlobTypeFromBlobToBlob(svm *ScenarioVariationManager) {
	fileName := "testS2SVHD.vhd"
	body := NewRandomObjectContentContainer(svm, 4*common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			BlobType: pointerTo(common.EBlobType.BlockBlob()),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(blob.BlobTypeBlockBlob),
			},
		},
	}, true)
}
