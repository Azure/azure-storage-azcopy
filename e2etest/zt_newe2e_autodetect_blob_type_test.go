package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&AutoDetectBlobTypeTestSuite{})
}

type AutoDetectBlobTypeTestSuite struct{}

func (s *AutoDetectBlobTypeTestSuite) Scenario_AutoInferBlobTypeVHD(svm *ScenarioVariationManager) {
	fileName := "myVHD.vHd" // awkward capitalization to see if AzCopy catches it.
	body := NewRandomObjectContentContainer(svm, 4*common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	// copy vhd file without specifying page blob. Page blob is inferred for VHD, VHDX, and VMDK
	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:   pointerTo(true),
				BlockSizeMB: pointerTo(4.0),
			},
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

func (s *AutoDetectBlobTypeTestSuite) Scenario_InferBlobTypeFilePageBlob(svm *ScenarioVariationManager) {
	fileName := "testS2SVHD.vhd"
	body := NewRandomObjectContentContainer(svm, 4*common.MegaByte)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcContainer, dstContainer},
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

func (s *AutoDetectBlobTypeTestSuite) Scenario_DetectBlobTypeBlobBlob(svm *ScenarioVariationManager) {
	fileName := "testS2SVHD.vhd"
	body := NewRandomObjectContentContainer(svm, 4*common.MegaByte)

	// Upload to Azure Blob Storage as Block Blob and detect as Block Blob.
	// AzCopy detects the source blob type.
	// This means that in all scenarios EXCEPT Blob -> Blob, .vhd corresponds to page blob.
	// However, in Blob -> Blob, we preserve the blob type instead of detecting it.
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcContainer, dstContainer},
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
				Type: pointerTo(blob.BlobTypeBlockBlob),
			},
		},
	}, true)
}
