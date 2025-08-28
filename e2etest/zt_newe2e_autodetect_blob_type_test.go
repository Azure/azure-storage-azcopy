package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&AutoDetectBlobTypeTestSuite{})
}

type AutoDetectBlobTypeTestSuite struct{}

func (s *AutoDetectBlobTypeTestSuite) Scenario_AutoInferDetectBlobTypeVHD(svm *ScenarioVariationManager) {
	fileName := "myVHD.vHd" // awkward capitalization to see if AzCopy catches it.
	body := NewRandomObjectContentContainer(4 * common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.FileSMB(), common.ELocation.Blob()})), ResourceDefinitionContainer{}).
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
				Type: common.Iff(srcObj.Location() == common.ELocation.Blob(), pointerTo(blob.BlobTypeBlockBlob), pointerTo(blob.BlobTypePageBlob)),
			},
		},
	}, true)
}
