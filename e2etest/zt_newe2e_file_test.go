package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	//suiteManager.RegisterSuite(&FileTestSuite{})
}

type FileTestSuite struct{}

func (s *FileTestSuite) Scenario_FileUploadDownloadDifferentSizes(svm *ScenarioVariationManager) {
	size := ResolveVariation(svm, []int64{0, 1, 4*common.MegaByte - 1, 4 * common.MegaByte, 4*common.MegaByte + 1})
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File()) // awkward capitalization to see if AzCopy catches it.

	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})

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
	}, true)
}
