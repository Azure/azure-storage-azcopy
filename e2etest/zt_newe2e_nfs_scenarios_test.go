package e2etest

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&FilesNFSTestSuite{})
}

type FilesNFSTestSuite struct{}

func (s *FilesNFSTestSuite) Scenario_TransferData(svm *ScenarioVariationManager) {

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())

	body := NewRandomObjectContentContainer(SizeFromString("10K"))
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
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
	fmt.Println("stdOut", stdOut)
	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

}
