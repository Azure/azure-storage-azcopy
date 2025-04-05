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

	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")

	body := NewRandomObjectContentContainer(SizeFromString("10K"))
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test.txt"),
		Body:       body,
		ObjectProperties: ObjectProperties{
			//FileNFSProperties: &FileNFSProperties{
			//	FileCreationTime:  pointerTo(time.Now()),
			//	FileLastWriteTime: pointerTo(time.Now()),
			//},
			FileNFSPermissions: &FileNFSPermissions{
				FileMode: pointerTo("0755"),
			},
		},
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
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					NFS:                 pointerTo(true),
					PreserveProperties:  pointerTo(true),
					PreservePermissions: pointerTo(true),
				},
			},
		})
	fmt.Println("stdOut", stdOut)
	dstObj := dstContainer.GetObject(svm, "test.txt", common.EEntityType.File())
	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body:             body,
		ObjectProperties: srcObj.GetProperties(svm),
	}, true)
}
