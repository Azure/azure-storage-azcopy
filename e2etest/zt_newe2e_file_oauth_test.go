package e2etest

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&FileOAuthTestSuite{})
}

type FileOAuthTestSuite struct{}

func (s *FileOAuthTestSuite) Scenario_FileBlobOAuthSyncError(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{}, true)
	// Should error out
	ValidateContainsError(svm, stdout,
		[]string{"S2S sync from Azure File authenticated with Azure AD to Blob/BlobFS is not supported"})
}

func (s *FileOAuthTestSuite) Scenario_FileBlobOAuthNoError(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{}, true)
	// Should not error out
	ValidateDoesNotContainError(svm, stdout,
		[]string{"S2S copy from Azure File authenticated with Azure AD to Blob/BlobFS is not supported"})
}

func (s *FileOAuthTestSuite) Scenario_FileBlobOAuth(svm *ScenarioVariationManager) {
	name := "test"
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.File()),
		ResourceDefinitionObject{
			ObjectName: &name,
			Body:       NewRandomObjectContentContainer(SizeFromString("10k")),
		})
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).GetObject(svm,
		name, common.EEntityType.File())

	stdOut, _ := RunAzCopy(svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{})},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					FromTo:    pointerTo(common.EFromTo.FileBlob()),
				},
				BlobType: pointerTo(ResolveVariation(svm,
					[]common.BlobType{common.EBlobType.PageBlob(),
						common.EBlobType.AppendBlob(),
						common.EBlobType.BlockBlob()})),
			},
		})
	svm.t.Log(fmt.Sprintf("Stdout %v", stdOut))
	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{}, true)
}
