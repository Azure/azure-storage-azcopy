package e2etest

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&FileOAuthTestSuite{})
}

type FileOAuthTestSuite struct{}

func (s *FileOAuthTestSuite) Scenario_FileBlobOAuthError(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	dstAuth := ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstContainer, dstAuth, svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: true,
		})

	ValidateErrorOutput(svm, stdout, fmt.Sprintf("S2S %s from Azure File authenticated with Azure AD to Blob/BlobFS is not supported", azCopyVerb))
}

// @brief This scenario performs sync operation from local to file share.
// @validation This test verifies if the warning message is displayed on console if the
// user tries to perform sync operation from local to file share.
func (s *FileOAuthTestSuite) Scenario_SyncFromLocalToFSWarningMsg(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})

	dstAuth := ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstContainer, dstAuth, svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: false,
		})

	ValidateErrorOutput(svm, stdout, fmt.Sprintf("DRYRUN: warn %s", cmd.LocalToFileShareWarnMsg))
}
