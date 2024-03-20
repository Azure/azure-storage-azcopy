package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strings"
)

func init() {
	suiteManager.RegisterSuite(&FileOAuthTestSuite{})
}

type FileOAuthTestSuite struct{}

func (s *FileOAuthTestSuite) Scenario_FileBlobOAuthError(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcService := CreateResource[ServiceResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()})), ResourceDefinitionService{})
	dstService := CreateResource[ServiceResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionService{})

	dstAuth := ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcService, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstService, dstAuth, svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: true,
		})

	for _, line := range stdout.RawOutput {
		if strings.Contains(line, "S2S copy from Azure File authenticated with Azure AD to Blob/BlobFS is not supported") {
			return
		}
	}
	svm.Error("expected error message not found in azcopy output")
}
