package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&ErrorTestSuite{})
}

type ErrorTestSuite struct{}

func (s *ErrorTestSuite) Scenario_FileBlobOAuthError(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early

	srcService := CreateResource[ServiceResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()})), ResourceDefinitionService{})
	dstService := CreateResource[ServiceResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionService{})

	dstAuth := ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()})

	RunAzCopy(
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
}
