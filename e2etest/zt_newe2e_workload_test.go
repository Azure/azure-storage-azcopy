package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"time"
)

func init() {
	suiteManager.RegisterEarlyRunSuite(&WorkloadIdentitySuite{})
}

type WorkloadIdentitySuite struct{}

// Run only in environments that support and are set up for Workload Identity (ex: Azure Pipeline, Azure Kubernetes Service)
func (s *WorkloadIdentitySuite) Scenario_SingleFileUploadDownloadWorkloadIdentity(svm *ScenarioVariationManager) {
	// Run only in environments that support and are set up for Workload Identity (ex: Azure Pipeline, Azure Kubernetes Service)
	if os.Getenv("NEW_E2E_ENVIRONMENT") != "AzurePipeline" {
		svm.Skip("Workload Identity is only supported in environments specifically set up for it.")
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	body := NewRandomObjectContentContainer(SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	// no local->local
	if srcObj.Location().IsLocal() && dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			Environment: &AzCopyEnvironment{
				AutoLoginMode: pointerTo(common.EAutoLoginType.Workload().String()),
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}
