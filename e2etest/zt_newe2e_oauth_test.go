package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

func init() {
	suiteManager.RegisterSuite(&DeviceLoginManualSuite{})
	suiteManager.RegisterSuite(&DeviceLoginAutoSuite{})
}

func Scenario_CopySync(svm *ScenarioVariationManager, env *AzCopyEnvironment) {
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

	body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	// no file -> blob, no local->local
	if srcObj.Location().IsLocal() == dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}
	if srcObj.Location() == common.ELocation.File() && dstObj.Location() == common.ELocation.Blob() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			Environment: env,
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}

type DeviceLoginManualSuite struct {
}

func (s *DeviceLoginManualSuite) SetupSuite(a Asserter) {
	RunAzCopyLoginLogout(a, AzCopyVerbLogin)
}

func (s *DeviceLoginManualSuite) TeardownSuite(a Asserter) {
	RunAzCopyLoginLogout(a, AzCopyVerbLogout)
}

func (s *DeviceLoginManualSuite) Scenario_CopySync(svm *ScenarioVariationManager) {
	Scenario_CopySync(svm, nil)
}

type DeviceLoginAutoSuite struct {
}

func (s *DeviceLoginAutoSuite) Scenario_CopySync(svm *ScenarioVariationManager) {
	Scenario_CopySync(svm, &AzCopyEnvironment{AutoLoginMode: pointerTo(common.AutologinTypeDevice)})
}
