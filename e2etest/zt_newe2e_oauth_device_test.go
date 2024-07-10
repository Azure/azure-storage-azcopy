package e2etest

import (
	"flag"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

var runDeviceCodeTest = flag.Bool("device-code", false, "Whether or not to run device code tests. These must be run manually due to interactive nature.")

func init() {
	if runDeviceCodeTest != nil && *runDeviceCodeTest {
		suiteManager.RegisterSuite(&DeviceLoginManualSuite{})
	}
}

func Scenario_CopySync(svm *ScenarioVariationManager, env *AzCopyEnvironment) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	//dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
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
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})
	//srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{
	//	ObjectName: pointerTo("test"),
	//	Body:       body,
	//})

	// no local->local
	if srcObj.Location().IsLocal() && dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
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

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	// Validate that the network stats were updated
	ValidateStatsReturned(svm, stdout)
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
