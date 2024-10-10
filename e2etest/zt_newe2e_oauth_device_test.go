package e2etest

import (
	"flag"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/testSuite/cmd"
	"path"
	"strings"
	"time"
)

var runDeviceCodeTest = flag.Bool("device-code", false, "Whether or not to run device code tests. These must be run manually due to interactive nature.")

var manualEnv AzCopyEnvironment

func init() {
	if runDeviceCodeTest != nil && *runDeviceCodeTest {
		suiteManager.RegisterSuite(&DeviceLoginManualSuite{})
	}
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

	// no local->local
	if srcObj.Location().IsLocal() && dstObj.Location().IsLocal() {
		svm.InvalidateScenario()
		return
	}
	azcopyLogPathFolder := cmd.GetAzCopyAppPath()
	azcopyJobPlanFolder := path.Join(azcopyLogPathFolder, "plans")

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Environment: &AzCopyEnvironment{
				ManualLogin:     true,
				LogLocation:     &azcopyLogPathFolder,
				JobPlanLocation: &azcopyJobPlanFolder,
			},
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{})},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	ValidateAADAuth(svm, stdout, srcObj.Location(), dstObj.Location())
}

func ValidateAADAuth(a Asserter, stdout AzCopyStdout, src, dst common.Location) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	// Check for successful login
	loggedInDst := false
	loggedInSrc := false
	for _, p := range stdout.RawStdout() {
		loggedInDst = loggedInDst || strings.Contains(p, "Authenticating to destination using Azure AD")
		loggedInSrc = loggedInSrc || strings.Contains(p, "Authenticating to source using Azure AD")
	}
	a.AssertNow("source login should be successful", Equal{}, loggedInSrc, src.IsRemote())
	a.AssertNow("dest login should be successful", Equal{}, loggedInDst, dst.IsRemote())
}

type DeviceLoginManualSuite struct {
}

func (s *DeviceLoginManualSuite) SetupSuite(a Asserter) {
	stdout := RunAzCopyLoginLogout(a, AzCopyVerbLogin)
	ValidateSuccessfulLogin(a, stdout)
}

func ValidateSuccessfulLogin(a Asserter, stdout AzCopyStdout) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	// Check for successful login
	loggedIn := false
	for _, p := range stdout.RawStdout() {
		loggedIn = loggedIn || strings.Contains(p, "Login succeeded")
	}
	a.AssertNow("login should be successful", Equal{}, loggedIn, true)
}

func (s *DeviceLoginManualSuite) TeardownSuite(a Asserter) {
	RunAzCopyLoginLogout(a, AzCopyVerbLogout)
}

func (s *DeviceLoginManualSuite) Scenario_CopySync(svm *ScenarioVariationManager) {
	Scenario_CopySync(svm, nil)
}
