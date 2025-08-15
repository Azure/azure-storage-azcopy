package e2etest

import (
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	// Deregistered the example suite so test doesn't run, re-enable for local test writing/usage of example
	//suiteManager.RegisterSuite(&ExampleSuite{})
}

type ExampleSuite struct{}

func (s *ExampleSuite) SetupSuite(a Asserter) {
	//a.Log("Setup logging!")
}

func (s *ExampleSuite) TeardownSuite(a Asserter) {
	//a.Log("Teardown logging!")
	//a.OnError("Oops!")
}

func (s *ExampleSuite) Scenario_SingleFileCopySyncS2S(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	}) // todo: generic CreateResource is something to pursue in another branch, but it's an interesting thought.
	// Scale up from service to container
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})), ResourceDefinitionContainer{})

	if srcObj.Location().IsRemote() == dstContainer.Location().IsRemote() {
		svm.InvalidateScenario()
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy}),
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.BlobPermissions{Read: true, List: true}).String(),
					},
				}),
				dstContainer,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, "test", common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}
