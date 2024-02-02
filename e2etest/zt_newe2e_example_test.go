package e2etest

import (
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&ExampleSuite{})
}

type ExampleSuite struct{}

func (s *ExampleSuite) SetupSuite(a Asserter) {
	//a.Log("Setup logging!")
}

func (s *ExampleSuite) TeardownSuite(a Asserter) {
	//a.Log("Teardown logging!")
	//a.Error("Oops!")
}

func (s *ExampleSuite) Scenario_SingleFileCopySyncS2S(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()}))
	//svm.InsertVariationSeparator("->")
	dstService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	}) // todo: generic CreateResource is something to pursue in another branch, but it's an interesting thought.
	// Scale up from service to container
	dstObj := CreateResource[ContainerResourceManager](svm, dstService, ResourceDefinitionContainer{})

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy}),
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						Permissions: (&blobsas.BlobPermissions{Read: true, List: true}).String(),
					},
				}),
				dstObj,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},

				ListOfFiles: []string{"test"},
			},
		})

	ValidateResource[ObjectResourceManager](svm, srcObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}
