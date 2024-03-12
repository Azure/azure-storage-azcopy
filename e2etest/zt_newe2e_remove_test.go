package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&RemoveSuite{})
}

type RemoveSuite struct{}

func (s *RemoveSuite) SetupSuite(a Asserter) {
	//a.Log("Setup logging!")
}

func (s *RemoveSuite) TeardownSuite(a Asserter) {
	//a.Log("Teardown logging!")
	//a.Error("Oops!")
}

func (s *RemoveSuite) Scenario_SingleFileRemoveBlobFSEncodedPath(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryHNSAcct)
	srcService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.BlobFS()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("0K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("%23%25%3F"),
		Body:       body,
	})

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: ResolveVariation(svm, []AzCopyVerb{AzCopyVerbRemove}),
			Targets: []ResourceManager{
				srcObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.BlobPermissions{Read: true, List: true, Delete: true}).String(),
					},
				}),
			},
			Flags: RemoveFlags{},
		})
	ValidateResource[ObjectResourceManager](svm, srcObj, ResourceDefinitionObject{
		ObjectShouldExist: to.Ptr(false),
	}, false)
}
