package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strconv"
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

func (s *RemoveSuite) Scenario_EmptySASErrorCodes(svm *ScenarioVariationManager) {
	// Scale up from service to object
	// File - ShareNotFound error
	// BlobFS - errors out in log file, not stdout
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionObject{})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
			},
			ShouldFail: true,
		})

	// Validate that the stdout contains these error URLs
	ValidateErrorOutput(svm, stdout, "https://aka.ms/AzCopyError/NoAuthenticationInformation")
}

func (s *RemoveSuite) Scenario_RemoveVirtualDirectory(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_5_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 5 {
		name := "dir_5_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		obj.Body = nil
		obj.ObjectShouldExist = to.Ptr(false)
		srcObjs[name] = obj
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObject, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, srcContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}
