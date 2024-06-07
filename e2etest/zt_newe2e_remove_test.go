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

func (s *RemoveSuite) Scenario_RemoveVirtualDirectory(svm *ScenarioVariationManager) {
	cont := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dirName := "dir_10_files_rm_oauth"
	objects := make([]string, 10)
	for i := range 10 {
		objects[i] = dirName + "/" + strconv.Itoa(i) + ".bin"
		CreateResource[ObjectResourceManager](svm, cont, ResourceDefinitionObject{
			ObjectName: pointerTo(objects[i]),
			Body:       NewRandomObjectContentContainer(svm, SizeFromString("1K")),
		})
	}
	virtualDir := cont.GetObject(svm, dirName, common.EEntityType.Folder())
	auth := ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()})
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(virtualDir, auth, svm, CreateAzCopyTargetOptions{}),
			},
			Flags: RemoveFlags{
				Recursive: to.Ptr(true),
			},
		})
	for _, objName := range objects {
		obj := cont.GetObject(svm, objName, common.EEntityType.File())
		ValidateResource[ObjectResourceManager](svm, obj, ResourceDefinitionObject{
			ObjectShouldExist: to.Ptr(false),
		}, false)
	}
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
			Verb: AzCopyVerbRemove,
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
