package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&FileOAuthTestSuite{})
}

type FileOAuthTestSuite struct{}

// Scenario_FileBlobOAuthNoError tests S2S FileBlob (default BlockBlob) copies using OAuth are successful
func (s *FileOAuthTestSuite) Scenario_FileBlobOAuthNoError(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	verb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbSync, AzCopyVerbCopy})
	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: verb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					FromTo:    pointerTo(common.EFromTo.FileBlob()),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{}, true)
	ValidateDoesNotContainError(svm, stdout,
		[]string{"S2S copy from Azure File authenticated with Azure AD to Blob/BlobFS is not supported",
			"S2S sync from Azure File authenticated with Azure AD to Blob/BlobFS is not supported"})
}

// Test FilePageBlob and FileAppendBlob copies
func (s *FileOAuthTestSuite) Scenario_AllBlobTypesOAuth(svm *ScenarioVariationManager) {
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionObject{})
	blobTypesSDK := ResolveVariation(svm, []blob.BlobType{blob.BlobTypeAppendBlob, blob.BlobTypePageBlob})
	dstObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()),
		ResourceDefinitionObject{
			ObjectProperties: ObjectProperties{
				BlobProperties: BlobProperties{
					Type: pointerTo(blobTypesSDK),
				},
			},
		})

	// Matches the SDK blob type to respective enum type
	var blobType common.BlobType
	switch blobTypesSDK {
	case blob.BlobTypeAppendBlob:
		blobType = common.EBlobType.AppendBlob()
	case blob.BlobTypePageBlob:
		blobType = common.EBlobType.PageBlob()
	default:
		blobType = common.EBlobType.BlockBlob()
	}

	RunAzCopy(svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{})},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					FromTo:    pointerTo(common.EFromTo.FileBlob()),
				},
				BlobType: pointerTo(blobType),
			},
		})
	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{}, true)
}
