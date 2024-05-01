package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&ListSuite{})
}

type ListSuite struct{}

// TODO : Test json for majority of cases and add a few for text output
func (s *ListSuite) Scenario_ListBasic(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("1K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				GlobalFlags: GlobalFlags{
					OutputType: to.Ptr(common.EOutputFormat.Json()),
				},
			},
		})

	expectedObjects := map[AzCopyOutputKey]cmd.AzCopyListObject{
		AzCopyOutputKey{Path: "test"}: {Path: "test", ContentLength: "1.00 KiB"},
	}
	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListRunningTally(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("1K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	}) // todo: generic CreateResource is something to pursue in another branch, but it's an interesting thought.

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				GlobalFlags: GlobalFlags{
					OutputType: to.Ptr(common.EOutputFormat.Json()),
				},
				RunningTally: to.Ptr(true),
			},
		})

	expectedObjects := map[AzCopyOutputKey]cmd.AzCopyListObject{
		AzCopyOutputKey{Path: "test"}: {Path: "test", ContentLength: "1.00 KiB"},
	}
	expectedSummary := &cmd.AzCopyListSummary{FileCount: "1", TotalFileSize: "1.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListVersionIdNoAdditionalVersions(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(svm, SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB", VersionId: versionId}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				GlobalFlags: GlobalFlags{
					OutputType: to.Ptr(common.EOutputFormat.Json()),
				},
				RunningTally: to.Ptr(true),
				Properties:   to.Ptr("VersionId"),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "3", TotalFileSize: "3.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListVersionIdWithVersions(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}
	for i, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(svm, SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB", VersionId: versionId}

		// Create a new version of the blob for the first two blobs
		if i < 2 {
			obj.Create(svm, NewRandomObjectContentContainer(svm, SizeFromString("2K")), ObjectProperties{})
			props = obj.GetProperties(svm)
			versionId = common.IffNotNil(props.BlobProperties.VersionId, "")
			expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "2.00 KiB", VersionId: versionId}
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				GlobalFlags: GlobalFlags{
					OutputType: to.Ptr(common.EOutputFormat.Json()),
				},
				RunningTally: to.Ptr(true),
				Properties:   to.Ptr("VersionId"),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "4", TotalFileSize: "6.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListWithVersions(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}
	for i, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(svm, SizeFromString("1K")),
		})

		// Create a new version of the blob for the first two blobs
		if i < 2 {
			obj.Create(svm, NewRandomObjectContentContainer(svm, SizeFromString("2K")), ObjectProperties{})
			expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "2.00 KiB"}
		} else {
			expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB"}
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "4", TotalFileSize: "6.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *FileOAuthTestSuite) Scenario_ListFileOAuth(svm *ScenarioVariationManager) {
	srcShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	fileNames := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}

	for _, fileName := range fileNames {
		_ = CreateResource[ObjectResourceManager](svm, srcShare, ResourceDefinitionObject{
			ObjectName: pointerTo(fileName),
			Body:       NewRandomObjectContentContainer(svm, SizeFromString("1K")),
		})

		expectedObjects[AzCopyOutputKey{Path: fileName}] = cmd.AzCopyListObject{Path: fileName, ContentLength: "1.00 KiB"}
	}

	// add the directories in the expected objects
	expectedObjects[AzCopyOutputKey{Path: "/"}] = cmd.AzCopyListObject{Path: "/", ContentLength: "0.00 B"}
	expectedObjects[AzCopyOutputKey{Path: "foo/"}] = cmd.AzCopyListObject{Path: "foo/", ContentLength: "0.00 B"}
	expectedObjects[AzCopyOutputKey{Path: "test/"}] = cmd.AzCopyListObject{Path: "test/", ContentLength: "0.00 B"}
	expectedObjects[AzCopyOutputKey{Path: "sub1/"}] = cmd.AzCopyListObject{Path: "sub1/", ContentLength: "0.00 B"}
	expectedObjects[AzCopyOutputKey{Path: "sub1/test/"}] = cmd.AzCopyListObject{Path: "sub1/test/", ContentLength: "0.00 B"}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcShare, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "9", TotalFileSize: "4.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}
