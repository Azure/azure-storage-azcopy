package e2etest

import (
	"fmt"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&JobsListSuite{})
}

type JobsListSuite struct{}

func (s *JobsListSuite) Scenario_JobsListBasic(svm *ScenarioVariationManager) {
	srcService := GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("1K"))
	var expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject
	if srcService.Location() == common.ELocation.Blob() {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{}
	} else {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{
			AzCopyOutputKey{Path: "/"}: {Path: "/", ContentLength: "0.00 B"},
		}
	}
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})
	expectedObjects[AzCopyOutputKey{Path: "test"}] = cmd.AzCopyListObject{Path: "test", ContentLength: "1.00 KiB"}

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
			Flags: ListFlags{},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)

	jobsListOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsList,
			PositionalArgs: []string{"list"},
			Stdout:         &AzCopyParsedJobsListStdout{},
		})
	ValidateJobsListOutput(svm, jobsListOutput, 1)
	fmt.Println("stdout Output: ", jobsListOutput)
}
