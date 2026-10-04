package e2etest

import (
	"os"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&JobsRemoveSuite{})
}

type JobsRemoveSuite struct{}

func (s *JobsRemoveSuite) Scenario_JobsRemoveNoJob(svm *ScenarioVariationManager) {
	logsDir, err := os.MkdirTemp("", "testLogs")
	if err != nil {
		svm.t.Fatal("failed to create logs dir:", err)
	}
	defer os.RemoveAll(logsDir)

	jobPlanDir, err := os.MkdirTemp("", "testPlans")
	if err != nil {
		svm.t.Fatal("failed to create plan file dir:", err)
	}
	defer os.RemoveAll(jobPlanDir)

	jobsRemoveOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:       AzCopyVerbJobsRemove,
			ShouldFail: true,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateContainsError(svm, jobsRemoveOutput, []string{"remove job command requires the JobID"})
}

func (s *JobsRemoveSuite) Scenario_JobsRemoveInvalidJob(svm *ScenarioVariationManager) {
	logsDir, err := os.MkdirTemp("", "testLogs")
	if err != nil {
		svm.t.Fatal("failed to create logs dir:", err)
	}
	defer os.RemoveAll(logsDir)

	jobPlanDir, err := os.MkdirTemp("", "testPlans")
	if err != nil {
		svm.t.Fatal("failed to create plan file dir:", err)
	}
	defer os.RemoveAll(jobPlanDir)

	jobsRemoveOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsRemove,
			PositionalArgs: []string{"invalid-job-id"},
			ShouldFail:     true,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateContainsError(svm, jobsRemoveOutput, []string{"invalid jobId given invalid-job-id"})
}

func (s *JobsRemoveSuite) Scenario_JobsRemoveNonExistentJob(svm *ScenarioVariationManager) {
	logsDir, err := os.MkdirTemp("", "testLogs")
	if err != nil {
		svm.t.Fatal("failed to create logs dir:", err)
	}
	defer os.RemoveAll(logsDir)

	jobPlanDir, err := os.MkdirTemp("", "testPlans")
	if err != nil {
		svm.t.Fatal("failed to create plan file dir:", err)
	}
	defer os.RemoveAll(jobPlanDir)

	jobsRemoveOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsRemove,
			PositionalArgs: []string{"4a2542df-90ce-1043-69f0-e7481379b5b1"}, // some random job ID
			ShouldFail:     true,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateContainsError(svm, jobsRemoveOutput, []string{"failed to remove log and job plan files for job 4a2542df-90ce-1043-69f0-e7481379b5b1 due to error: cannot find any log or job plan file with the specified ID"})
}

func (s *JobsRemoveSuite) Scenario_JobsRemove(svm *ScenarioVariationManager) {
	logsDir, err := os.MkdirTemp("", "testLogs")
	if err != nil {
		svm.t.Fatal("failed to create logs dir:", err)
	}
	defer os.RemoveAll(logsDir)

	jobPlanDir, err := os.MkdirTemp("", "testPlans")
	if err != nil {
		svm.t.Fatal("failed to create plan file dir:", err)
	}
	defer os.RemoveAll(jobPlanDir)

	body := NewZeroObjectContentContainer(0)
	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, "test", common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, "test", common.EEntityType.File())
	sasOpts := GenericAccountSignatureValues{}
	completed, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
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
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	if !svm.Dryrun() {
		completedId := getJobID(svm, completed)

		jobsRemoveOutput, _ := RunAzCopy(
			svm,
			AzCopyCommand{
				Verb:           AzCopyVerbJobsRemove,
				PositionalArgs: []string{completedId},
				ShouldFail:     false,
				Environment: &AzCopyEnvironment{
					LogLocation:     &logsDir,
					JobPlanLocation: &jobPlanDir,
				},
			})

		ValidateMessageOutput(svm, jobsRemoveOutput, "Successfully removed log and job plan files for job "+completedId, true)
	}
}
