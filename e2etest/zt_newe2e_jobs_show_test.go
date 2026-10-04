package e2etest

import (
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&JobsShowSuite{})
}

type JobsShowSuite struct{}

func (s *JobsShowSuite) Scenario_JobsShowNoJob(svm *ScenarioVariationManager) {
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

	jobsShowOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:       AzCopyVerbJobsShow,
			ShouldFail: true,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateContainsError(svm, jobsShowOutput, []string{"show job command requires the JobID"})
}

func (s *JobsShowSuite) Scenario_JobsShowInvalidJob(svm *ScenarioVariationManager) {
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

	jobsShowOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsShow,
			PositionalArgs: []string{"invalid-job-id"},
			ShouldFail:     true,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateContainsError(svm, jobsShowOutput, []string{"invalid jobId given invalid-job-id"})
}

func (s *JobsShowSuite) Scenario_JobsShowNonExistentJob(svm *ScenarioVariationManager) {
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

	jobsShowOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsShow,
			PositionalArgs: []string{"4a2542df-90ce-1043-69f0-e7481379b5b1"}, // some random job ID
			ShouldFail:     true,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateContainsError(svm, jobsShowOutput, []string{"no job with JobId 4a2542df-90ce-1043-69f0-e7481379b5b1 exists"})
}

func (s *JobsShowSuite) Scenario_JobsShow(svm *ScenarioVariationManager) {
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
	completed, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				srcObj, dstObj,
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

		jobsShowOutput, _ := RunAzCopy(
			svm,
			AzCopyCommand{
				Verb:           AzCopyVerbJobsShow,
				PositionalArgs: []string{completedId},
				Environment: &AzCopyEnvironment{
					LogLocation:     &logsDir,
					JobPlanLocation: &jobPlanDir,
				},
			})

		out, ok := jobsShowOutput.(*AzCopyParsedJobsShowStdout)
		svm.AssertNow("stdout must be AzCopyParsedJobsShowStdout", Equal{}, ok, true)
		svm.Assert("job ID must match", Equal{}, out.summary.JobID.String(), completedId)
		svm.Assert("job status must be completed", Equal{}, out.summary.JobStatus, common.EJobStatus.Completed())
		svm.Assert("job must have 1 transfer", Equal{}, int(out.summary.TotalTransfers), 1)
	}
}

func (s *JobsShowSuite) Scenario_JobsShowWithStatus(svm *ScenarioVariationManager) {
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

		jobsShowOutput, _ := RunAzCopy(
			svm,
			AzCopyCommand{
				Verb:           AzCopyVerbJobsShow,
				PositionalArgs: []string{completedId},
				Flags: JobsShowFlags{
					WithStatus: to.Ptr(common.ETransferStatus.Success()),
				},
				Environment: &AzCopyEnvironment{
					LogLocation:     &logsDir,
					JobPlanLocation: &jobPlanDir,
				},
			})

		out, ok := jobsShowOutput.(*AzCopyParsedJobsShowStdout)
		svm.AssertNow("stdout must be AzCopyParsedJobsShowStdout", Equal{}, ok, true)
		svm.Assert("job ID must match", Equal{}, out.transfers.JobID.String(), completedId)
		svm.Assert("job must have 1 transfer", Equal{}, len(out.transfers.Details), 1)
		svm.Assert("transfer src must be test", Equal{}, strings.Contains(out.transfers.Details[0].Src, "test"), true)
		svm.Assert("transfer dst must be test", Equal{}, strings.Contains(out.transfers.Details[0].Dst, "test"), true)
	}
}
