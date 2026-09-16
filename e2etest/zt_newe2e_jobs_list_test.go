package e2etest

import (
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&JobsListSuite{})
}

type JobsListSuite struct{}

func (s *JobsListSuite) Scenario_JobsListNoJobs(svm *ScenarioVariationManager) {
	modes := []string{"enabled"}
	if AppInsightsTelemetryValidationEnabled() {
		modes = append(modes, "cli-optout", "env-optout", "missing-key", "missing-endpoint")
	}
	mode := ResolveVariation(svm, modes)
	flags := JobsListFlags{}
	environment := &AzCopyEnvironment{}
	expected := &telemetryExpectation{CommandOnly: true}
	switch mode {
	case "cli-optout":
		flags.DisableTelemetry = pointerTo(true)
	case "env-optout":
		environment.DisableTelemetry = pointerTo(true)
	case "missing-key":
		for _, part := range strings.Split(GlobalConfig.AppInsightsValidationConfig.ConnectionString, ";") {
			if strings.HasPrefix(strings.ToLower(part), "ingestionendpoint=") {
				environment.TelemetryConnectionString = &part
			}
		}
		svm.AssertNow("configured ingestion endpoint", Not{IsNil{}}, environment.TelemetryConnectionString)
	case "missing-endpoint":
		environment.TelemetryConnectionString = pointerTo("InstrumentationKey=11111111-2222-3333-4444-555555555555")
	}
	if mode != "enabled" {
		expected = &telemetryExpectation{NoEvents: true}
	}
	jobsListOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:        AzCopyVerbJobsList,
			Stdout:      &AzCopyParsedJobsListStdout{},
			Flags:       flags,
			Environment: environment,
			Telemetry:   expected,
		})
	ValidateJobsListOutput(svm, jobsListOutput, 0, []string{})
}

func (s *JobsListSuite) Scenario_JobsListAll(svm *ScenarioVariationManager) {

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

	// Job 1
	body := NewZeroObjectContentContainer(0)
	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, "test", common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, "test", common.EEntityType.File())
	sasOpts := GenericAccountSignatureValues{}
	job1, _ := RunAzCopy(
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

	// Job 2
	job2, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync,
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
	jobsListOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:      AzCopyVerbJobsList,
			Telemetry: &telemetryExpectation{CommandOnly: true},
			Stdout:    &AzCopyParsedJobsListStdout{},
			Flags:     JobsListFlags{},
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})
	if !svm.Dryrun() {
		j1 := getJobID(svm, job1)
		j2 := getJobID(svm, job2)
		ValidateJobsListOutput(svm, jobsListOutput, 2, []string{j1, j2})
	}
}

func (s *JobsListSuite) Scenario_JobsListWithStatus(svm *ScenarioVariationManager) {

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

	// Job 1
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

	// Failed Job
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
				AzCopyTarget{dstObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
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
			ShouldFail: true,
		})
	jobsListOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:   AzCopyVerbJobsList,
			Stdout: &AzCopyParsedJobsListStdout{},
			Flags: JobsListFlags{
				WithStatus: to.Ptr(common.EJobStatus.Completed()),
			},
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})
	if !svm.Dryrun() {
		completedId := getJobID(svm, completed)
		ValidateJobsListOutput(svm, jobsListOutput, 1, []string{completedId})
	}
}
