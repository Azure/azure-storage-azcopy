package e2etest

import "github.com/Azure/azure-storage-azcopy/v10/cmd"

func init() {
	suiteManager.RegisterSuite(&JobsCleanSuite{})
}

type JobsCleanSuite struct{}

func (s *JobsCleanSuite) Scenario_JobsCleanBasic(svm *ScenarioVariationManager) {

	jobsCleanOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsCleanup,
			PositionalArgs: []string{"clean"},
			Flags:          ListFlags{},
			ShouldFail:     false,
		})

	ValidateMessageOutput(svm, jobsCleanOutput, cmd.JobsCleanupSuccessMsg)
}
