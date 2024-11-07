package e2etest

func init() {
	suiteManager.RegisterSuite(&JobsListSuite{})
}

type JobsListSuite struct{}

func (s *JobsListSuite) Scenario_JobsListBasic(svm *ScenarioVariationManager) {

	jobsListOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobs,
			PositionalArgs: []string{"list"},
			Stdout:         &AzcopyJobListOutput{},
			ShouldFail:     false,
		})
	ValidateJobsListOutput(svm, jobsListOutput, 0)
}
