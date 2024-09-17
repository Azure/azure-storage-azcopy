package e2etest

import (
	"fmt"
)

func init() {
	suiteManager.RegisterSuite(&JobsListSuite{})
}

type JobsListSuite struct{}

func (s *JobsListSuite) Scenario_JobsListBasic(svm *ScenarioVariationManager) {

	jobsListOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsList,
			PositionalArgs: []string{"list"},
			Stdout:         &AzCopyParsedJobsListStdout{},
			Flags:          ListFlags{},
		})
	ValidateJobsListOutput(svm, jobsListOutput, 0)
	fmt.Println("stdout Output: ", jobsListOutput)
}
