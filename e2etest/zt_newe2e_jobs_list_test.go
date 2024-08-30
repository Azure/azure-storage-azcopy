package e2etest

import (
	"fmt"
)

func init() {
	suiteManager.RegisterSuite(&JobsListSuite{})
}

type JobsListSuite struct{}

func (s *JobsListSuite) Scenario_JobsListBasic(svm *ScenarioVariationManager) {
	//_, _ = RunAzCopy(
	//	svm,
	//	AzCopyCommand{
	//		Verb:           AzCopyVerbJobsList,
	//		PositionalArgs: []string{"list"},
	//	})

	jloutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:           AzCopyVerbJobsList,
			PositionalArgs: []string{"list"},
		})
	ValidateJobsListOutput(svm, jloutput, 0)
	fmt.Println("stdout Output: ", jloutput)
}
