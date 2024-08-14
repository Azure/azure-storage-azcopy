package e2etest

import "fmt"

func init() {
	suiteManager.RegisterSuite(&JobsListSuite{})
}

type JobsListSuite struct{}

func (s *JobsListSuite) Scenario_BasicJobsList(svm *ScenarioVariationManager) {
	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbJobsList,
		})

	fmt.Println("STDOUT*********", stdout)
}
