package e2etest

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
)

func init() {
	suiteManager.RegisterSuite(&JobsCleanSuite{})
}

type JobsCleanSuite struct{}

func (s *JobsCleanSuite) Scenario_JobsCleanBasic(svm *ScenarioVariationManager) {

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

	_, err = os.Create(filepath.Join(logsDir + fmt.Sprintf("%s.log", "jobID1")))
	_, err = os.Create(filepath.Join(logsDir + fmt.Sprintf("%s.log", "jobID2")))

	jobsCleanOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:       AzCopyVerbJobsClean,
			ShouldFail: false,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateMessageOutput(svm, jobsCleanOutput, cmd.JobsCleanupSuccessMsg, true)
}
