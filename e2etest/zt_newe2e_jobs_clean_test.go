package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	suiteManager.RegisterSuite(&JobsCleanSuite{})
}

type JobsCleanSuite struct{}

// Clean all files/jobs
func (s *JobsCleanSuite) Scenario_JobsCleanAll(svm *ScenarioVariationManager) {

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

	// TEMPORARY -- revert when this branch picks up cmd/jobsClean.go.
	// Upstream this is cmd.JobsCleanupSuccessMsg (origin/main:cmd/jobsClean.go:34).
	// cmd/jobsClean.go does not exist anywhere on the feature/azfilestofilesazcopy line,
	// so that reference fails to compile and takes the ENTIRE e2etest package with it --
	// meaning no e2e test of any kind can run. Inlined here so the rest of the suite is
	// usable. Note Scenario_JobsCleanAll itself still cannot pass on this branch, because
	// the jobs-clean command it drives doesn't exist here either; don't include it in -run.
	ValidateMessageOutput(svm, jobsCleanOutput, "Successfully removed all jobs.", true)
	validateDirSize(svm, logsDir, 1) // log directory will have a file matching the current job ID.
	validateDirSize(svm, jobPlanDir, 0)
}

func validateDirSize(a Asserter, dir string, filesExpected int) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	files, err := os.ReadDir(dir)
	a.AssertNow("failed to read directory", IsNil{}, err)
	a.AssertNow("directory is not empty", Equal{}, len(files), filesExpected)
}

func (s *JobsCleanSuite) Scenario_JobsCleanWithStatus(svm *ScenarioVariationManager) {

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

	// Completed Job
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
	failed, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync,
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

	jobsCleanOutput, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbJobsClean,
			Flags: JobsCleanFlags{
				WithStatus: pointerTo(common.EJobStatus.Completed()),
			},
			ShouldFail: false,
			Environment: &AzCopyEnvironment{
				LogLocation:     &logsDir,
				JobPlanLocation: &jobPlanDir,
			},
		})

	ValidateMessageOutput(svm, jobsCleanOutput, "Successfully removed jobs with status: Completed.", true)
	if !svm.Dryrun() {
		completedId := getJobID(svm, completed)
		failedId := getJobID(svm, failed)
		validateDoesNotExist(svm, logsDir, completedId)
		//validateDoesNotExist(svm, jobPlanDir, completedId)
		validateExists(svm, logsDir, failedId)
		//validateExists(svm, jobPlanDir, failedId) // TODO (gapra): Looks like the new e2e test framework deletes the job plan files after the job is completed, so this check fails.
	}
}

func getJobID(a Asserter, stdOut AzCopyStdout) string {
	parsedStdout := GetTypeOrAssert[*AzCopyParsedCopySyncRemoveStdout](a, stdOut)
	return parsedStdout.InitMsg.JobID
}

func validateDoesNotExist(a Asserter, dir string, jobId string) {
	files, err := os.ReadDir(dir)
	a.AssertNow("failed to read directory", IsNil{}, err)

	for _, file := range files {
		if strings.HasPrefix(file.Name(), jobId) {
			a.Error(fmt.Sprintf("file starting with job id '%s' found when it should have been deleted: %s", jobId, file.Name()))
		}
	}
}

func validateExists(a Asserter, dir string, jobId string) {
	files, err := os.ReadDir(dir)
	a.AssertNow("failed to read directory", IsNil{}, err)

	found := false
	for _, file := range files {
		if strings.HasPrefix(file.Name(), jobId) {
			found = true
			break
		}
	}

	if !found {
		a.Error(fmt.Sprintf("could not find a file starting with job id '%s' in directory '%s'", jobId, dir))
	}
}
