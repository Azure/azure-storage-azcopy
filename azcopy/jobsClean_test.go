package azcopy

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// setupTest creates temporary directories for job plans and logs using t.TempDir,
// and sets the package-level paths to them.
// It returns a cleanup function that restores global state and should be deferred by the caller.
func setupTest(t *testing.T) func() {
	// t.TempDir() automatically creates a temporary directory and schedules it for cleanup.
	tmpDir := t.TempDir()

	planDir := filepath.Join(tmpDir, "plans")
	logDir := filepath.Join(tmpDir, "logs")
	err := os.Mkdir(planDir, 0755)
	assert.NoError(t, err)
	err = os.Mkdir(logDir, 0755)
	assert.NoError(t, err)

	// Override the default paths
	originalPlanPath := common.AzcopyJobPlanFolder
	originalLogPath := common.LogPathFolder
	common.AzcopyJobPlanFolder = planDir
	common.LogPathFolder = logDir

	// Return a cleanup function to restore global variables.
	// The directory itself will be cleaned up by the test framework.
	return func() {
		common.AzcopyJobPlanFolder = originalPlanPath
		common.LogPathFolder = originalLogPath
	}
}

// createDummyFile creates an empty file at the specified path.
func createDummyFile(t *testing.T, path string) {
	f, err := os.Create(path)
	assert.NoError(t, err)
	f.Close()
}

// Helper to create a job plan file with a specific status
func createJobWithStatus(t *testing.T, status common.JobStatus) common.JobID {
	jobID := common.NewJobID()
	// Create a dummy job part order request
	order := common.CopyJobPartOrderRequest{
		JobID:         jobID,
		PartNum:       0,
		CommandString: jobID.String(), // This is just a placeholder; in a real scenario, it would be the command string for the job.
	}

	// Create the plan file name
	planFileName := ste.JobPartPlanFileName(fmt.Sprintf("%s--%05d.steV%d", jobID, 0, ste.DataSchemaVersion))

	// Create the plan file
	planFileName.Create(order)

	// Map the file to update the status
	mmf := planFileName.Map()
	plan := mmf.Plan()
	plan.SetJobStatus(status)
	mmf.Unmap()

	// Create a corresponding log file
	createDummyFile(t, filepath.Join(common.LogPathFolder, fmt.Sprintf("%s.log", jobID)))

	return jobID
}

func TestCleanJobs_All(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()

	// --- Setup ---
	createDummyFile(t, filepath.Join(common.AzcopyJobPlanFolder, "d46d16af-8835-e646-505e-7774b547f0a1.steV10"))
	createDummyFile(t, filepath.Join(common.LogPathFolder, "d46d16af-8835-e646-505e-7774b547f0a1.log"))
	createDummyFile(t, filepath.Join(common.AzcopyJobPlanFolder, "072b2cb5-1c5f-e84a-63e1-2b6cf2946142.steV10"))
	createDummyFile(t, filepath.Join(common.LogPathFolder, "072b2cb5-1c5f-e84a-63e1-2b6cf2946142.log"))

	// Create a log file for the current job that should NOT be deleted
	currentJobID := common.NewJobID()
	currentJobLog := filepath.Join(common.LogPathFolder, fmt.Sprintf("%s.log", currentJobID))
	createDummyFile(t, currentJobLog)

	client := Client{CurrentJobID: currentJobID}
	opts := CleanJobsOptions{} // Default is to clean all jobs

	// --- Action ---
	result, err := client.CleanJobs(opts)

	// --- Assertions ---
	assert.NoError(t, err)
	assert.Equal(t, 4, result.Count) // 2 plans + 2 logs
	assert.Nil(t, result.Jobs)

	// Verify files were deleted
	files, err := os.ReadDir(common.AzcopyJobPlanFolder)
	assert.NoError(t, err)
	assert.Len(t, files, 0)

	// Verify only the current job's log remains
	logFiles, err := os.ReadDir(common.LogPathFolder)
	assert.NoError(t, err)
	assert.Len(t, logFiles, 1)
	assert.Equal(t, filepath.Base(currentJobLog), logFiles[0].Name())
}

func TestCleanJobs_WithStatus(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	completedJobID := createJobWithStatus(t, common.EJobStatus.Completed())
	failedJobID := createJobWithStatus(t, common.EJobStatus.Failed())
	inProgressJobID := createJobWithStatus(t, common.EJobStatus.InProgress())

	client := Client{} // A dummy current job ID
	opts := CleanJobsOptions{
		WithStatus: to.Ptr(common.EJobStatus.Completed()),
	}

	// --- Action ---
	result, err := client.CleanJobs(opts)

	// --- Assertions ---
	a.NoError(err)
	a.Equal(2, result.Count) // 1 plan + 1 log
	a.NotNil(result.Jobs)
	a.Len(result.Jobs, 1)
	a.Equal(completedJobID, result.Jobs[0])

	// Verify completed job files were deleted
	completedPlanPath := filepath.Join(common.AzcopyJobPlanFolder, fmt.Sprintf("%s--00000.steV%d", completedJobID, ste.DataSchemaVersion))
	_, err = os.Stat(completedPlanPath)
	a.True(os.IsNotExist(err), "Completed job plan should be deleted")

	completedLogPath := filepath.Join(common.LogPathFolder, fmt.Sprintf("%s.log", completedJobID))
	_, err = os.Stat(completedLogPath)
	a.True(os.IsNotExist(err), "Completed job log should be deleted")

	// Verify other job files still exist
	planFiles, err := os.ReadDir(common.AzcopyJobPlanFolder)
	a.NoError(err)
	a.Len(planFiles, 2) // Failed and InProgress jobs should remain

	logFiles, err := os.ReadDir(common.LogPathFolder)
	a.NoError(err)
	a.Len(logFiles, 2) // Failed and InProgress logs should remain

	// Check that the correct files remain
	expectedPlans := map[string]bool{
		fmt.Sprintf("%s--00000.steV%d", failedJobID, ste.DataSchemaVersion):     true,
		fmt.Sprintf("%s--00000.steV%d", inProgressJobID, ste.DataSchemaVersion): true,
	}
	for _, f := range planFiles {
		a.True(expectedPlans[f.Name()])
	}
}
