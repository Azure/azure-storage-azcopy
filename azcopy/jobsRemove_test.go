package azcopy

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveJob_EmptyJobID(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	client := Client{}
	opts := RemoveJobOptions{
		JobID: common.JobID{}, // Empty JobID
	}

	// --- Action ---
	result, err := client.RemoveJob(opts)

	// --- Assertions ---
	a.Error(err)
	a.Equal("remove job requires the JobID", err.Error())
	a.Equal(0, result.Count)
}

func TestRemoveJob_NonExistent(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	client := Client{}
	opts := RemoveJobOptions{
		JobID: common.NewJobID(), // A job that doesn't exist
	}

	// --- Action ---
	result, err := client.RemoveJob(opts)

	// --- Assertions ---
	a.Error(err)
	a.Contains(err.Error(), "cannot find any log or job plan file")
	a.Equal(0, result.Count)
}

func TestRemoveJob_Success(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	jobIDToRemove := createJobWithStatus(t, common.EJobStatus.Completed())
	otherJobID := createJobWithStatus(t, common.EJobStatus.Completed())

	client := Client{}
	opts := RemoveJobOptions{
		JobID: jobIDToRemove,
	}

	// --- Action ---
	result, err := client.RemoveJob(opts)

	// --- Assertions ---
	a.NoError(err)
	a.Equal(2, result.Count) // 1 plan + 1 log

	// Verify the correct job files were deleted
	removedPlanPath := filepath.Join(common.AzcopyJobPlanFolder, fmt.Sprintf("%s--00000.steV%d", jobIDToRemove, ste.DataSchemaVersion))
	_, err = os.Stat(removedPlanPath)
	a.True(os.IsNotExist(err), "Plan file for the removed job should be deleted")

	removedLogPath := filepath.Join(common.LogPathFolder, fmt.Sprintf("%s.log", jobIDToRemove))
	_, err = os.Stat(removedLogPath)
	a.True(os.IsNotExist(err), "Log file for the removed job should be deleted")

	// Verify the other job's files still exist
	otherPlanPath := filepath.Join(common.AzcopyJobPlanFolder, fmt.Sprintf("%s--00000.steV%d", otherJobID, ste.DataSchemaVersion))
	_, err = os.Stat(otherPlanPath)
	a.NoError(err, "Plan file for the other job should still exist")

	otherLogPath := filepath.Join(common.LogPathFolder, fmt.Sprintf("%s.log", otherJobID))
	_, err = os.Stat(otherLogPath)
	a.NoError(err, "Log file for the other job should still exist")
}
