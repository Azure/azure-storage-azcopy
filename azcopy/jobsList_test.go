package azcopy

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestListJobs_All(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	// Create jobs in a specific order with slight delays to ensure distinct start times
	jobIDCompleted := createJobWithStatus(t, common.EJobStatus.Completed())
	time.Sleep(10 * time.Millisecond)
	jobIDFailed := createJobWithStatus(t, common.EJobStatus.Failed())
	time.Sleep(10 * time.Millisecond)
	jobIDInProgress := createJobWithStatus(t, common.EJobStatus.InProgress())

	client := Client{}
	opts := ListJobsOptions{} // Default is to list all jobs

	// --- Action ---
	result, err := client.ListJobs(opts)

	// --- Assertions ---
	a.NoError(err)
	a.Len(result.Details, 3)

	// Verify jobs are sorted by most recent first
	a.Equal(jobIDInProgress, result.Details[0].JobID)
	a.Equal(jobIDInProgress.String(), result.Details[0].Command)
	a.Equal(common.EJobStatus.InProgress(), result.Details[0].Status)

	a.Equal(jobIDFailed, result.Details[1].JobID)
	a.Equal(jobIDFailed.String(), result.Details[1].Command)
	a.Equal(common.EJobStatus.Failed(), result.Details[1].Status)

	a.Equal(jobIDCompleted, result.Details[2].JobID)
	a.Equal(jobIDCompleted.String(), result.Details[2].Command)
	a.Equal(common.EJobStatus.Completed(), result.Details[2].Status)
}

func TestListJobs_WithStatus(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	// Create jobs in a specific order with slight delays to ensure distinct start times
	jobIDCompleted := createJobWithStatus(t, common.EJobStatus.Completed())
	time.Sleep(10 * time.Millisecond)
	_ = createJobWithStatus(t, common.EJobStatus.Failed())
	time.Sleep(10 * time.Millisecond)
	_ = createJobWithStatus(t, common.EJobStatus.InProgress())

	client := Client{}
	opts := ListJobsOptions{
		WithStatus: to.Ptr(common.EJobStatus.Completed()),
	}

	// --- Action ---
	result, err := client.ListJobs(opts)

	// --- Assertions ---
	a.NoError(err)
	a.Len(result.Details, 1)

	a.Equal(jobIDCompleted, result.Details[0].JobID)
	a.Equal(jobIDCompleted.String(), result.Details[0].Command)
	a.Equal(common.EJobStatus.Completed(), result.Details[0].Status)
}

func TestListJobs_NoJobs(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()
	a := assert.New(t)

	// --- Setup ---
	client := Client{}
	opts := ListJobsOptions{}

	// --- Action ---
	result, err := client.ListJobs(opts)

	// --- Assertions ---
	a.NoError(err)
	a.Len(result.Details, 0)
}

func TestSortJobs(t *testing.T) {
	a := assert.New(t)
	// setup
	job2 := JobDetail{
		JobID:     common.NewJobID(),
		StartTime: time.Now(),
		Command:   "dummy2",
	}

	// sleep for a bit so that the time stamp is different
	time.Sleep(time.Millisecond)
	job1 := JobDetail{
		JobID:     common.NewJobID(),
		StartTime: time.Now(),
		Command:   "dummy1",
	}

	// sleep for a bit so that the time stamp is different
	time.Sleep(time.Millisecond)
	job0 := JobDetail{
		JobID:     common.NewJobID(),
		StartTime: time.Now(),
		Command:   "dummy0",
	}
	jobsList := []JobDetail{job2, job1, job0}

	// act
	sortJobs(jobsList)

	// verify
	a.Equal(job0, jobsList[0])
	a.Equal(job1, jobsList[1])
	a.Equal(job2, jobsList[2])
}
