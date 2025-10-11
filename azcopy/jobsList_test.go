package azcopy

import (
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

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
