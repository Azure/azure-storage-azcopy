package azcopy

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"sort"
	"time"
)

type ListJobsOptions struct {
	WithStatus *common.JobStatus // Default: All
}

type JobDetail struct {
	JobID     common.JobID
	StartTime time.Time
	Status    common.JobStatus
	Command   string
}

type ListJobsResponse struct {
	Details []JobDetail
}

func (c Client) ListJobs(opts ListJobsOptions) (result ListJobsResponse, err error) {
	status := common.IffNil(opts.WithStatus, common.EJobStatus.All())

	resp := jobsAdmin.ListJobs(status)
	if resp.ErrorMessage != "" {
		return ListJobsResponse{}, fmt.Errorf("failed to list jobs due to error: %s", resp.ErrorMessage)
	}

	var details []JobDetail
	for _, job := range resp.JobIDDetails {
		details = append(details, JobDetail{
			JobID:     job.JobId,
			StartTime: time.Unix(0, job.StartTime),
			Status:    job.JobStatus,
			Command:   job.CommandString,
		})
	}

	// before displaying the jobs, sort them accordingly so that they are displayed in a consistent way
	sortJobs(details)

	return ListJobsResponse{
		Details: details,
	}, nil
}

func sortJobs(jobsDetails []JobDetail) {
	// sort the jobs so that the latest one is shown first
	sort.Slice(jobsDetails, func(i, j int) bool {
		// this function essentially asks whether i should be placed before j
		// we say yes if the job i is more recent
		return jobsDetails[i].StartTime.After(jobsDetails[j].StartTime)
	})
}
