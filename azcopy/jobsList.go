package azcopy

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
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

	return ListJobsResponse{
		Details: details,
	}, nil
}
