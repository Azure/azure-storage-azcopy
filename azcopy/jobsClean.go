package azcopy

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

// TODO (gapra) : Consider adding an onDelete callback to CleanJobsOptions? That way we can print to console as we clean jobs.
type CleanJobsOptions struct {
	WithStatus *common.JobStatus // Default: All
}

type CleanJobsResult struct {
	Count int            // Number of jobs cleaned
	Jobs  []common.JobID // List of job IDs cleaned if WithStatus is not All, otherwise nil
}

// CleanJobs removes jobs with a specified status.
// If WithStatus is All, it cleans all jobs and returns the count of jobs cleaned.
// If WithStatus is not All, it cleans jobs with that status and returns the count of jobs cleaned and list of job IDs cleaned.
func (c Client) CleanJobs(opts CleanJobsOptions) (result CleanJobsResult, err error) {
	result = CleanJobsResult{}
	status := common.IffNil(opts.WithStatus, common.EJobStatus.All())

	if status == common.EJobStatus.All() {
		result.Count, err = jobsAdmin.BlindDeleteAllJobFiles(c.JobPlanFolder, c.LogPathFolder, c.CurrentJobID)
	} else {
		resp := jobsAdmin.ListJobs(status)
		if resp.ErrorMessage != "" {
			return result, fmt.Errorf("failed to list jobs due to error: %s", resp.ErrorMessage)
		}
		for _, job := range resp.JobIDDetails {
			if result.Jobs == nil {
				result.Jobs = []common.JobID{}
			}
			result.Jobs = append(result.Jobs, job.JobId)
			err = jobsAdmin.RemoveSingleJobFiles(c.JobPlanFolder, c.LogPathFolder, job.JobId)
			if err != nil {
				return result, fmt.Errorf("failed to remove job %s due to error: %w", job.JobId, err)
			} else {
				result.Count++
			}
		}
	}
	return result, err
}
