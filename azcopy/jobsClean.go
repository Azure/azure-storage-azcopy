package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type CleanJobs struct {
	WithStatus *common.JobStatus // Default: All
}

type CleanJobsResult struct {
	Count int            // Number of jobs cleaned
	Jobs  []common.JobID // List of job IDs cleaned if WithStatus is not All, otherwise nil
}

// CleanJobs removes jobs with a specified status.
// If WithStatus is All, it cleans all jobs and returns the count of jobs cleaned.
// If WithStatus is not All, it cleans jobs with that status and returns the count of jobs cleaned and list of job IDs cleaned.
func (c Client) CleanJobs(opts CleanJobs) (result CleanJobsResult, err error) {
	result = CleanJobsResult{}
	status := common.IffNil(opts.WithStatus, common.EJobStatus.All())
	options := cmd.JobsCleanOptions{
		WithStatus: status,
		OnJobDeletion: func(jobID common.JobID) {
			if status != common.EJobStatus.All() {
				if result.Jobs == nil {
					result.Jobs = []common.JobID{}
				}
				result.Jobs = append(result.Jobs, jobID)
			}
		},
	}
	result.Count, err = cmd.RunJobsClean(options)
	return result, err
}
