package azcopy

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type RemoveJobOptions struct {
	JobID common.JobID
}

type RemoveJobResult struct {
	Count int // Number of files cleaned
}

// RemoveJob removes a job with the specified JobID.
func (c Client) RemoveJob(opts RemoveJobOptions) (result RemoveJobResult, err error) {
	result = RemoveJobResult{}
	if opts.JobID.IsEmpty() {
		return result, errors.New("remove job requires the JobID")
	}
	result.Count, err = jobsAdmin.RemoveSingleJobFiles(opts.JobID)
	if err != nil {
		return result, fmt.Errorf("failed to remove log and job plan files for job %s due to error: %w", opts.JobID, err)
	}
	return result, nil
}
