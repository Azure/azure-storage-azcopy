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
}

// TODO: (gapra) Should we only return error or leave result for futureproofing?

// RemoveJob removes a job with the specified JobID.
func (c Client) RemoveJob(opts RemoveJobOptions) (result RemoveJobResult, err error) {
	result = RemoveJobResult{}
	if opts.JobID.IsEmpty() {
		return result, errors.New("RemoveJob requires the JobID")
	}
	err = jobsAdmin.RemoveSingleJobFiles(c.JobPlanFolder, c.LogPathFolder, opts.JobID)
	if err != nil {
		return result, fmt.Errorf("failed to remove log and job plan files for job %s due to error: %w", opts.JobID, err)
	}
	return result, nil
}
