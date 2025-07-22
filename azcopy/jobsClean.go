// Copyright © 2025 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	Count int            // Number of files cleaned
	Jobs  []common.JobID // List of job IDs cleaned if WithStatus is not All, otherwise nil
}

// CleanJobs removes jobs with a specified status.
// If WithStatus is All, it cleans all jobs and returns the count of jobs cleaned.
// If WithStatus is not All, it cleans jobs with that status and returns the count of jobs cleaned and list of job IDs cleaned.
func (c Client) CleanJobs(opts CleanJobsOptions) (result CleanJobsResult, err error) {
	result = CleanJobsResult{}
	status := common.IffNil(opts.WithStatus, common.EJobStatus.All())

	if status == common.EJobStatus.All() {
		result.Count, err = jobsAdmin.BlindDeleteAllJobFiles(c.CurrentJobID)
	} else {
		resp := jobsAdmin.ListJobs(status)
		if resp.ErrorMessage != "" {
			return result, fmt.Errorf("failed to list jobs due to error: %s", resp.ErrorMessage)
		}
		result.Jobs = []common.JobID{}
		for _, job := range resp.JobIDDetails {
			result.Jobs = append(result.Jobs, job.JobId)
			count, err := jobsAdmin.RemoveSingleJobFiles(job.JobId)
			if err != nil {
				return result, fmt.Errorf("failed to remove job %s due to error: %w", job.JobId, err)
			} else {
				result.Count += count
			}
		}
	}
	return result, err
}
