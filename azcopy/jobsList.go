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

func (c *Client) ListJobs(opts ListJobsOptions) (result ListJobsResponse, err error) {
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
