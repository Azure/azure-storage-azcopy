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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type CancelJobOptions struct {
	JobID common.JobID

	IgnoreErrorIfCompleted bool
}

// CancelJob cancels a job with the specified JobID.
func (c Client) CancelJob(opts CancelJobOptions) (*JobSummaryResponse, error) {
	if opts.JobID.IsEmpty() {
		return nil, errors.New("cancel job requires the JobID")
	}
	resp := jobsAdmin.CancelPauseJobOrder(opts.JobID, common.EJobStatus.Cancelling())
	if !resp.CancelledPauseResumed {
		if opts.IgnoreErrorIfCompleted && resp.JobStatus == common.EJobStatus.Completed() {
			summary, err := c.GetJobSummary(GetJobSummaryOptions{JobID: opts.JobID})
			if err != nil {
				return nil, err
			} else {
				return &summary, errors.New(resp.ErrorMsg)
			}
		}
		return nil, fmt.Errorf("failed to cancel job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}

	return nil, nil
}
