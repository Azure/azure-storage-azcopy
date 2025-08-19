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

// TODO: (gapra) We should refactor some of the common.XXXX types without the ErrorMsg field if possible.

type GetJobSummaryOptions struct {
}

type JobSummaryResponse common.ListJobSummaryResponse

func (c Client) GetJobSummary(jobID common.JobID, opts GetJobSummaryOptions) (result JobSummaryResponse, err error) {
	if jobID.IsEmpty() {
		return JobSummaryResponse{}, errors.New("get job statistics requires the JobID")
	}
	resp := jobsAdmin.GetJobSummary(jobID)

	if resp.ErrorMsg != "" {
		return JobSummaryResponse(resp), fmt.Errorf("failed to get job summary for job %s due to error: %s", jobID, resp.ErrorMsg)
	}

	return JobSummaryResponse(resp), nil
}

type ListJobTransfersOptions struct {
	WithStatus *common.TransferStatus
}

type ListJobTransfersResponse common.ListJobTransfersResponse

// ListJobTransfers lists the transfers for a job with the specified JobID and given transfer status.
func (c Client) ListJobTransfers(jobID common.JobID, opts ListJobTransfersOptions) (result ListJobTransfersResponse, err error) {
	if jobID.IsEmpty() {
		return result, errors.New("list job transfers requires the JobID")
	}
	status := common.IffNil(opts.WithStatus, common.ETransferStatus.All())

	resp := jobsAdmin.ListJobTransfers(common.ListJobTransfersRequest{JobID: jobID, OfStatus: status})

	if resp.ErrorMsg != "" {
		return ListJobTransfersResponse(resp), fmt.Errorf("failed to list transfers for job %s due to error: %s", jobID, resp.ErrorMsg)
	}
	return ListJobTransfersResponse(resp), nil
}
