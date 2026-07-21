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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

// TODO: (gapra) We should refactor some of the common.XXXX types without the ErrorMsg field if possible.

type GetJobSummaryOptions struct {
	JobID common.JobID
}

type JobSummaryResponse common.ListJobSummaryResponse

func (c Client) GetJobSummary(opts GetJobSummaryOptions) (result JobSummaryResponse, err error) {
	if opts.JobID.IsEmpty() {
		return JobSummaryResponse{}, errors.New("get job statistics requires the JobID")
	}
	resp := jobsAdmin.GetJobSummary(opts.JobID)

	if resp.ErrorMsg != "" {
		return JobSummaryResponse(resp), fmt.Errorf("failed to get job summary for job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}

	return JobSummaryResponse(resp), nil
}

type ListJobTransfersOptions struct {
	JobID      common.JobID
	WithStatus *common.TransferStatus
}

type ListJobTransfersResponse common.ListJobTransfersResponse

// ListJobTransfers lists the transfers for a job with the specified JobID and given transfer status.
func (c Client) ListJobTransfers(opts ListJobTransfersOptions) (result ListJobTransfersResponse, err error) {
	if opts.JobID.IsEmpty() {
		return result, errors.New("list job transfers requires the JobID")
	}
	status := common.IffNil(opts.WithStatus, common.ETransferStatus.All())

	resp := jobsAdmin.ListJobTransfers(common.ListJobTransfersRequest{JobID: opts.JobID, OfStatus: status})

	if resp.ErrorMsg != "" {
		return ListJobTransfersResponse(resp), fmt.Errorf("failed to list transfers for job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}
	return ListJobTransfersResponse(resp), nil
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(summary common.ListJobSummaryResponse, commonOutputFormat common.OutputFormat, lcm common.LifecycleMgr) {
	// Reset the bytes over the wire counter
	summary.BytesOverWire = 0

	if commonOutputFormat == common.EOutputFormat.Json() {
		lcm.Output(
			func(_ common.OutputFormat) string {
				buf, err := json.Marshal(summary)
				if err != nil {
					panic(err)
				}

				return string(buf)
			}, common.EOutputMessageType.GetJobSummary())
	}
	lcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary) // see note below re % complete being approximate. We can't include "approx" in the JSON.
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		return fmt.Sprintf(
			`
Job %s summary
Number of File Transfers: %v
Number of Folder Property Transfers: %v
Number of Symlink Transfers: %v
Total Number of Transfers: %v
Number of File Transfers Completed: %v
Number of Folder Transfers Completed: %v
Number of File Transfers Failed: %v
Number of Folder Transfers Failed: %v
Number of File Transfers Skipped: %v
Number of Folder Transfers Skipped: %v
Total Number of Bytes Transferred: %v
Percent Complete (approx): %.1f
Final Job Status: %v
`,
			summary.JobID.String(),
			summary.FileTransfers,
			summary.FolderPropertyTransfers,
			summary.SymlinkTransfers,
			summary.TotalTransfers,
			summary.TransfersCompleted-summary.FoldersCompleted,
			summary.FoldersCompleted,
			summary.TransfersFailed-summary.FoldersFailed,
			summary.FoldersFailed,
			summary.TransfersSkipped-summary.FoldersSkipped,
			summary.FoldersSkipped,
			summary.TotalBytesTransferred,
			summary.PercentComplete, // noted as approx in the format string because won't include in-flight files if this Show command is run from a different process
			summary.JobStatus,
		)
	}, common.EExitCode.Success())
}
