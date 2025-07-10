package azcopy

import (
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
