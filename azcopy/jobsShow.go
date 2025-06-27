package azcopy

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

// TODO: (gapra) We should refactor some of the common.XXXX types without the ErrorMsg field if possible.

type GetJobStatisticsOptions struct {
	JobID common.JobID
}

type JobStatistics common.ListJobSummaryResponse

func (c Client) GetJobStatistics(opts GetJobStatisticsOptions) (result JobStatistics, err error) {
	if opts.JobID.IsEmpty() {
		return JobStatistics{}, errors.New("JobsShow requires the JobID")
	}
	resp := jobsAdmin.GetJobSummary(opts.JobID)

	if resp.ErrorMsg != "" {
		return JobStatistics{}, fmt.Errorf("failed to get job summary for job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}

	return JobStatistics(resp), nil
}

type ListTransfersOptions struct {
	JobID      common.JobID
	WithStatus *common.TransferStatus
}

type ListTransfersResponse common.ListJobTransfersResponse

// ListTransfers lists the transfers for a job with the specified JobID and given transfer status.
func (c Client) ListTransfers(opts ListTransfersOptions) (result ListTransfersResponse, err error) {
	if opts.JobID.IsEmpty() {
		return result, errors.New("ListTransfers requires the JobID")
	}
	status := common.IffNil(opts.WithStatus, common.ETransferStatus.All())

	resp := jobsAdmin.ListJobTransfers(common.ListJobTransfersRequest{JobID: opts.JobID, OfStatus: status})

	if resp.ErrorMsg != "" {
		return ListTransfersResponse{}, fmt.Errorf("failed to list transfers for job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}
	return ListTransfersResponse(resp), nil
}
