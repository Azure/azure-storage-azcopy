package azcopy

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
)

type GetJobStatisticsOptions struct {
	JobID common.JobID
}

type JobStatistics struct {
	JobID                    common.JobID
	FileTransfers            uint32
	FolderTransfers          uint32
	SymlinkTransfers         uint32
	TotalTransfers           uint32
	FileTransfersCompleted   uint32 // TODO: (gapra) Should we rename these FilesCompleted/FoldersCompleted/FilesFailed. Consider what will happen when we add metadata transfers and such?
	FolderTransfersCompleted uint32
	FileTransfersFailed      uint32
	FolderTransfersFailed    uint32
	FileTransfersSkipped     uint32
	FolderTransfersSkipped   uint32
	TotalBytesTransferred    uint64
	PercentComplete          float32
	JobStatus                common.JobStatus
}

func GetJobStatistics(opts GetJobStatisticsOptions) (result JobStatistics, err error) {
	if opts.JobID.IsEmpty() {
		return JobStatistics{}, errors.New("JobsShow requires the JobID")
	}
	resp := jobsAdmin.GetJobSummary(opts.JobID)

	if resp.ErrorMsg != "" {
		return JobStatistics{}, fmt.Errorf("failed to get job statistics for job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}

	return JobStatistics{
		JobID:                    resp.JobID,
		FileTransfers:            resp.FileTransfers,
		FolderTransfers:          resp.FolderPropertyTransfers,
		SymlinkTransfers:         resp.SymlinkTransfers,
		TotalTransfers:           resp.TotalTransfers,
		FileTransfersCompleted:   resp.TransfersCompleted - resp.FoldersCompleted,
		FolderTransfersCompleted: resp.FoldersCompleted,
		FileTransfersFailed:      resp.TransfersFailed - resp.FoldersFailed,
		FolderTransfersFailed:    resp.FoldersFailed,
		FileTransfersSkipped:     resp.TransfersSkipped - resp.FoldersSkipped,
		FolderTransfersSkipped:   resp.FoldersSkipped,
		TotalBytesTransferred:    resp.TotalBytesTransferred,
		PercentComplete:          resp.PercentComplete,
		JobStatus:                resp.JobStatus,
	}, nil
}

type ListTransfersOptions struct {
	JobID      common.JobID
	WithStatus *common.TransferStatus
}

type TransferDetail struct {
	Src            string
	Dst            string
	TransferStatus common.TransferStatus
}

type ListTransfersResponse struct {
	JobID   common.JobID
	Details []TransferDetail
}

// ListTransfers lists the transfers for a job with the specified JobID and given transfer status.
func ListTransfers(opts ListTransfersOptions) (result ListTransfersResponse, err error) {
	if opts.JobID.IsEmpty() {
		return result, errors.New("ListTransfers requires the JobID")
	}
	status := common.IffNil(opts.WithStatus, common.ETransferStatus.All())

	resp := jobsAdmin.ListJobTransfers(common.ListJobTransfersRequest{JobID: opts.JobID, OfStatus: status})

	if resp.ErrorMsg != "" {
		return ListTransfersResponse{}, fmt.Errorf("failed to list transfers for job %s due to error: %s", opts.JobID, resp.ErrorMsg)
	}

	details := []TransferDetail{}
	for _, transfer := range resp.Details {
		folderChar := common.Iff(transfer.IsFolderProperties, "/", "")
		details = append(details, TransferDetail{
			Src:            transfer.Src + folderChar,
			Dst:            transfer.Dst + folderChar,
			TransferStatus: transfer.TransferStatus,
		})
	}
	return ListTransfersResponse{
		JobID:   resp.JobID,
		Details: details,
	}, nil
}
