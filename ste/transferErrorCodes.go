package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// failTransferBeforeStart ends a transfer that failed before any chunk was
// scheduled. It records errMsg together with the HTTP status and error
// codes of err, which may be nil, the same way failActiveTransfer does for
// failures after that point.
func failTransferBeforeStart(jptm IJobPartTransferMgr, info *TransferInfo, errMsg string, err error) {
	codes := common.TransferFailureCodesFor(err, info.Source, info.Destination)
	jptm.LogSendError(info.Source, info.Destination, errMsg, int(codes.StatusCode))
	jptm.SetErrorCode(codes.StatusCode)
	jptm.SetTransferErrorCodes(codes)
	jptm.SetErrorMessage(errMsg)
	jptm.SetStatus(common.ETransferStatus.Failed())
	jptm.ReportTransferDone()
}
