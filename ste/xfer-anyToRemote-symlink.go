package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func anyToRemote_symlink(jptm IJobPartTransferMgr, info *TransferInfo, senderFactory senderFactory, sipf sourceInfoProviderFactory) {
	// Check if cancelled
	if jptm.WasCanceled() {
		/* This is earliest we detect that jptm has been cancelled before we reach destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	// Create SIP
	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if srcInfoProvider.EntityType() != common.EEntityType.Symlink() {
		panic("configuration error. Source Info Provider does not have symlink entity type")
	}
	symSIP, ok := srcInfoProvider.(ISymlinkBearingSourceInfoProvider)
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "source info provider implementation does not support symlinks", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	path, err := symSIP.ReadLink()
	if err != nil {
		jptm.FailActiveSend("getting symlink path", err)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	baseSender, err := senderFactory(jptm, info.Destination, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	s, ok := baseSender.(symlinkSender) // todo: symlinkSender
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "sender implementation does not support symlinks", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// write the symlink
	err = s.SendSymlink(path)
	if err != nil {
		jptm.FailActiveSend("creating destination symlink representative", err)
	}

	commonSenderCompletion(jptm, baseSender, info)
}
