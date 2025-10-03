package azcopy

import (
	"context"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type transferExecutor struct {
	opts *CookedTransferOptions
	trp  *remoteProvider
	tpt  *transferProgressTracker
}

func newCopyTransferExecutor(ctx context.Context, jobID common.JobID, src, dst string, opts CopyOptions, handler CopyJobHandler, uotm *common.UserOAuthTokenManager) (t *transferExecutor, err error) {
	cookedOpts, err := newCookedCopyOptions(src, dst, opts)
	if err != nil {
		return nil, err
	}

	copyRemote, err := NewCopyRemoteProvider(ctx, uotm, cookedOpts.source, cookedOpts.destination,
		cookedOpts.fromTo, cookedOpts.cpkOptions, cookedOpts.trailingDot)
	if err != nil {
		return nil, err
	}

	progressTracker := newTransferProgressTracker(jobID, handler)

	return &transferExecutor{opts: cookedOpts, trp: copyRemote, tpt: progressTracker}, nil
}

// TODO: newSetPropertiesTransferExecutor

// TODO: newRemoveTransferExecutor
