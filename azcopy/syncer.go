package azcopy

import (
	"context"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type syncer struct {
	opts *CookedSyncOptions
	srp  *remoteProvider
	spt  *syncProgressTracker
}

func newSyncer(ctx context.Context, jobID common.JobID, src, dst string, opts SyncOptions, handler SyncJobHandler, uotm *common.UserOAuthTokenManager) (s *syncer, err error) {
	cookedOpts, err := newCookedSyncOptions(src, dst, opts)
	if err != nil {
		return nil, err
	}

	syncRemote, err := NewSyncRemoteProvider(ctx, uotm, cookedOpts.source, cookedOpts.destination,
		cookedOpts.fromTo, cookedOpts.cpkOptions, cookedOpts.trailingDot)
	if err != nil {
		return nil, err
	}

	progressTracker := newSyncProgressTracker(jobID, handler)

	return &syncer{opts: cookedOpts, srp: syncRemote, spt: progressTracker}, nil
}
