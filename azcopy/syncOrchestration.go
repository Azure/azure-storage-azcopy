package azcopy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// PreparedSync supports existing embedded callers that separately prepare and run enumeration.
// Both this adapter and Client.Sync use the same library executor.
type PreparedSync struct {
	syncer     *syncer
	enumerator *traverser.SyncEnumerator
	ctx        context.Context
	cancel     context.CancelFunc
	lifecycle  *jobLifecycleManager
}

func (c *Client) PrepareSync(ctx context.Context, src, dst string, opts SyncOptions) (*PreparedSync, error) {
	if ctx == nil {
		return nil, fmt.Errorf("a context is required for sync")
	}
	if src == "" || dst == "" {
		return nil, fmt.Errorf("source and destination must be specified for sync")
	}
	ctx, cancel := context.WithCancel(ctx)
	jobID := opts.JobID
	if jobID.IsEmpty() {
		jobID = common.NewJobID()
	}
	manager := opts.CredentialManager
	if manager == nil {
		manager = c.GetCredentialManager()
	}
	s, err := newSyncer(ctx, jobID, src, dst, opts, manager)
	if err != nil {
		cancel()
		return nil, err
	}
	s.logger = common.AzcopyScanningLogger
	mgr := NewJobLifecycleManager(common.GetLifecycleMgr())
	enumerator, err := s.initEnumerator(ctx, c.GetLogLevel(), mgr)
	if err != nil {
		cancel()
		return nil, err
	}
	return &PreparedSync{syncer: s, enumerator: enumerator, ctx: ctx, cancel: cancel, lifecycle: mgr}, nil
}

func (p *PreparedSync) Enumerator() *traverser.SyncEnumerator { return p.enumerator }

func (p *PreparedSync) Enumerate(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("a context is required for sync enumeration")
	}
	stop := context.AfterFunc(ctx, p.cancel)
	defer stop()
	err := p.syncer.enumerate(p.ctx, p.enumerator)
	if err == nil {
		err = p.ctx.Err()
	}
	if err != nil {
		return errors.Join(err, p.syncer.cancelAndDrain(p.cancel, p.syncer.spt.jobID, p.lifecycle))
	}
	return nil
}

func (p *PreparedSync) Cancel() { p.cancel() }

func (p *PreparedSync) ScanProgress() SyncScanProgress {
	return SyncScanProgress{
		MoverSyncStats:          p.syncer.spt.Stats(),
		SourceFilesScanned:      p.syncer.spt.getSourceFilesScanned(),
		DestinationFilesScanned: p.syncer.spt.getDestinationFilesScanned(),
		JobID:                   p.syncer.spt.jobID,
	}
}

type PreparedSyncState struct {
	SyncEnumerationStats
	JobID                   common.JobID
	FirstPartOrdered        bool
	ScanningComplete        bool
	DeletionCount           uint32
	SkippedSymlinkCount     uint32
	SkippedSpecialFileCount uint32
	SkippedHardlinkCount    uint32
}

func (p *PreparedSync) State() PreparedSyncState {
	tracker := p.syncer.spt
	return PreparedSyncState{
		SyncEnumerationStats:    tracker.GetEnumerationStats(),
		JobID:                   tracker.jobID,
		FirstPartOrdered:        tracker.firstPartOrdered(),
		ScanningComplete:        tracker.CompletedEnumeration(),
		DeletionCount:           tracker.getDeletionCount(),
		SkippedSymlinkCount:     tracker.getSkippedSymlinkCount(),
		SkippedSpecialFileCount: tracker.getSkippedSpecialFileCount(),
		SkippedHardlinkCount:    tracker.getSkippedHardlinkCount(),
	}
}

func (s *syncer) cancelAndDrain(cancel context.CancelFunc, jobID common.JobID, lifecycle *jobLifecycleManager) error {
	cancel()
	cleanupCtx, stopCleanup := context.WithTimeout(context.Background(), time.Minute)
	defer stopCleanup()
	if !s.opts.dryrun {
		if err := jobsAdmin.RequestJobCancellation(jobID); err != nil {
			return err
		}
	}
	if s.processor != nil {
		if err := s.processor.AbortAndWait(cleanupCtx); err != nil {
			return err
		}
	}
	if s.opts.dryrun {
		return nil
	}
	return lifecycle.CancelAndDrain(cleanupCtx, jobID)
}

func (s *syncer) enumerate(ctx context.Context, enumerator *traverser.SyncEnumerator) error {
	if !s.opts.useSyncOrchestrator {
		return enumerator.Enumerate()
	}
	logger, lifecycle := s.logger, common.GetLifecycleMgr()
	monitor := common.GlobalSystemStatsMonitor
	statsID := func(id common.CustomStatsID) common.CustomStatsID {
		return common.CustomStatsID(fmt.Sprintf("%s:%s", id, s.spt.jobID))
	}
	job := traverser.SyncJob{
		Source: s.opts.source, Destination: s.opts.destination, FromTo: s.opts.fromTo,
		JobID: s.spt.jobID, Recursive: s.opts.recursive, DeleteDestination: s.opts.deleteDestination,
		PreserveInfo: s.opts.preserveInfo, UseStreamingMergeJoin: s.opts.useStreamingMergeJoin,
		EncodeDestinationPath: func(path string) string { return PathEncodeRules(path, s.opts.fromTo, false, false) },
		Log: func(level common.LogLevel, message string, console bool) {
			if logger != nil {
				logger.Log(common.LogError, message)
			}
			if logger == nil || console {
				if level == common.LogError || level == common.LogPanic || level == common.LogWarning {
					lifecycle.Warn("[AzCopy] " + message)
				} else {
					lifecycle.Info("[AzCopy] " + message)
				}
			}
		},
		IncrementSourceFolderEnumerationFailed:       func() { s.spt.incSourceEnumerationFailure(common.EEntityType.Folder()) },
		IncrementDestinationFolderEnumerationFailed:  s.spt.IncrementDestinationFolderEnumerationFailed,
		IncrementDestinationFolderEnumerationSkipped: s.spt.IncrementDestinationFolderEnumerationSkipped,
	}
	if monitor != nil {
		job.RegisterStats = func(id common.CustomStatsID, callback common.CustomStatsCallback) {
			monitor.RegisterCustomStatsCallback(statsID(id), callback)
		}
		job.UnregisterStats = func(id common.CustomStatsID) { monitor.UnregisterCustomStatsCallback(statsID(id)) }
		job.CollectStats = func(id common.CustomStatsID) { monitor.ForceCollectCustomStats(statsID(id)) }
		job.LogStats = monitor.LogAdhocCustomStats
	}
	return traverser.RunSyncOrchestrator(ctx, job, enumerator)
}
