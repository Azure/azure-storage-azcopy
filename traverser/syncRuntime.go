//go:build smslidingwindow

package traverser

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type syncRun struct {
	job                                  SyncJob
	orchestratorOptions                  *SyncOrchestratorOptions
	semaphore                            *ThrottleSemaphore
	isGCPSource                          bool
	enableDebugLogs                      bool
	enableThrottleLogs                   bool
	crawlParallelism                     int32
	maxActiveFiles                       int64
	maxActivelyEnumeratingDirectories    int64
	targetSlotRatio                      float64
	activeDirectories                    atomic.Int64
	srcDirEnumerating                    atomic.Int64
	dstDirEnumerating                    atomic.Int64
	totalFilesInIndexer                  atomic.Int64
	totalDirectoriesProcessed            atomic.Uint64
	dstDirEnumerationSkippedBasedOnCTime atomic.Uint32
	enableThrottling                     bool
	enableFileBasedThrottling            bool
	enableMemoryBasedThrottling          bool
	enableGoroutineBasedThrottling       bool
	activeFilesLimit                     atomic.Int64
	enumeratingDirectoryLimit            atomic.Int64
	activeMergeJoinDirs                  atomic.Int64
	mergeJoinChanDryEvents               atomic.Int64
	mergeJoinChanFullEvents              atomic.Int64
}

func RunSyncOrchestrator(ctx context.Context, job SyncJob, enumerator *SyncEnumerator) error {
	if ctx == nil || enumerator == nil || enumerator.orchestratorOptions == nil {
		return errors.New("sync orchestration requires a context, enumerator, and options")
	}
	if enumerator.scheduleTransfer == nil || enumerator.finalize == nil || job.EncodeDestinationPath == nil {
		return errors.New("sync orchestration requires transfer, finalization, and destination-path callbacks")
	}
	if job.NewTraverser == nil {
		job.NewTraverser = InitResourceTraverser
	}
	settings := *enumerator.orchestratorOptions
	settings.fromTo = job.FromTo
	owned := *enumerator
	owned.orchestratorOptions = &settings
	run := &syncRun{
		job:                 job,
		orchestratorOptions: &settings,
		targetSlotRatio:     defaultTargetSlotRatio,
		enableThrottleLogs:  true,
		enableThrottling:    true,
	}
	return run.syncOrchestratorHandler(&run.job, &owned, ctx)
}

func (r *syncRun) reportSourceFailure() {
	if r.job.IncrementSourceFolderEnumerationFailed != nil {
		r.job.IncrementSourceFolderEnumerationFailed()
	}
}

func (r *syncRun) reportDestinationFailure() {
	if r.job.IncrementDestinationFolderEnumerationFailed != nil {
		r.job.IncrementDestinationFolderEnumerationFailed()
	}
}

func (r *syncRun) reportDestinationSkipped() {
	if r.job.IncrementDestinationFolderEnumerationSkipped != nil {
		r.job.IncrementDestinationFolderEnumerationSkipped()
	}
}

func (r *syncRun) RegisterGlobalCustomStatsCallback(id common.CustomStatsID, callback common.CustomStatsCallback) {
	if r.job.RegisterStats != nil {
		r.job.RegisterStats(id, callback)
	}
}

func (r *syncRun) UnregisterGlobalCustomStatsCallback(id common.CustomStatsID) {
	if r.job.UnregisterStats != nil {
		r.job.UnregisterStats(id)
	}
}

func (r *syncRun) ForceCollectGlobalCustomStats(id common.CustomStatsID) {
	if r.job.CollectStats != nil {
		r.job.CollectStats(id)
	}
}
