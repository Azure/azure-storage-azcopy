package cmd

import (
	"context"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type CustomSyncHandlerFunc func(*cookedSyncCmdArgs, *syncEnumerator, context.Context) error

var UseSyncOrchestrator = traverser.UseSyncOrchestrator
var CustomSyncHandler CustomSyncHandlerFunc = func() CustomSyncHandlerFunc {
	if !traverser.UseSyncOrchestrator {
		return nil
	}
	return syncOrchestratorHandler
}()

func GetCustomSyncHandlerInfo() string {
	return traverser.GetCustomSyncHandlerInfo()
}

func syncOrchestratorHandler(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error {
	logger, lifecycle := common.AzcopyScanningLogger, glcm
	monitor := common.GlobalSystemStatsMonitor
	statsID := func(id common.CustomStatsID) common.CustomStatsID {
		return common.CustomStatsID(fmt.Sprintf("%s:%s", id, cca.jobID))
	}
	job := traverser.SyncJob{
		Source:                                 cca.source,
		Destination:                            cca.destination,
		FromTo:                                 cca.fromTo,
		JobID:                                  cca.jobID,
		Recursive:                              cca.recursive,
		DeleteDestination:                      cca.deleteDestination,
		PreserveInfo:                           cca.preserveInfo,
		UseStreamingMergeJoin:                  cca.useStreamingMergeJoin,
		SetCancel:                              func(cancel context.CancelFunc) { cca.orchestratorCancel = cancel },
		EncodeDestinationPath:                  func(path string) string { return pathEncodeRules(path, cca.fromTo, false, false) },
		IncrementSourceFolderEnumerationFailed: cca.IncrementSourceFolderEnumerationFailed,
		IncrementDestinationFolderEnumerationFailed:  cca.IncrementDestinationFolderEnumerationFailed,
		IncrementDestinationFolderEnumerationSkipped: cca.IncrementDestinationFolderEnumerationSkipped,
		Log: func(level common.LogLevel, msg string, console bool) {
			if logger != nil {
				logger.Log(common.LogError, msg)
			}
			if logger == nil || console {
				if level == common.LogError || level == common.LogPanic || level == common.LogWarning {
					lifecycle.Warn("[AzCopy] " + msg)
				} else {
					lifecycle.Info("[AzCopy] " + msg)
				}
			}
		},
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
