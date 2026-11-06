//go:build smslidingwindow

package traverser

import (
	"context"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// These shims let the external traverser_test package, which can import azcopy for the real
// path-encoding rules without an import cycle, reach unexported sync orchestrator internals.

func SyncDirectoryResources(job *SyncJob, relativePath string) (common.ResourceString, common.ResourceString) {
	return (&syncRun{job: *job}).syncDirectoryResources(relativePath)
}

func ValidateSyncJob(job *SyncJob, options *SyncOrchestratorOptions) error {
	r := &syncRun{job: *job, orchestratorOptions: options}
	return r.validate(&r.job, options)
}

// RunSyncOrchestratorUnthrottled runs the crawl directly with a fixed parallelism and throttling off.
func RunSyncOrchestratorUnthrottled(ctx context.Context, job *SyncJob, enumerator *SyncEnumerator, crawlParallelism int32) error {
	r := &syncRun{job: *job, orchestratorOptions: enumerator.orchestratorOptions, crawlParallelism: crawlParallelism}
	return r.runSyncOrchestrator(&r.job, enumerator, ctx)
}
