//go:build smslidingwindow

package traverser

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestM2ConcurrentSyncRunsKeepJobStateSeparate(t *testing.T) {
	t.Setenv("MOVER_SYNC_MJ_TRAV", "2")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	errs := make(chan error, 2)
	options := NewSyncOrchestratorOptions(100, true, time.Time{}, false, 3)
	var transferred [2][]string
	var finalized [2]bool
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		go func() {
			var once sync.Once
			job := SyncJob{
				Source:                common.ResourceString{Value: fmt.Sprintf("https://s3.amazonaws.com/bucket-%d/", i)},
				Destination:           common.ResourceString{Value: fmt.Sprintf("https://example.blob.core.windows.net/container-%d/", i)},
				FromTo:                common.EFromTo.S3Blob(),
				JobID:                 common.NewJobID(),
				UseStreamingMergeJoin: true,
				EncodeDestinationPath: func(path string) string { return path },
				Log:                   func(common.LogLevel, string, bool) {},
			}
			job.NewTraverser = func(_ common.ResourceString, location common.Location, ctx context.Context, _ InitResourceTraverserOptions) (ResourceTraverser, error) {
				if location == common.ELocation.S3() {
					once.Do(func() { started <- struct{}{} })
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return &fakeMergeJoinTraverser{objects: []StoredObject{mjTestFile(fmt.Sprintf("job-%d.txt", i))}}, nil
				}
				return &fakeMergeJoinTraverser{}, nil
			}
			enumerator := NewSyncEnumerator(
				&fakeMergeJoinTraverser{}, &fakeMergeJoinTraverser{}, NewObjectIndexer(), nil,
				func(StoredObject) error { return fmt.Errorf("unexpected deletion") },
				func() error {
					mu.Lock()
					finalized[i] = true
					mu.Unlock()
					return nil
				},
				SyncEnumeratorOptions{
					PrimaryTemplate:     ResourceTraverserTemplate{Location: common.ELocation.S3()},
					SecondaryTemplate:   ResourceTraverserTemplate{Location: common.ELocation.Blob()},
					OrchestratorOptions: &options,
					ScheduleTransfer: func(object StoredObject) error {
						mu.Lock()
						transferred[i] = append(transferred[i], object.RelativePath)
						mu.Unlock()
						return nil
					},
				},
			)
			errs <- RunSyncOrchestrator(ctx, job, enumerator)
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("concurrent orchestrators did not start")
		}
	}
	close(release)
	for i := 0; i < 2; i++ {
		require.NoError(t, <-errs)
	}
	require.Equal(t, []string{"job-0.txt"}, transferred[0])
	require.Equal(t, []string{"job-1.txt"}, transferred[1])
	require.Equal(t, [2]bool{true, true}, finalized)
	require.Equal(t, int32(3), options.parallelTraversers, "per-job merge-join tuning must not mutate caller options")
	require.Equal(t, common.EFromTo.Unknown(), options.fromTo, "per-job protocol must not leak into shared input options")
}

func TestM2HNSDirectorySelfEntriesCannotDeleteExistingDirectories(t *testing.T) {
	for _, dir := range []string{"", "/", "/a/", "///a/"} {
		t.Run(dir, func(t *testing.T) {
			settings := NewSyncOrchestratorOptions(100, false, time.Time{}, false, 2)
			settings.SetFromTo(common.EFromTo.BlobFSBlobFS())
			run := &syncRun{job: SyncJob{Log: func(common.LogLevel, string, bool) {}}, orchestratorOptions: &settings}
			indexer := NewObjectIndexer()
			var deleted []string
			comparator := NewSyncDestinationComparator(indexer, func(StoredObject) error { return nil },
				func(object StoredObject) error {
					deleted = append(deleted, object.RelativePath)
					return nil
				}, common.ESyncHashType.None(), false, false, common.EDeleteDestination.True(), nil, &settings)
			enumerator := &SyncEnumerator{ObjectIndexer: indexer, orchestratorOptions: &settings}
			traversal := run.newSyncTraverser(enumerator, dir, comparator.ProcessIfNecessary)
			self := mjTestFolder("")
			require.NoError(t, traversal.processor(self))
			require.Contains(t, indexer.IndexMap, strings.TrimPrefix(dir, "/"))
			require.Empty(t, traversal.sub_dirs, "a directory self-entry must never re-enqueue itself")
			require.NoError(t, traversal.customComparator(self))
			require.Empty(t, deleted, "an unchanged source directory must never be deleted at the destination")
			require.Empty(t, indexer.IndexMap, "source and destination self-entries must use the same key")
		})
	}
}
