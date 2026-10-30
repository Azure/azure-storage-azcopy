//go:build smslidingwindow

package traverser

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStaticFlatRangesCoverage checks fixed boundaries and unique ownership across worker settings.
func TestStaticFlatRangesCoverage(t *testing.T) {
	for workers := enum.MinFlatListParallelism; workers <= enum.MaxFlatListParallelism; workers++ {
		ranges := flatListRanges(workers)
		require.Empty(t, ranges[0].start)
		require.Empty(t, ranges[len(ranges)-1].end)
		if workers == 1 {
			require.Equal(t, []flatListRange{{}}, ranges)
		} else {
			require.Len(t, ranges, 63, "workers=%d", workers)
			require.Len(t, ranges, enum.MaxFlatListParallelism, "range count must match the supported maximum")
			for i, boundary := range "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" {
				require.Equal(t, string(boundary), ranges[i].end, "workers=%d boundary=%d", workers, i)
			}
		}
		for i := 1; i < len(ranges); i++ {
			require.Equal(t, ranges[i-1].end, ranges[i].start)
		}
		keys := []string{"/a", "0", "9", "A", "Z", "_", "a", "a//b", "d", "dir-000000000/blob", "z", "zzz", "\u03bb"}
		for ch := 'A'; ch <= 'Z'; ch++ {
			keys = append(keys, string(ch), string(ch)+"/\u4e2d", string(ch)+"suffix")
		}
		for _, key := range keys {
			owners := 0
			for _, bounds := range ranges {
				if key >= bounds.start && (bounds.end == "" || key < bounds.end) {
					owners++
				}
			}
			require.Equal(t, 1, owners, "key=%q workers=%d", key, workers)
		}
	}
}

// TestStaticFlatListParallelism checks the sixteen-worker default, overrides and explicit input errors.
func TestStaticFlatListParallelism(t *testing.T) {
	setting := enum.EEnvironmentVariable.MoverFlatListParallelism()
	t.Run("unset", func(t *testing.T) {
		t.Setenv(setting.Name, "")
		require.NoError(t, setting.Clear())
		require.Equal(t, "16", setting.Get())
		got, err := flatListParallelism()
		require.NoError(t, err)
		require.Equal(t, 16, got)
	})
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", 16}, {"1", 1}, {"2", 2}, {"8", 8}, {"16", 16}, {"32", 32}, {"33", 33}, {"37", 37}, {"63", 63},
		{"0", 0}, {"64", 0}, {"65", 0}, {"-1", 0}, {"bad", 0},
	} {
		t.Run("value="+tc.value, func(t *testing.T) {
			t.Setenv(setting.Name, tc.value)
			got, err := flatListParallelism()
			if tc.want == 0 {
				require.EqualError(t, err, setting.Name+" must be an integer between 1 and 63")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// TestStaticFlatRangesWorkerLimit checks concurrency limits and exactly-once range execution.
func TestStaticFlatRangesWorkerLimit(t *testing.T) {
	for _, workers := range []int{1, 8, 16, 32, enum.MaxFlatListParallelism} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			wantPeak := min(workers, len(flatListRanges(workers)))
			release := make(chan struct{})
			var mu sync.Mutex
			active, peak := 0, 0
			seen := make(map[flatListRange]int)
			err := runFlatListRanges(ctx, workers, nil, func(_ context.Context, bounds flatListRange) error {
				mu.Lock()
				active++
				peak = max(peak, active)
				seen[bounds]++
				// Hold the first wave until every worker is active, avoiding timing-based assertions.
				if len(seen) == wantPeak {
					close(release)
				}
				mu.Unlock()
				defer func() {
					mu.Lock()
					active--
					mu.Unlock()
				}()
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			require.NoError(t, err)
			require.Equal(t, wantPeak, peak)
			require.Zero(t, active)
			require.Len(t, seen, len(flatListRanges(workers)))
			for _, bounds := range flatListRanges(workers) {
				require.Equal(t, 1, seen[bounds], "range=%+v", bounds)
			}
		})
	}
}

// TestStaticFlatRangeStats checks minute cadence, queue accounting and reporter shutdown.
func TestStaticFlatRangeStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := captureFlatListLogs(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- runFlatListRanges(ctx, 8, nil, func(ctx context.Context, _ flatListRange) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		synctest.Wait()
		time.Sleep(59 * time.Second)
		synctest.Wait()
		require.NotContains(t, logger.text(), "[FlatListRangeStats]")
		time.Sleep(time.Second)
		synctest.Wait()
		require.Contains(t, logger.text(), "[FlatListRangeStats] activeWorkers=8/8 queuedRanges=55 completedRanges=0 failedRanges=0 cancelledRanges=0 totalRanges=63 final=false")
		close(release)
		require.NoError(t, <-done)
		synctest.Wait()
		require.Contains(t, logger.text(), "[FlatListRangeStats] activeWorkers=0/8 queuedRanges=0 completedRanges=63 failedRanges=0 cancelledRanges=0 totalRanges=63 final=true")
		require.Equal(t, 2, strings.Count(logger.text(), "[FlatListRangeStats]"))
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		require.Equal(t, 2, strings.Count(logger.text(), "[FlatListRangeStats]"), "reporter must stop when enumeration returns")
	})
}

// TestStaticFlatRangesContinueAfterFailure checks that healthy and queued ranges still finish.
func TestStaticFlatRangesContinueAfterFailure(t *testing.T) {
	logger := captureFlatListLogs(t)
	firstErr, lastErr := errors.New("first range failed"), errors.New("last range failed")
	firstFailed := make(chan struct{})
	var calls, completed atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runFlatListRanges(ctx, 2, nil, func(ctx context.Context, bounds flatListRange) error {
		calls.Add(1)
		if bounds.start == "" {
			close(firstFailed)
			return firstErr
		}
		select {
		case <-firstFailed:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if bounds.start == "z" {
			return lastErr
		}
		completed.Add(1)
		return nil
	})
	require.NoError(t, ctx.Err())
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, lastErr)
	require.EqualValues(t, 63, calls.Load())
	require.EqualValues(t, 61, completed.Load())
	require.Equal(t, 2, strings.Count(logger.text(), "[AzCopy] [ERROR] [FlatListRangeFailed]"))
	require.Equal(t, 61, strings.Count(logger.text(), "[AzCopy] [INFO] [FlatListRangeComplete]"))
	require.Contains(t, logger.text(), "activeWorkers=0/2 queuedRanges=0 completedRanges=61 failedRanges=2 cancelledRanges=0 totalRanges=63 final=true")
}

// TestStaticFlatRangesParentCancellation checks that cancellation stops and joins active tasks.
func TestStaticFlatRangesParentCancellation(t *testing.T) {
	logger := captureFlatListLogs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var started, stopped, reported atomic.Int32
	err := runFlatListRanges(ctx, 2, func(flatListRange, error) bool {
		reported.Add(1)
		return true
	}, func(ctx context.Context, _ flatListRange) error {
		if started.Add(1) == 2 {
			cancel()
		}
		<-ctx.Done()
		stopped.Add(1)
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 2, started.Load())
	require.EqualValues(t, 2, stopped.Load())
	require.Zero(t, reported.Load(), "job cancellation must not be reported as range failures")
	require.Contains(t, logger.text(), "activeWorkers=0/2 queuedRanges=0 completedRanges=0 failedRanges=0 cancelledRanges=63 totalRanges=63 final=true")
}

// TestStaticFlatRangesAllFailures checks that reporting every failed range cannot stall scheduling.
func TestStaticFlatRangesAllFailures(t *testing.T) {
	logger := captureFlatListLogs(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failure := errors.New("listing failed")
	var calls atomic.Int32
	err := runFlatListRanges(ctx, 8, nil, func(context.Context, flatListRange) error {
		calls.Add(1)
		return failure
	})
	require.NoError(t, ctx.Err())
	require.ErrorIs(t, err, failure)
	require.EqualValues(t, 63, calls.Load())
	require.Equal(t, 63, strings.Count(logger.text(), "[FlatListRangeFailed]"))
	require.NotContains(t, logger.text(), "[FlatListRangeComplete]")
}

// staticTestTraverser serves sorted, bounded Arrow pages through a real blob traverser.
// The failure selector injects protocol, authorization, cancellation or bounds errors.
func staticTestTraverser(t *testing.T, keys []string, root, failure string) (*BlobTraverser, *atomic.Int32) {
	t.Helper()
	keys = append([]string(nil), keys...)
	sort.Strings(keys)
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		q := r.URL.Query()
		assert.Equal(t, root, q.Get("prefix"))
		assert.Empty(t, q.Get("delimiter"))
		if failure == "legacy" {
			assert.NotContains(t, r.Header.Get("Accept"), "application/vnd.apache.arrow.stream")
			assert.Empty(t, q.Get("startFrom"))
			assert.Empty(t, q.Get("endBefore"))
		} else {
			assert.Equal(t, "2026-06-06", r.Header.Get("x-ms-version"))
		}
		if failure == "xml" {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>")
			return
		}
		if failure == "403" || ((failure == "403-a" || failure == "403-z") &&
			q.Get("startFrom") == root+strings.TrimPrefix(failure, "403-")) {
			w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, "<Error><Code>AuthorizationPermissionMismatch</Code></Error>")
			return
		}

		if failure == "404" {
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "<Error><Code>BlobNotFound</Code></Error>")
			return
		}
		if failure == "cancel" || (failure == "cancel-a" && q.Get("startFrom") == root+"a") {
			<-r.Context().Done()
			return
		}
		var selected []string
		for _, key := range keys {
			full := root + key
			if full >= q.Get("startFrom") && (q.Get("endBefore") == "" || full < q.Get("endBefore")) {
				selected = append(selected, key)
			}
		}
		// An empty first page with a continuation token must not be mistaken for exhaustion.
		next, page := "0", []string(nil)
		if marker := q.Get("marker"); marker != "" {
			offset, err := strconv.Atoi(marker)
			require.NoError(t, err)
			end := offset + 3
			if end > len(selected) {
				end = len(selected)
			}
			page, next = selected[offset:end], ""
			if end < len(selected) {
				next = strconv.Itoa(end)
			}
		}
		if failure == "bounds" {
			page, next = []string{"outside"}, ""
		}
		if failure == "legacy" {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<EnumerationResults><Blobs>")
			for _, key := range page {
				fmt.Fprintf(w, "<Blob><Name>%s</Name><Properties><Last-Modified>Sun, 13 Sep 2020 12:26:40 GMT</Last-Modified><Etag>etag</Etag><Content-Length>1</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>", html.EscapeString(root+key))
			}
			fmt.Fprintf(w, "</Blobs><NextMarker>%s</NextMarker></EnumerationResults>", html.EscapeString(next))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		w.Header().Set("x-ms-version", "2026-06-06")
		_, _ = w.Write(flatTestArrowPage(t, page, root, next))
	}))
	t.Cleanup(server.Close)
	endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	client, err := service.NewClientWithNoCredential(endpoint, &service.ClientOptions{
		ClientOptions: azcore.ClientOptions{PerCallPolicies: []policy.Policy{ste.NewVersionPolicy()},
			Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	require.NoError(t, err)
	location := common.ELocation.Blob()
	return NewBlobTraverser(endpoint+"/c/"+root, client, context.Background(),
		InitResourceTraverserOptions{FlatListing: true, Recursive: true, DestResourceType: &location}), calls
}

// TestStaticFlatRangeReporting checks bounded delivery and explicit failure when delivery is unavailable.
func TestStaticFlatRangeReporting(t *testing.T) {
	for _, mode := range []string{"available", "full", "nil"} {
		t.Run(mode, func(t *testing.T) {
			logger := captureFlatListLogs(t)
			var ch chan TraverserErrorItemInfo
			if mode == "available" {
				ch = make(chan TraverserErrorItemInfo, 63)
			} else if mode == "full" {
				ch = make(chan TraverserErrorItemInfo, 1)
				ch <- ErrorBlobInfo{}
			}
			src := &BlobTraverser{RawURL: "https://account.blob.core.windows.net/c/root/?sig=not-for-logs", errorChannel: ch}
			failure := errors.New("listing failed")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var calls atomic.Int32
			err := runFlatListRanges(ctx, 8, src.reportFlatRangeError, func(_ context.Context, bounds flatListRange) error {
				calls.Add(1)
				return src.flatRangeError(bounds, failure)
			})
			require.NoError(t, ctx.Err())
			require.EqualValues(t, 63, calls.Load())
			require.Contains(t, logger.text(), "activeWorkers=0/8 queuedRanges=0 completedRanges=0 failedRanges=63 cancelledRanges=0 totalRanges=63 final=true")
			if mode != "available" {
				require.ErrorIs(t, err, failure)
				return
			}
			require.NoError(t, err)
			close(ch)
			seen := make(map[flatListRange]bool)
			for item := range ch {
				info, ok := item.(*flatListRangeErrorInfo)
				require.True(t, ok)
				start, end := info.RangeBounds()
				bounds := flatListRange{start, end}
				require.False(t, seen[bounds])
				seen[bounds] = true
				require.True(t, info.IsDir())
				require.False(t, info.IsDestinationError())
				require.Equal(t, "https://account.blob.core.windows.net/c/root/", info.FullPath())
				require.NotContains(t, info.ErrorMessage().Error(), "not-for-logs")
				require.ErrorIs(t, info.ErrorMessage(), failure)
			}
			require.Len(t, seen, 63)
		})
	}
}

// TestStaticFlatRangesCopyReporting keeps finalization reachable without hiding dispatcher failures.
func TestStaticFlatRangesCopyReporting(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, "2")
	for _, tc := range []struct {
		name              string
		reporting         bool
		processingFailure bool
	}{
		{"reported-listing-failure", true, false},
		{"unreported-listing-failure", false, false},
		{"processing-failure", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, _ := staticTestTraverser(t, []string{"0/early", "a/file", "z/new"}, "source/", "403-a")
			var ch chan TraverserErrorItemInfo
			if tc.reporting {
				ch = make(chan TraverserErrorItemInfo, 100)
			}
			src.errorChannel = ch
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			src.ctx = ctx
			var got []string
			finalized := false
			failure := errors.New("dispatcher failed")
			e := NewCopyEnumerator(src, nil, func(obj StoredObject) error {
				if tc.processingFailure {
					return failure
				}
				got = append(got, obj.RelativePath)
				return nil
			}, func() error { finalized = true; return nil })
			err := e.Enumerate()
			require.NoError(t, ctx.Err())
			if tc.processingFailure {
				require.ErrorIs(t, err, failure)
			} else if tc.reporting {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, `flat range ["a","b")`)
				require.ErrorContains(t, err, "AuthorizationPermissionMismatch")
			}
			require.Equal(t, tc.reporting && !tc.processingFailure, finalized)
			if !tc.processingFailure {
				require.ElementsMatch(t, []string{"0/early", "z/new"}, got)
			}
			if tc.reporting {
				close(ch)
				require.Len(t, ch, 1)
				info := (<-ch).(*flatListRangeErrorInfo)
				require.Equal(t, flatListRange{"a", "b"}, info.bounds)
			}
		})
	}
}

// TestStaticFlatRangesProcessingFailure keeps merge processing errors fatal, not partial listing failures.
func TestStaticFlatRangesProcessingFailure(t *testing.T) {
	src, _ := staticTestTraverser(t, nil, "source/", "")
	dst, _ := staticTestTraverser(t, []string{"a/delete"}, "destination/", "")
	dst.isSyncDestination = true
	ch := make(chan TraverserErrorItemInfo, 100)
	src.errorChannel, dst.errorChannel = ch, ch
	var deleted []string
	var mu sync.Mutex
	e, _ := newMergeJoinRecordingEnumerator(&deleted, &mu)
	failure := errors.New("delete processing failed")
	e.objectComparator = func(StoredObject) error { return failure }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.ErrorIs(t, (&syncRun{}).mergeStaticFlatRanges(ctx, 2, e, mirrorSyncArgs(), src, dst), failure)
	require.NoError(t, ctx.Err())
	close(ch)
	require.Empty(t, ch)
}

// TestStaticFlatRangesOrchestratorReporting checks mixed endpoint failures, no root duplicate, and finalization.
func TestStaticFlatRangesOrchestratorReporting(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the sync orchestrator normalizes source URLs as filesystem paths on Windows")
	}
	for _, channelAvailable := range []bool{true, false} {
		t.Run(fmt.Sprintf("channelAvailable=%t", channelAvailable), func(t *testing.T) {
			src, _ := staticTestTraverser(t, []string{"0/early", "a/file", "z/new"}, "source/", "403-a")
			dst, _ := staticTestTraverser(t, []string{"a/keep", "m/remove", "z/extra"}, "destination/", "403-z")
			orchestratorOptions := &SyncOrchestratorOptions{fromTo: common.EFromTo.BlobBlob()}
			cca := mirrorSyncArgs()
			cca.Source = common.ResourceString{Value: strings.TrimSuffix(src.RawURL, "/")}
			cca.Destination = common.ResourceString{Value: strings.TrimSuffix(dst.RawURL, "/")}
			cca.UseFlatListing, cca.UseStreamingMergeJoin = true, true
			cca.NewTraverser = InitResourceTraverser
			cca.EncodeDestinationPath = func(path string) string { return path }
			var ch chan TraverserErrorItemInfo
			if channelAvailable {
				ch = make(chan TraverserErrorItemInfo, 100)
			}
			options := InitResourceTraverserOptions{
				Credential: &common.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()}, ErrorChannel: ch,
			}
			targetOptions := options
			targetOptions.IsSyncDestination = true
			configureFlatSyncTraversers(cca, &options, &targetOptions)
			var deleted []string
			var mu sync.Mutex
			e, scheduledTransferSources := newMergeJoinRecordingEnumerator(&deleted, &mu)
			e.primaryTraverser = src
			e.primaryTraverserTemplate = ResourceTraverserTemplate{Location: common.ELocation.Blob(), Options: options}
			e.secondaryTraverserTemplate = ResourceTraverserTemplate{Location: common.ELocation.Blob(), Options: targetOptions}
			e.orchestratorOptions = orchestratorOptions
			finalized := false
			e.finalize = func() error { finalized = true; return nil }
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			run := &syncRun{job: *cca, orchestratorOptions: orchestratorOptions, crawlParallelism: 1}
			err := run.runSyncOrchestrator(cca, e, ctx)
			require.NoError(t, ctx.Err())
			if !channelAvailable {
				require.Error(t, err)
				require.False(t, finalized)
				return
			}
			require.NoError(t, err)
			require.True(t, finalized)
			require.Equal(t, []string{"0/early"}, scheduledTransferSources())
			require.Equal(t, []string{"m/remove"}, deleted)
			close(ch)
			require.Len(t, ch, 2, "no duplicate root-directory error")
			seen := make(map[string]bool)
			for item := range ch {
				info, ok := item.(*flatListRangeErrorInfo)
				require.True(t, ok)
				start, _ := info.RangeBounds()
				require.False(t, seen[start])
				seen[start] = true
				require.Equal(t, start == "z", info.IsDestinationError())
				if info.IsDestinationError() {
					require.Equal(t, dst.RawURL, info.FullPath())
				} else {
					require.Equal(t, src.RawURL, info.FullPath())
				}
			}
			require.True(t, seen["a"] && seen["z"])
		})
	}
}

// TestStaticFlatRangesEnablement checks that only opted-in flat listing schedules ranges.
func TestStaticFlatRangesEnablement(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, "8")
	for _, tc := range []struct {
		name      string
		enabled   bool
		wantCalls int32
	}{
		{"legacy-flat-mode", false, 2},
		{"mover-flat-mode", true, 126},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := []string{"a/file", "z/file"}
			responseMode := ""
			if !tc.enabled {
				responseMode = "legacy"
			}
			src, calls := staticTestTraverser(t, keys, "root/", responseMode)
			src.FlatListingEnabled = tc.enabled
			if tc.enabled {
				require.NoError(t, src.validateFlatRanges())
			} else {
				require.Error(t, src.validateFlatRanges())
			}
			var got []string
			err := src.Traverse(nil, func(object StoredObject) error {
				got = append(got, object.RelativePath)
				return nil
			}, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, keys, got)
			require.Equal(t, tc.wantCalls, calls.Load())
		})
	}
}

// TestStaticFlatRangesCopyAndPairedSync checks copy coverage and sync/mirror decisions across roots.
func TestStaticFlatRangesCopyAndPairedSync(t *testing.T) {
	srcKeys := []string{"/a", "0", "9", "A", "Z", "_", "a", "a//b", "a/../b", "d", "z", "\u03bb/\u4e2d"}
	dstKeys := []string{"/a", "a//b", "backup/extra", "dir-100/extra", "zzz/extra"}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%c/sparse-%09d", 'a'+i%26, i*7919)
		srcKeys = append(srcKeys, key)
		if i%3 != 0 {
			dstKeys = append(dstKeys, key)
		}
	}
	t.Run("copy", func(t *testing.T) {
		setting := enum.EEnvironmentVariable.MoverFlatListParallelism()
		t.Setenv(setting.Name, "")
		require.NoError(t, setting.Clear())
		src, calls := staticTestTraverser(t, srcKeys, "root/", "")
		var got []string
		require.NoError(t, src.Traverse(nil, func(o StoredObject) error {
			got = append(got, o.RelativePath)
			return nil
		}, nil))
		require.ElementsMatch(t, srcKeys, got)
		require.GreaterOrEqual(t, calls.Load(), int32(len(flatListRanges(8))*2))
	})
	for _, mirror := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync-mirror=%t", mirror), func(t *testing.T) {
			src, _ := staticTestTraverser(t, srcKeys, "source/", "")
			dst, _ := staticTestTraverser(t, dstKeys, "destination/", "")
			dst.isSyncDestination = true
			var deleted []string
			var mu sync.Mutex
			enumerator, scheduledTransferSources := newMergeJoinRecordingEnumerator(&deleted, &mu)
			var unchanged int
			enumerator.primaryTraverserTemplate.Options.IncrementNotTransferred = func(common.EntityType) { unchanged++ }
			cca := mirrorSyncArgs()
			if !mirror {
				cca.DeleteDestination = common.EDeleteDestination.False()
			}
			require.NoError(t, (&syncRun{}).mergeStaticFlatRanges(context.Background(), 8, enumerator, cca, src, dst))
			sourceSet, destinationSet := map[string]bool{}, map[string]bool{}
			for _, key := range srcKeys {
				sourceSet[key] = true
			}
			for _, key := range dstKeys {
				destinationSet[key] = true
			}
			var copies, deletes []string
			same := 0
			for key := range sourceSet {
				if destinationSet[key] {
					same++
				} else {
					copies = append(copies, key)
				}
			}
			for key := range destinationSet {
				if !sourceSet[key] && mirror {
					deletes = append(deletes, key)
				}
			}
			actual := scheduledTransferSources()
			for i, path := range actual {
				decoded, err := url.PathUnescape(path)
				require.NoError(t, err)
				actual[i] = decoded
			}
			require.ElementsMatch(t, copies, actual)
			require.ElementsMatch(t, deletes, deleted)
			require.Equal(t, same, unchanged)
			require.Empty(t, enumerator.ObjectIndexer.IndexMap)
		})
	}
}

// TestStaticFlatRangesFailureAndCancellation checks that failed listings cannot authorize deletes.
// Out-of-range records must also be rejected before reaching the object processor.
func TestStaticFlatRangesFailureAndCancellation(t *testing.T) {
	for _, failure := range []string{"xml", "403", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			src, _ := staticTestTraverser(t, nil, "", failure)
			dst, _ := staticTestTraverser(t, []string{"a", "z"}, "", "")
			dst.isSyncDestination = true
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			var deleted []string
			var mu sync.Mutex
			enumerator, _ := newMergeJoinRecordingEnumerator(&deleted, &mu)
			require.Error(t, (&syncRun{}).mergeStaticFlatRanges(ctx, 8, enumerator, mirrorSyncArgs(), src, dst))
			require.Empty(t, deleted)
		})
	}
	t.Run("bounds-before-emission", func(t *testing.T) {
		src, _ := staticTestTraverser(t, nil, "", "bounds")
		traverser := &flatRangeTraverser{blob: src, bounds: flatListRange{start: "a", end: "b"}}
		emitted := 0
		require.Error(t, traverser.Traverse(nil, func(StoredObject) error { emitted++; return nil }, nil))
		require.Zero(t, emitted)
	})
}

// TestStaticFlatRangesSyncContinuesAfterFailure checks healthy-range progress and delete isolation.
func TestStaticFlatRangesSyncContinuesAfterFailure(t *testing.T) {
	for _, failedSide := range []string{"source", "destination"} {
		t.Run(failedSide, func(t *testing.T) {
			sourceFailure, destinationFailure := "", ""
			if failedSide == "source" {
				sourceFailure = "403-a"
				destinationFailure = "cancel-a"
			} else {
				sourceFailure = "cancel-a"
				destinationFailure = "403-a"
			}
			src, _ := staticTestTraverser(t, []string{"a/file", "z/new"}, "source/", sourceFailure)
			dst, _ := staticTestTraverser(t, []string{"a/keep", "z/extra"}, "destination/", destinationFailure)
			dst.isSyncDestination = true
			var deleted []string
			var mu sync.Mutex
			enumerator, scheduledTransferSources := newMergeJoinRecordingEnumerator(&deleted, &mu)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := (&syncRun{}).mergeStaticFlatRanges(ctx, 2, enumerator, mirrorSyncArgs(), src, dst)
			require.NoError(t, ctx.Err())
			require.ErrorContains(t, err, `flat range ["a","b")`)
			require.ErrorContains(t, err, failedSide+" traversal error")
			var traversalErr *mergeJoinTraversalError
			require.ErrorAs(t, err, &traversalErr)
			require.Equal(t, common.ELocation.Blob(), traversalErr.location)
			require.Equal(t, []string{"z/new"}, scheduledTransferSources())
			require.Equal(t, []string{"z/extra"}, deleted)
		})
	}
}

// TestStaticFlatRangesAbsentDestination remains a successful source-only sync, not a pair failure.
func TestStaticFlatRangesAbsentDestination(t *testing.T) {
	src, _ := staticTestTraverser(t, []string{"a/file", "z/new"}, "source/", "")
	dst, _ := staticTestTraverser(t, nil, "destination/", "404")
	dst.isSyncDestination = true
	var deleted []string
	var mu sync.Mutex
	enumerator, scheduledTransferSources := newMergeJoinRecordingEnumerator(&deleted, &mu)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, (&syncRun{}).mergeStaticFlatRanges(ctx, 2, enumerator, mirrorSyncArgs(), src, dst))
	require.NoError(t, ctx.Err())
	require.ElementsMatch(t, []string{"a/file", "z/new"}, scheduledTransferSources())
	require.Empty(t, deleted)
}

// TestStaticFlatRangePageEdges checks page boundaries without tracking cross-page ordering.
// Invalid edges must fail before any record from that page is emitted.
func TestStaticFlatRangePageEdges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pages     [][]string
		want      []string
		wantError bool
	}{
		{"empty", [][]string{{}}, nil, false},
		{"single", [][]string{{"a"}}, []string{"a"}, false},
		{"multiple-pages", [][]string{{"a", "b"}, {}, {"c"}}, []string{"a", "b", "c"}, false},
		{"before-start", [][]string{{"0", "b"}}, nil, true},
		{"exclusive-end", [][]string{{"b", "d"}}, nil, true},
		{"reversed-edges", [][]string{{"c", "b"}}, nil, true},
		{"duplicate-edges", [][]string{{"b", "b"}}, nil, true},
		{"cross-page-repeat", [][]string{{"b"}, {}, {"b"}}, []string{"b", "b"}, false},
		{"cross-page-regression", [][]string{{"c"}, {"b"}}, []string{"c", "b"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listing, _ := newFlatTestListing(t, context.Background(), tc.pages, "root/", nil)
			traverser := &flatRangeTraverser{blob: listing.traverser, bounds: flatListRange{"a", "d"}}
			var got []string
			err := traverser.Traverse(nil, func(object StoredObject) error {
				got = append(got, object.RelativePath)
				return nil
			}, nil)
			if tc.wantError {
				require.ErrorContains(t, err, "static range bounds/order violation")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// TestStaticFlatRangeSyncProducerOrdering retains the sync producer's cross-page order guard.
func TestStaticFlatRangeSyncProducerOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listing, _ := newFlatTestListing(t, ctx, [][]string{{"c"}, {}, {"b"}}, "root/", nil)
	traverser := &flatRangeTraverser{blob: listing.traverser, bounds: flatListRange{"a", "d"}}
	sideErr := &twoWaySideErr{}
	folderCh, fileCh := (&syncRun{}).traverserToTypedChannels(ctx, traverser, nil, "SRC[]", sideErr, false)
	drainTypedChannelsForTest(folderCh, fileCh)
	require.ErrorContains(t, sideErr.get(), "ORDER VIOLATION")
}

// TestStaticFlatRawDeletePaths protects literal separators and dot segments in deletion paths.
func TestStaticFlatRawDeletePaths(t *testing.T) {
	for _, tc := range [][3]string{{"", "/a", "/a"}, {"root/", "a//b", "root/a//b"}, {"root", "../a", "root/../a"}, {"root", "", "root"}} {
		require.Equal(t, tc[2], common.FlatBlobObjectPath(tc[0], tc[1]))
	}
}

// TestStaticFlatPoolSizing checks the serial pool and two connections per active range pair.
func TestStaticFlatPoolSizing(t *testing.T) {
	captureCrawlLogs(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	for _, tc := range []struct {
		value       string
		connections int
	}{{"", 32}, {"1", 4}, {"8", 16}, {"16", 32}, {"32", 64}, {"37", 74}, {"63", 126}, {"64", 0}, {"65", 0}, {"bad", 0}} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, tc.value)
			pool := &flatListPoolTransport{shared: server.Client()}
			ctx := context.WithValue(context.Background(), flatListPoolKey{}, struct{}{})
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			require.NoError(t, err)
			response, err := pool.Do(req)
			if tc.connections == 0 {
				require.Error(t, err)
				require.Nil(t, pool.listing)
				return
			}
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			defer pool.listing.CloseIdleConnections()
			require.Equal(t, tc.connections, pool.listing.Transport.(*http.Transport).MaxConnsPerHost)
		})
	}
}

// TestStaticFlatPrefixIntersection checks request bounds for overlapping and disjoint prefilters.
func TestStaticFlatPrefixIntersection(t *testing.T) {
	for _, tc := range []struct {
		prefix     string
		bounds     flatListRange
		start, end string
		empty      bool
	}{
		{"root/", flatListRange{"a", "b"}, "root/a", "root/b", false},
		{"root/ab", flatListRange{"a", "b"}, "root/ab", "", false},
		{"root/z", flatListRange{"a", "b"}, "", "", true},
		{"root/a", flatListRange{"b", "c"}, "", "", true},
		{"root/ab", flatListRange{"aa", "ac"}, "root/ab", "", false},
		{"root/a", flatListRange{"ab", "ac"}, "root/ab", "root/ac", false},
		{"root/z", flatListRange{"z", ""}, "root/z", "", false},
	} {
		start, end, empty := flatListRequestBounds("root/", tc.prefix, tc.bounds)
		require.Equal(t, tc.start, start)
		require.Equal(t, tc.end, end)
		require.Equal(t, tc.empty, empty)
	}
}
