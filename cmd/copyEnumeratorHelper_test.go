// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type crawlTestLogger struct {
	common.ILoggerResetable
	mu       sync.Mutex
	messages []string
}

func TestFlatListTimingMeasuresRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("x-ms-error-code", "ServerBusy")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `<Error><Code>ServerBusy</Code><Message>busy</Message></Error>`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`)
	}))
	defer server.Close()
	client, err := container.NewClientWithNoCredential(server.URL+"/container", &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Retry:            policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
			PerRetryPolicies: []policy.Policy{flatListTimingPolicy{}},
		},
	})
	require.NoError(t, err)
	var timing flatListPageTiming
	ctx := context.WithValue(context.Background(), flatListTimingKey{}, &timing)
	start := time.Now()
	_, err = client.NewListBlobsFlatPager(nil).NextPage(ctx)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, 2, timing.attempts)
	require.Equal(t, 1, timing.throttles)
	require.Positive(t, timing.attemptTime)
	require.Positive(t, timing.retryGap)
	require.LessOrEqual(t, timing.attemptTime+timing.retryGap, elapsed)
}

type flatRetryTransport func(*http.Request) (*http.Response, error)

func (f flatRetryTransport) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestFlatListRetryOptionsWiring(t *testing.T) {
	captureCrawlLogs(t)
	t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "")
	t.Setenv("MOVER_FLAT_LIST_DIAGNOSTICS", "false")
	destination := common.ELocation.Blob()
	baseline := createClientOptions(azcopyScanningLogger, nil, nil).Retry
	traverser, err := InitResourceTraverser(
		common.ResourceString{Value: "https://example.blob.core.windows.net/c"},
		common.ELocation.Blob(), context.Background(), InitResourceTraverserOptions{
			Credential: &cred.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()},
			Recursive:  true, FlatListing: true, DestResourceType: &destination,
		})
	require.NoError(t, err)
	blobTraverser, ok := traverser.(*blobTraverser)
	require.True(t, ok)
	require.False(t, blobTraverser.flatListDiagnostics)
	if !buildmode.HighPerf() {
		require.Nil(t, blobTraverser.flatListRetryOptions)
		return
	}
	require.NotNil(t, blobTraverser.flatListRetryOptions)
	actual := *blobTraverser.flatListRetryOptions
	require.Equal(t, 100*time.Millisecond, actual.RetryDelay)
	require.Equal(t, ste.UploadMaxTries, int(actual.MaxRetries))
	require.Equal(t, ste.UploadTryTimeout, actual.TryTimeout)
	require.Equal(t, ste.UploadMaxRetryDelay, actual.MaxRetryDelay)
	require.Equal(t, baseline.StatusCodes, actual.StatusCodes)
	require.Equal(t, baseline.ShouldRetry == nil, actual.ShouldRetry == nil)
	require.Equal(t, time.Second, createClientOptions(azcopyScanningLogger, nil, nil).Retry.RetryDelay)
}

func TestFlatListPageRetries(t *testing.T) {
	captureCrawlLogs(t)
	t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "")
	t.Setenv("MOVER_FLAT_LIST_DIAGNOSTICS", "false")
	for _, tc := range []struct {
		name          string
		noOptIn       bool
		status        int
		failures      int
		retries       int32
		cap           time.Duration
		header        string
		headerValue   string
		headerDelay   time.Duration
		cancelBackoff bool
		rejectRetry   bool
		statusCodes   []int
		wantCalls     int
		wantError     bool
		checkShared   bool
	}{
		{name: "503 then success", status: 503, failures: 1, retries: 3, wantCalls: 2, checkShared: true},
		{name: "without flat opt in", noOptIn: true, status: 503, failures: 1, retries: 3, wantCalls: 2},
		{name: "repeated 503 increasing delays", status: 503, failures: 10, retries: 3, wantCalls: 4, wantError: true},
		{name: "twenty retries preserved", status: 503, failures: 30, retries: 20, cap: 50 * time.Millisecond, wantCalls: 21, wantError: true},
		{name: "disabled retries preserved", status: 503, failures: 1, retries: -1, wantCalls: 1, wantError: true},
		{name: "retry after seconds", status: 503, failures: 1, retries: 3, header: "Retry-After", headerValue: "2", headerDelay: 2 * time.Second, wantCalls: 2},
		{name: "retry after date", status: 503, failures: 1, retries: 3, header: "Retry-After", headerValue: "date", headerDelay: 3 * time.Second, wantCalls: 2},
		{name: "retry after milliseconds", status: 503, failures: 1, retries: 3, header: "retry-after-ms", headerValue: "250", headerDelay: 250 * time.Millisecond, wantCalls: 2},
		{name: "x ms retry after", status: 503, failures: 1, retries: 3, header: "x-ms-retry-after-ms", headerValue: "350", headerDelay: 350 * time.Millisecond, wantCalls: 2},
		{name: "retry after exceeds cap", status: 503, failures: 1, retries: 3, cap: time.Second, header: "Retry-After", headerValue: "2", wantCalls: 1, wantError: true},
		{name: "cancellation during backoff", status: 503, failures: 10, retries: 3, cancelBackoff: true, wantCalls: 1, wantError: true},
		{name: "retry predicate preserved", status: 503, failures: 1, retries: 3, rejectRetry: true, wantCalls: 1, wantError: true},
		{name: "status eligibility preserved", status: 503, failures: 1, retries: 3, statusCodes: []int{429}, wantCalls: 1, wantError: true},
		{name: "429 remains retryable", status: 429, failures: 1, retries: 3, wantCalls: 2},
		{name: "500 remains retryable", status: 500, failures: 1, retries: 3, wantCalls: 2},
		{name: "403 not retried", status: 403, failures: 1, retries: 3, wantCalls: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				options := policy.RetryOptions{
					MaxRetries: tc.retries, RetryDelay: time.Second,
					MaxRetryDelay: time.Minute, TryTimeout: 12 * time.Second, StatusCodes: tc.statusCodes,
				}
				if tc.cap != 0 {
					options.MaxRetryDelay = tc.cap
				}
				predicateCalls := 0
				if tc.rejectRetry {
					options.ShouldRetry = func(*http.Response, error) bool {
						predicateCalls++
						return false
					}
				}
				var attempts []time.Time
				transport := flatRetryTransport(func(req *http.Request) (*http.Response, error) {
					attempts = append(attempts, time.Now())
					deadline, ok := req.Context().Deadline()
					require.True(t, ok, "per-attempt timeout must survive the override")
					require.Equal(t, options.TryTimeout, deadline.Sub(time.Now()))
					status := http.StatusOK
					body := `<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`
					headers := http.Header{"Content-Type": []string{"application/xml"}}
					if len(attempts) <= tc.failures {
						status = tc.status
						body = `<Error><Code>ServerBusy</Code><Message>fixture</Message></Error>`
						headers.Set("x-ms-error-code", "ServerBusy")
						if tc.header != "" {
							value := tc.headerValue
							if value == "date" {
								value = time.Now().Add(tc.headerDelay).Format(http.TimeFormat)
							}
							headers.Set(tc.header, value)
						}
					}
					return &http.Response{StatusCode: status, Header: headers,
						Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
				})
				client, err := container.NewClientWithNoCredential("https://example.blob.core.windows.net/c",
					&container.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: options, Transport: transport}})
				require.NoError(t, err)
				destination := common.ELocation.Blob()
				traverser := newBlobTraverser(client.URL(), nil, ctx, InitResourceTraverserOptions{
					FlatListing: !tc.noOptIn, Recursive: true, DestResourceType: &destination, retryOptions: options,
				})
				if tc.cancelBackoff {
					time.AfterFunc(50*time.Millisecond, cancel)
				}
				start := time.Now()
				err = traverser.serialList(client, "c", "", "", nil, func(StoredObject) error { return nil }, nil)
				if tc.wantError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Len(t, attempts, tc.wantCalls)
				if tc.rejectRetry {
					require.Equal(t, 1, predicateCalls)
				}
				if tc.cancelBackoff {
					require.ErrorContains(t, err, "context canceled")
					require.Equal(t, 50*time.Millisecond, time.Since(start))
				}
				base := options.RetryDelay
				if buildmode.HighPerf() && !tc.noOptIn {
					base = 100 * time.Millisecond
				}
				for i := 1; i < len(attempts); i++ {
					gap := attempts[i].Sub(attempts[i-1])
					if tc.headerDelay > 0 {
						require.Equal(t, tc.headerDelay, gap)
					} else {
						delay := time.Duration((1<<i)-1) * base
						minimum := min(time.Duration(float64(delay)*0.8), options.MaxRetryDelay)
						maximum := min(time.Duration(float64(delay)*1.3), options.MaxRetryDelay)
						require.GreaterOrEqual(t, gap, minimum)
						require.LessOrEqual(t, gap, maximum)
					}
				}
				if tc.checkShared {
					attempts = nil
					_, err := client.NewListBlobsFlatPager(nil).NextPage(ctx)
					require.NoError(t, err)
					require.Len(t, attempts, 2)
					require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 800*time.Millisecond)
					require.Less(t, attempts[1].Sub(attempts[0]), 1300*time.Millisecond)
				}
			})
		})
	}
}

func (l *crawlTestLogger) ShouldLog(common.LogLevel) bool { return false }
func (l *crawlTestLogger) Log(_ common.LogLevel, message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, message)
}
func (l *crawlTestLogger) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.messages, "\n")
}

type crawlTestLifecycle struct{ common.LifecycleMgr }

func (*crawlTestLifecycle) Info(string) {}
func (*crawlTestLifecycle) Warn(string) {}

func captureCrawlLogs(t *testing.T) *crawlTestLogger {
	t.Helper()
	logger := &crawlTestLogger{}
	oldLogger, oldLCM := azcopyScanningLogger, glcm
	azcopyScanningLogger, glcm = logger, &crawlTestLifecycle{}
	t.Cleanup(func() { azcopyScanningLogger, glcm = oldLogger, oldLCM })
	return logger
}

func TestResolveHighPerfMaxQueueDirs(t *testing.T) {
	captureCrawlLogs(t)
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", 50_000_000},
		{"   ", 50_000_000},
		{"100000000", 100_000_000},
		{" 1234567 ", 1_234_567},
		{"0", 50_000_000},
		{"-1", 50_000_000},
		{"invalid", 50_000_000},
		{"999999999999999999999", 50_000_000},
		{"17", 17}, // A subsequent job's override must be reread.
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("MOVER_HIGH_PERF_MAX_QUEUED_DIRS", tc.value)
			want := tc.want
			if !buildmode.HighPerf() {
				want = 0
			}
			assert.Equal(t, want, resolveHighPerfMaxQueueDirs())
		})
	}
}

func TestBlobCrawlStatsLifecycle(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelParent=%t", cancelParent), func(t *testing.T) {
			logger := captureCrawlLogs(t)
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stats := &parallel.CrawlStats{ActiveWorkers: 7, QueuedDirs: 42, PeakQueuedDirs: 123, MaxQueueDirectories: 50_000_000}
				stop := startBlobCrawlStats(ctx, stats, 100, true)
				defer stop()
				synctest.Wait()
				time.Sleep(30 * time.Second)
				synctest.Wait()
				if buildmode.HighPerf() {
					assert.Contains(t, logger.text(), "[CrawlConfig] mode=blob-parallel, crawlParallelism=100, maxQueueDirectories=50000000, randomDequeue=true")
					assert.Contains(t, logger.text(), "[CrawlStats] mode=blob-parallel, activeWorkers=7/100, queuedDirs=42")
					assert.Contains(t, logger.text(), "peakQueuedDirs=123, final=false")
				}
				if cancelParent {
					cancel()
					synctest.Wait()
				}
				stop()
				if buildmode.HighPerf() {
					assert.Contains(t, logger.text(), "peakQueuedDirs=123, final=true")
				} else {
					assert.Empty(t, logger.text())
				}
				before := logger.text()
				time.Sleep(60 * time.Second)
				synctest.Wait()
				assert.Equal(t, before, logger.text(), "monitor must stop when enumeration exits")
			})
		})
	}
}

func TestBlobParallelCrawlConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs/><NextMarker/></EnumerationResults>`)
	}))
	defer server.Close()
	client, err := container.NewClientWithNoCredential(server.URL+"/container", nil)
	if !assert.NoError(t, err) {
		return
	}
	oldParallelism := EnumerationParallelism
	EnumerationParallelism = 100
	defer func() { EnumerationParallelism = oldParallelism }()
	for _, override := range []string{"", "12345"} {
		for _, suppress := range []bool{false, true} {
			t.Run(fmt.Sprintf("override=%s/suppress=%t", override, suppress), func(t *testing.T) {
				logger := captureCrawlLogs(t)
				t.Setenv("MOVER_HIGH_PERF_MAX_QUEUED_DIRS", override)
				traverser := newBlobTraverser(server.URL+"/container", nil, context.Background(),
					InitResourceTraverserOptions{Recursive: true, SuppressCrawlStats: suppress})
				err := traverser.parallelList(client, "container", "", "", nil, func(StoredObject) error {
					t.Error("empty listing must not emit objects")
					return nil
				}, nil)
				assert.NoError(t, err)
				if buildmode.HighPerf() && !suppress {
					want := "50000000"
					if override != "" {
						want = override
					}
					assert.Contains(t, logger.text(), "crawlParallelism=100, maxQueueDirectories="+want)
					assert.Contains(t, logger.text(), "activeWorkers=0/100, queuedDirs=0")
					assert.Contains(t, logger.text(), "final=true")
				} else {
					assert.NotContains(t, logger.text(), "[CrawlConfig]")
					assert.NotContains(t, logger.text(), "[CrawlStats]")
				}
			})
		}
	}
}

func newLocalRes(path string) common.ResourceString {
	return common.ResourceString{Value: path}
}

func newRemoteRes(url string) common.ResourceString {
	r, err := SplitResourceString(url, common.ELocation.Blob())
	if err != nil {
		panic("can't parse resource string")
	}
	return r
}

func TestRelativePath(t *testing.T) {
	a := assert.New(t)
	// setup
	cca := CookedCopyCmdArgs{
		Source:      newLocalRes("a/b/"),
		Destination: newLocalRes("y/z/"),
	}

	object := StoredObject{
		name:         "c.txt",
		entityType:   1,
		relativePath: "c.txt",
	}

	// execute
	srcRelPath := cca.MakeEscapedRelativePath(true, false, false, object)
	destRelPath := cca.MakeEscapedRelativePath(false, true, false, object)

	// assert
	a.Equal("/c.txt", srcRelPath)
	a.Equal("/c.txt", destRelPath)
}

func TestFlatListingSelection(t *testing.T) {
	t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "")
	blob := common.ELocation.Blob()
	for _, tc := range []struct {
		name string
		flat bool
		hns  bool
		dest common.Location
		want bool
	}{
		{"FNS-FNS opt in", true, false, blob, true},
		{"unknown source", false, false, blob, false},
		{"HNS-FNS", true, true, blob, false},
		{"FNS-HNS", true, false, common.ELocation.BlobFS(), false},
		{"FNS-Files", true, false, common.ELocation.File(), false},
		{"FNS-local", true, false, common.ELocation.Local(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			traverser := newBlobTraverser("https://example.blob.core.windows.net/c", nil, context.Background(),
				InitResourceTraverserOptions{FlatListing: tc.flat, Recursive: true, DestResourceType: &tc.dest},
				BlobTraverserOptions{isDFS: &tc.hns})
			require.Equal(t, !(tc.want && buildmode.HighPerf()), traverser.parallelListing)
			require.Equal(t, tc.want && buildmode.HighPerf(), traverser.flatListRetryOptions != nil)
		})
	}
	t.Run("legacy forced serial without opt in", func(t *testing.T) {
		t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "true")
		traverser := newBlobTraverser("https://example.blob.core.windows.net/c", nil, context.Background(),
			InitResourceTraverserOptions{Recursive: true, DestResourceType: &blob})
		require.False(t, traverser.parallelListing)
		require.Nil(t, traverser.flatListRetryOptions)
	})
}

// flatTestListing exercises the real SDK pager and blob object construction, without Azure access.
type flatTestListing struct {
	traverser *blobTraverser
}

func (f *flatTestListing) IsDirectory(bool) (bool, error) { return true, nil }

func (f *flatTestListing) Traverse(pre objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	return f.traverser.Traverse(pre, processor, filters)
}

func newFlatTestListing(t *testing.T, ctx context.Context, pages [][]string, prefix string,
	beforePage func(int, http.ResponseWriter, *http.Request) bool) (*flatTestListing, *atomic.Int32) {
	t.Helper()
	if !buildmode.HighPerf() {
		t.Skip("flat enumeration requires the high-performance mover profile")
	}
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		require.Equal(t, "list", r.URL.Query().Get("comp"))
		require.Empty(t, r.URL.Query().Get("delimiter"))
		require.Equal(t, prefix, r.URL.Query().Get("prefix"))
		page := 0
		if marker := r.URL.Query().Get("marker"); marker != "" {
			var err error
			page, err = strconv.Atoi(marker)
			require.NoError(t, err)
		}
		if beforePage != nil && !beforePage(page, w, r) {
			return
		}
		if page >= len(pages) {
			t.Errorf("unexpected page %d", page)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<EnumerationResults><Blobs>`)
		for _, name := range pages[page] {
			fmt.Fprintf(w, `<Blob><Name>%s</Name><Properties><Last-Modified>Sun, 13 Sep 2020 12:26:40 GMT</Last-Modified><Etag>etag</Etag><Content-Length>1</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>`,
				html.EscapeString(prefix+name))
		}
		fmt.Fprint(w, `</Blobs><NextMarker>`)
		if page+1 < len(pages) {
			fmt.Fprint(w, page+1)
		}
		fmt.Fprint(w, `</NextMarker></EnumerationResults>`)
	}))
	t.Cleanup(server.Close)
	// localhost avoids the SDK's IP-address emulator URL parsing rules.
	endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	client, err := service.NewClientWithNoCredential(endpoint, nil)
	require.NoError(t, err)
	location := common.ELocation.Blob()
	traverser := newBlobTraverser(endpoint+"/c/"+prefix, client, ctx,
		InitResourceTraverserOptions{FlatListing: true, Recursive: true, DestResourceType: &location})
	return &flatTestListing{traverser}, calls
}

func TestFlatCopyNestedBlobsAcrossPages(t *testing.T) {
	listing, calls := newFlatTestListing(t, context.Background(),
		[][]string{{"a", "a-1/item", "a/deep/blob"}, {"b space/%literal", "z/end"}}, "selected/", nil)
	var paths []string
	var scanned int
	listing.traverser.incrementEnumerationCounter = func(kind common.EntityType) {
		require.Equal(t, common.EEntityType.File(), kind)
		scanned++
	}
	err := listing.Traverse(noPreProccessor, func(obj StoredObject) error {
		require.False(t, obj.isVirtualPrefix)
		paths = append(paths, obj.relativePath)
		return nil
	}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "a-1/item", "a/deep/blob", "b space/%literal", "z/end"}, paths)
	require.Equal(t, 5, scanned)
	require.EqualValues(t, 2, calls.Load())
}

func TestFlatCopyFiltersMarkerAndPropagatesProcessorError(t *testing.T) {
	listing, _ := newFlatTestListing(t, context.Background(),
		[][]string{{"__AZURE_STORAGE_MOVER_MARKER", "nested/file"}}, "", nil)
	wantErr := fmt.Errorf("stop scheduling")
	var paths []string
	err := listing.Traverse(noPreProccessor, func(obj StoredObject) error {
		paths = append(paths, obj.relativePath)
		return wantErr
	}, buildRegexFilters([]string{".*__AZURE_STORAGE_MOVER_MARKER.*"}, false))
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []string{"nested/file"}, paths)
}

func TestFlatListingPageFailure(t *testing.T) {
	listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a/file"}, {}}, "",
		func(page int, w http.ResponseWriter, r *http.Request) bool {
			if page == 1 {
				w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `<Error><Code>AuthorizationPermissionMismatch</Code><Message>denied</Message></Error>`)
				return false
			}
			return true
		})
	err := listing.Traverse(noPreProccessor, func(StoredObject) error { return nil }, nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "AuthorizationPermissionMismatch"))
}
