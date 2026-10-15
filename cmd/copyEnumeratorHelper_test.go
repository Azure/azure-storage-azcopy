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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type crawlTestLogger struct {
	common.ILoggerResetable
	mu       sync.Mutex
	messages []string
}

type liveFlatListMetrics struct {
	format                        string
	attempts, throttles, responses int64
	bytes                         int64
	attemptTime                   time.Duration
}

type liveFlatListGuardError struct{}

func (*liveFlatListGuardError) Error() string { return "listing benchmark request guard" }
func (*liveFlatListGuardError) NonRetriable() {}

var errLiveFlatListGuard = &liveFlatListGuardError{}

func (m *liveFlatListMetrics) Do(req *policy.Request) (*http.Response, error) {
	if req.Raw().Method != http.MethodGet || req.Raw().URL.Query().Get("comp") != "list" {
		return nil, fmt.Errorf("%w: refused a non-list request", errLiveFlatListGuard)
	}
	version := ""
	for name, values := range req.Raw().Header {
		if strings.EqualFold(name, "x-ms-version") && len(values) > 0 {
			version = values[0]
		}
	}
	if version != "2026-06-06" {
		return nil, fmt.Errorf("%w: requires service version 2026-06-06", errLiveFlatListGuard)
	}
	m.attempts++
	start := time.Now()
	resp, err := req.Next()
	m.attemptTime += time.Since(start)
	if resp != nil {
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			m.throttles++
		}
		if resp.StatusCode == http.StatusOK {
			expected := "application/xml"
			if m.format == "arrow" {
				expected = "application/vnd.apache.arrow.stream"
			}
			if !strings.HasPrefix(resp.Header.Get("Content-Type"), expected) {
				resp.Body.Close()
				return nil, fmt.Errorf("%w: requested %s but received %q", errLiveFlatListGuard, m.format, resp.Header.Get("Content-Type"))
			}
			m.responses++
		}
		if resp.Body != nil {
			resp.Body = &liveFlatListBody{ReadCloser: resp.Body, metrics: m}
		}
	}
	return resp, err
}

type liveFlatListBody struct {
	io.ReadCloser
	metrics *liveFlatListMetrics
}

func (r *liveFlatListBody) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.metrics.bytes += int64(n)
	return n, err
}

func TestFlatListingBenchmarkGuard(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "2026-06-06", r.Header.Get("x-ms-version"))
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`)
	}))
	defer server.Close()
	metrics := &liveFlatListMetrics{format: "xml"}
	options := createClientOptions(nil, nil, nil)
	options.PerRetryPolicies = append(options.PerRetryPolicies, metrics)
	client, err := container.NewClientWithNoCredential(server.URL+"/c", &container.ClientOptions{ClientOptions: options})
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, "2026-06-06")
	_, err = client.NewListBlobsFlatPager(nil).NextPage(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, metrics.responses)
	require.Positive(t, metrics.bytes)
	metrics.format = "arrow"
	_, err = client.NewListBlobsFlatPager(nil).NextPage(ctx)
	require.ErrorIs(t, err, errLiveFlatListGuard)
	_, err = client.Delete(ctx, nil)
	require.ErrorIs(t, err, errLiveFlatListGuard)
	_, err = client.NewListBlobsFlatPager(nil).NextPage(context.WithValue(ctx, ste.ServiceAPIVersionOverride, "2025-05-05"))
	require.ErrorIs(t, err, errLiveFlatListGuard)
	require.EqualValues(t, 2, calls.Load(), "non-list and wrong-version requests must never reach the service")
}

// TestLiveFlatListingBenchmark is read-only and skipped unless explicitly opted in.
// Compile with go test -c, then run only this test inside the worker container.
func TestLiveFlatListingBenchmark(t *testing.T) {
	if os.Getenv("AZCOPY_LIVE_LIST_BENCHMARK") != "true" {
		t.Skip("set AZCOPY_LIVE_LIST_BENCHMARK=true to run the live listing benchmark")
	}
	require.True(t, buildmode.HighPerf(), "benchmark requires MOVER_HIGH_PERF=true")
	source := os.Getenv("LIST_BENCH_SOURCE")
	parts, err := blob.ParseURL(source)
	require.NoError(t, err)
	require.Equal(t, "https", parts.Scheme)
	require.True(t, strings.HasSuffix(parts.Host, ".blob.core.windows.net"))
	require.NotEmpty(t, parts.ContainerName)
	require.Empty(t, parts.SAS.Encode(), "provide the list-only SAS separately")
	sas := os.Getenv("LIST_BENCH_SAS")
	require.NotEmpty(t, sas, "LIST_BENCH_SAS must contain a short-lived container SAS for listing with tags")
	format := os.Getenv("LIST_BENCH_FORMAT")
	require.Contains(t, []string{"xml", "arrow"}, format)
	duration, err := time.ParseDuration(os.Getenv("LIST_BENCH_DURATION"))
	require.NoError(t, err)
	require.Greater(t, duration, time.Duration(0))
	require.LessOrEqual(t, duration, 10*time.Minute)

	metrics := &liveFlatListMetrics{format: format}
	options := createClientOptions(nil, nil, nil)
	options.PerRetryPolicies = append(options.PerRetryPolicies, metrics, flatListTimingPolicy{})
	client, err := service.NewClientWithNoCredential(parts.Scheme+"://"+parts.Host+"?"+sas,
		&service.ClientOptions{ClientOptions: options})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, "2026-06-06")
	location := common.ELocation.Blob()
	traverser := newBlobTraverser(source, client, ctx, InitResourceTraverserOptions{
		FlatListing: true, Recursive: true, DestResourceType: &location, PreserveBlobTags: true,
	})
	traverser.flatListArrow = format == "arrow"
	traverser.flatListDiagnostics = true
	var count int64
	start := time.Now()
	fmt.Printf("[LiveFlatListStart] format=%s source=%s limit=%s cpus=%d gomaxprocs=%d\n",
		format, source, duration, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	err = traverser.serialList(client.NewContainerClient(parts.ContainerName), parts.ContainerName, parts.BlobName, "",
		noPreProccessor, func(StoredObject) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			count++
			return nil
		}, nil)
	elapsed := time.Since(start)
	timedOut := errors.Is(err, context.DeadlineExceeded) && ctx.Err() == context.DeadlineExceeded
	var memoryStats runtime.MemStats
	runtime.ReadMemStats(&memoryStats)
	result := map[string]any{
		"format": format, "source": source, "elapsed_seconds": elapsed.Seconds(),
		"blobs": count, "blobs_per_second": float64(count) / elapsed.Seconds(),
		"attempts": metrics.attempts, "throttles": metrics.throttles, "successful_responses": metrics.responses,
		"response_bytes": metrics.bytes, "http_attempt_seconds": metrics.attemptTime.Seconds(),
		"heap_bytes": memoryStats.HeapAlloc, "total_alloc_bytes": memoryStats.TotalAlloc,
		"gc_cycles": memoryStats.NumGC, "gc_pause_seconds": float64(memoryStats.PauseTotalNs) / float64(time.Second),
		"deadline_reached": timedOut, "completed_listing": err == nil,
	}
	data, marshalErr := json.Marshal(result)
	require.NoError(t, marshalErr)
	fmt.Printf("[LiveFlatListResult] %s\n", data)
	if !timedOut {
		require.NoError(t, err)
	}
	require.Positive(t, count)
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
			require.Equal(t, tc.want && buildmode.HighPerf(), traverser.flatListArrow)
		})
	}
	t.Run("legacy flat scan is not opted in", func(t *testing.T) {
		t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "true")
		traverser := newBlobTraverser("https://example.blob.core.windows.net/c", nil, context.Background(),
			InitResourceTraverserOptions{Recursive: true, DestResourceType: &blob})
		require.False(t, traverser.parallelListing)
		require.False(t, traverser.flatListArrow)
	})
	t.Run("unknown destination is not opted in", func(t *testing.T) {
		traverser := newBlobTraverser("https://example.blob.core.windows.net/c", nil, context.Background(),
			InitResourceTraverserOptions{FlatListing: true, Recursive: true})
		require.False(t, traverser.flatListArrow)
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
	beforePage func(int, http.ResponseWriter, *http.Request) bool, arrowResponse ...bool) (*flatTestListing, *atomic.Int32) {
	t.Helper()
	if !buildmode.HighPerf() {
		t.Skip("flat enumeration requires the high-performance mover profile")
	}
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			assert.Equal(t, "2025-05-05", r.Header.Get("x-ms-version"))
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		require.Equal(t, "list", r.URL.Query().Get("comp"))
		require.Empty(t, r.URL.Query().Get("delimiter"))
		require.Equal(t, prefix, r.URL.Query().Get("prefix"))
		assert.Equal(t, "2026-06-06", r.Header.Get("x-ms-version"))
		assert.Contains(t, r.Header.Get("Accept"), "application/vnd.apache.arrow.stream")
		assert.Contains(t, r.URL.Query().Get("include"), "metadata")
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
		nextMarker := ""
		if page+1 < len(pages) {
			nextMarker = strconv.Itoa(page + 1)
		}
		w.Header().Set("x-ms-version", "2026-06-06")
		if len(arrowResponse) == 0 || arrowResponse[0] {
			data := flatTestArrowPage(t, pages[page], prefix, nextMarker)
			w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
			_, err := w.Write(data)
			if ctx.Err() == nil {
				assert.NoError(t, err)
			}
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
	client, err := service.NewClientWithNoCredential(endpoint, &service.ClientOptions{
		ClientOptions: azcore.ClientOptions{PerCallPolicies: []policy.Policy{ste.NewVersionPolicy()}},
	})
	require.NoError(t, err)
	location := common.ELocation.Blob()
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, "2025-05-05")
	traverser := newBlobTraverser(endpoint+"/c/"+prefix, client, ctx,
		InitResourceTraverserOptions{FlatListing: true, Recursive: true, DestResourceType: &location})
	return &flatTestListing{traverser}, calls
}

func flatTestArrowPage(t *testing.T, names []string, prefix, nextMarker string) []byte {
	t.Helper()
	md := arrow.NewMetadata([]string{"NextMarker", "NumberOfRecords"}, []string{nextMarker, strconv.Itoa(len(names))})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Name", Type: arrow.BinaryTypes.String},
		{Name: "Last-Modified", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "Etag", Type: arrow.BinaryTypes.String},
		{Name: "Content-Length", Type: arrow.PrimitiveTypes.Int64},
		{Name: "BlobType", Type: arrow.BinaryTypes.String},
		{Name: "Metadata", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
		{Name: "Tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)},
		{Name: "Content-MD5", Type: arrow.BinaryTypes.String},
	}, &md)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, name := range names {
		builder.Field(0).(*array.StringBuilder).Append(prefix + name)
		builder.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Unix(1600000000, 0).UnixMicro()))
		builder.Field(2).(*array.StringBuilder).Append("etag")
		builder.Field(3).(*array.Int64Builder).Append(1)
		builder.Field(4).(*array.StringBuilder).Append("BlockBlob")
		metadata := builder.Field(5).(*array.MapBuilder)
		metadata.Append(true)
		if strings.HasSuffix(name, "/") {
			metadata.KeyBuilder().(*array.StringBuilder).Append("hdi_isfolder")
			metadata.ItemBuilder().(*array.StringBuilder).Append("true")
		} else {
			metadata.KeyBuilder().(*array.StringBuilder).Append("owner")
			metadata.ItemBuilder().(*array.StringBuilder).Append("test")
		}
		tags := builder.Field(6).(*array.MapBuilder)
		tags.Append(true)
		tags.KeyBuilder().(*array.StringBuilder).Append("kind")
		tags.ItemBuilder().(*array.StringBuilder).Append("test")
		builder.Field(7).(*array.StringBuilder).Append("AAAAAAAAAAAAAAAAAAAAAA==")
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	var data bytes.Buffer
	writer := ipc.NewWriter(&data, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	return data.Bytes()
}

func TestFlatPhotonXMLFallback(t *testing.T) {
	listing, calls := newFlatTestListing(t, context.Background(), [][]string{{"a/file"}, {}, {"z/file"}}, "", nil, false)
	var paths []string
	require.NoError(t, listing.Traverse(noPreProccessor, func(obj StoredObject) error {
		paths = append(paths, obj.relativePath)
		return nil
	}, nil))
	require.Equal(t, []string{"a/file", "z/file"}, paths)
	require.EqualValues(t, 3, calls.Load())
	require.Equal(t, "2025-05-05", listing.traverser.ctx.Value(ste.ServiceAPIVersionOverride))
}

func TestFlatPhotonMetadataAndTags(t *testing.T) {
	listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a/"}, {}, {"a/file"}}, "", nil)
	listing.traverser.s2sPreserveSourceTags = true
	var objects []StoredObject
	require.NoError(t, listing.Traverse(noPreProccessor, func(obj StoredObject) error {
		objects = append(objects, obj)
		return nil
	}, nil))
	require.Len(t, objects, 1, "folder stubs must still be filtered using metadata")
	require.Equal(t, "a/file", objects[0].relativePath)
	require.Equal(t, int64(1), objects[0].size)
	require.Equal(t, time.Unix(1600000000, 0).UTC(), objects[0].lastModifiedTime.UTC())
	require.Equal(t, "test", *objects[0].Metadata["owner"])
	require.Equal(t, common.BlobTags{"kind": "test"}, objects[0].blobTags)
	require.Equal(t, make([]byte, 16), objects[0].md5)
}

func TestFlatPhotonInvalidArrowIsFatal(t *testing.T) {
	listing, _ := newFlatTestListing(t, context.Background(), [][]string{{}}, "",
		func(_ int, w http.ResponseWriter, _ *http.Request) bool {
			w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
			fmt.Fprint(w, "invalid Arrow stream")
			return false
		})
	err := listing.Traverse(noPreProccessor, func(StoredObject) error {
		t.Error("invalid response must not emit objects")
		return nil
	}, nil)
	require.ErrorContains(t, err, "Arrow")
}

func TestFlatPhotonRetriesPreserveHeaders(t *testing.T) {
	var attempts atomic.Int32
	listing, calls := newFlatTestListing(t, context.Background(), [][]string{{"nested/file"}}, "",
		func(_ int, w http.ResponseWriter, _ *http.Request) bool {
			if attempts.Add(1) == 1 {
				w.Header().Set("x-ms-error-code", "ServerBusy")
				w.Header().Set("x-ms-retry-after-ms", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `<Error><Code>ServerBusy</Code><Message>busy</Message></Error>`)
				return false
			}
			return true
		})
	var paths []string
	require.NoError(t, listing.Traverse(noPreProccessor, func(obj StoredObject) error {
		paths = append(paths, obj.relativePath)
		return nil
	}, nil))
	require.EqualValues(t, 2, calls.Load())
	require.Equal(t, []string{"nested/file"}, paths)
}

func TestFlatPhotonResponseFormatDiagnostics(t *testing.T) {
	for _, useArrow := range []bool{false, true} {
		t.Run(fmt.Sprintf("arrow=%t", useArrow), func(t *testing.T) {
			listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a/file"}}, "", nil, useArrow)
			logger := &crawlTestLogger{}
			previousLogger := azcopyScanningLogger
			azcopyScanningLogger = logger
			t.Cleanup(func() { azcopyScanningLogger = previousLogger })
			listing.traverser.flatListDiagnostics = true
			require.NoError(t, listing.Traverse(noPreProccessor, func(StoredObject) error { return nil }, nil))
			if useArrow {
				require.Contains(t, logger.text(), `content_type="application/vnd.apache.arrow.stream"`)
				require.Contains(t, logger.text(), "arrow_pages=1 xml_pages=0")
			} else {
				require.Contains(t, logger.text(), `content_type="application/xml"`)
				require.Contains(t, logger.text(), "arrow_pages=0 xml_pages=1")
			}
		})
	}
}

func TestFlatPhotonDoesNotChangeLegacyRequests(t *testing.T) {
	t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "")
	for _, hierarchical := range []bool{false, true} {
		t.Run(fmt.Sprintf("hierarchical=%t", hierarchical), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assert.Equal(t, "2025-05-05", r.Header.Get("x-ms-version"))
				assert.NotContains(t, r.Header.Get("Accept"), "apache.arrow")
				if hierarchical && r.Method == http.MethodGet {
					assert.Equal(t, "/", r.URL.Query().Get("delimiter"))
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, `<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`)
			}))
			defer server.Close()
			endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
			client, err := service.NewClientWithNoCredential(endpoint, &service.ClientOptions{
				ClientOptions: azcore.ClientOptions{PerCallPolicies: []policy.Policy{ste.NewVersionPolicy()}},
			})
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, "2025-05-05")
			location := common.ELocation.Blob()
			traverser := newBlobTraverser(endpoint+"/c", client, ctx,
				InitResourceTraverserOptions{Recursive: true, DestResourceType: &location})
			require.False(t, traverser.flatListArrow)
			traverser.parallelListing = hierarchical
			require.NoError(t, traverser.Traverse(noPreProccessor, func(StoredObject) error { return nil }, nil))
			require.Positive(t, calls.Load())
		})
	}
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
