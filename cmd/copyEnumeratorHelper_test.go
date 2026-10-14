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
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
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
		})
	}
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
