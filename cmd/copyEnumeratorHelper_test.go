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
	"crypto/tls"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
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

type flatListTestTransport func(*http.Request) (*http.Response, error)

func (f flatListTestTransport) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestFlatListDedicatedPoolRouting(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, "1")
	captureCrawlLogs(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "response")
	}))
	defer server.Close()
	shared := server.Client()
	pool := &flatListPoolTransport{shared: shared}
	for _, listing := range []bool{false, true, true, false} {
		var reused, fresh atomic.Int32
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				if info.Reused {
					reused.Add(1)
				} else {
					fresh.Add(1)
				}
			},
		})
		existing := pool.listing
		if listing {
			ctx = context.WithValue(ctx, flatListPoolKey{}, struct{}{})
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		resp, err := pool.Do(req)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		if existing == nil {
			require.EqualValues(t, 1, fresh.Load(), "first listing must not reuse the transfer connection")
		} else {
			require.EqualValues(t, 1, reused.Load(), "each path must retain and reuse its own pool")
		}
		if !listing && existing == nil {
			require.Nil(t, pool.listing, "non-listing requests must not create a pool")
		}
	}
	require.NotSame(t, shared, pool.listing)
	isolated := pool.listing.Transport.(*http.Transport)
	defer isolated.CloseIdleConnections()
	require.Equal(t, 4, isolated.MaxConnsPerHost)
	require.Equal(t, 4, isolated.MaxIdleConnsPerHost)
	require.Equal(t, 4, isolated.MaxIdleConns)
	require.Zero(t, shared.Transport.(*http.Transport).MaxConnsPerHost)
}

func TestFlatListDedicatedPoolSettingsAndGate(t *testing.T) {
	t.Setenv("MOVER_FLAT_LIST_DIAGNOSTICS", "false")
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "false")
	options := createClientOptions(nil, nil, nil)
	pool, enabled := options.Transport.(*flatListPoolTransport)
	require.Equal(t, buildmode.HighPerf(), enabled)
	if enabled {
		require.Same(t, common.GetGlobalHTTPClient(nil), pool.shared)
		require.Nil(t, pool.listing)
		client, err := common.NewIsolatedHTTPClient(pool.shared, 4)
		require.NoError(t, err)
		defer client.CloseIdleConnections()
	}
	source := &http.Transport{
		DisableCompression:  true,
		IdleConnTimeout:     180 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ReadBufferSize:      65536, WriteBufferSize: 65536,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		MaxConnsPerHost: 1024,
	}
	shared := &http.Client{Transport: source, Timeout: time.Minute}
	client, err := common.NewIsolatedHTTPClient(shared, 4)
	require.NoError(t, err)
	cloned := client.Transport.(*http.Transport)
	require.NotSame(t, source, cloned)
	require.Equal(t, shared.Timeout, client.Timeout)
	require.Equal(t, source.DisableCompression, cloned.DisableCompression)
	require.Equal(t, source.DisableKeepAlives, cloned.DisableKeepAlives)
	require.Equal(t, source.IdleConnTimeout, cloned.IdleConnTimeout)
	require.Equal(t, source.TLSHandshakeTimeout, cloned.TLSHandshakeTimeout)
	require.Equal(t, source.ReadBufferSize, cloned.ReadBufferSize)
	require.Equal(t, source.WriteBufferSize, cloned.WriteBufferSize)
	require.Equal(t, source.TLSClientConfig.MinVersion, cloned.TLSClientConfig.MinVersion)
	require.Equal(t, 1024, source.MaxConnsPerHost)
	require.Equal(t, 4, cloned.MaxConnsPerHost)
}

func TestFlatListDedicatedPoolError(t *testing.T) {
	pool := &flatListPoolTransport{shared: &http.Client{}}
	ctx := context.WithValue(context.Background(), flatListPoolKey{}, struct{}{})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		resp, err := pool.Do(req)
		require.Nil(t, resp)
		require.ErrorContains(t, err, "cannot isolate shared HTTP transport")
	}
}

func TestFlatListDedicatedPoolWithoutDiagnostics(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, "1")
	for _, legacyFlag := range []string{"", "false", "true"} {
		t.Run(fmt.Sprintf("legacyFlag=%q", legacyFlag), func(t *testing.T) {
			t.Setenv("MOVER_FLAT_LIST_DEDICATED_POOL", legacyFlag)
			t.Setenv("MOVER_FLAT_LIST_DIAGNOSTICS", "false")
			t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "false")
			logger := captureCrawlLogs(t)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, `<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`)
			}))
			defer server.Close()
			options := createClientOptions(nil, nil, nil)
			pool, enabled := options.Transport.(*flatListPoolTransport)
			require.Equal(t, buildmode.HighPerf(), enabled)
			if enabled {
				pool.shared = server.Client()
			} else {
				options.Transport = server.Client()
			}
			client, err := container.NewClientWithNoCredential(server.URL+"/c", &container.ClientOptions{
				ClientOptions: options,
			})
			require.NoError(t, err)
			traverser := &blobTraverser{
				ctx: context.Background(), recursive: true,
				flatListArrow: buildmode.HighPerf(),
			}
			err = traverser.serialList(client, "c", "", "", noPreProccessor, func(StoredObject) error {
				t.Error("empty listing must not deliver an object")
				return nil
			}, nil)
			require.NoError(t, err)
			if enabled {
				require.NotNil(t, pool.listing, "flat requests must use the pool without a timing context")
				defer pool.listing.CloseIdleConnections()
				require.Contains(t, logger.text(), "[FlatListPrefetch] enabled=true")
			} else {
				require.NotContains(t, logger.text(), "[FlatListPrefetch]")
			}
			for _, tag := range []string{"[FlatListHTTP]", "[FlatListPage]", "[FlatListTiming]"} {
				require.NotContains(t, logger.text(), tag)
			}
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

type crawlStatsTestLifecycle struct {
	common.LifecycleMgr
	crawlTestLogger
}

func (l *crawlStatsTestLifecycle) Info(message string) {
	l.Log(common.LogInfo, message)
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
			console := &crawlStatsTestLifecycle{}
			glcm = console
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stats := &parallel.CrawlStats{ActiveWorkers: 7, QueuedDirs: 42, PeakQueuedDirs: 123, MaxQueueDirectories: 50_000_000}
				stop := startBlobCrawlStats(ctx, stats, 100, true)
				defer stop()
				synctest.Wait()
				time.Sleep(59 * time.Second)
				synctest.Wait()
				assert.NotContains(t, console.text(), "[CrawlStats]")
				time.Sleep(time.Second)
				synctest.Wait()
				if buildmode.HighPerf() {
					assert.Contains(t, console.text(), "[CrawlConfig] mode=blob-parallel, crawlParallelism=100, maxQueueDirectories=50000000, randomDequeue=true")
					assert.Contains(t, console.text(), "[CrawlStats] mode=blob-parallel, activeWorkers=7/100, queuedDirs=42")
					assert.Contains(t, console.text(), "peakQueuedDirs=123, final=false")
				}
				if cancelParent {
					cancel()
					synctest.Wait()
				}
				stop()
				if buildmode.HighPerf() {
					assert.Contains(t, console.text(), "peakQueuedDirs=123, final=true")
				} else {
					assert.Empty(t, console.text())
				}
				before := console.text()
				time.Sleep(60 * time.Second)
				synctest.Wait()
				assert.Equal(t, before, console.text(), "monitor must stop when enumeration exits")
				assert.Empty(t, logger.text(), "crawl diagnostics must not write to the scanning log")
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
				console := &crawlStatsTestLifecycle{}
				glcm = console
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
					assert.Contains(t, console.text(), "crawlParallelism=100, maxQueueDirectories="+want)
					assert.Contains(t, console.text(), "activeWorkers=0/100, queuedDirs=0")
					assert.Contains(t, console.text(), "final=true")
				} else {
					assert.NotContains(t, console.text(), "[CrawlConfig]")
					assert.NotContains(t, console.text(), "[CrawlStats]")
				}
				assert.NotContains(t, logger.text(), "[CrawlConfig]")
				assert.NotContains(t, logger.text(), "[CrawlStats]")
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
			require.Equal(t, !tc.want, traverser.parallelListing)
			require.Equal(t, tc.want, traverser.flatListingEnabled)
			require.Equal(t, tc.want, traverser.flatListArrow)
		})
	}
	t.Run("legacy flat scan is not opted in", func(t *testing.T) {
		t.Setenv("AZCOPY_DISABLE_HIERARCHICAL_SCAN", "true")
		traverser := newBlobTraverser("https://example.blob.core.windows.net/c", nil, context.Background(),
			InitResourceTraverserOptions{Recursive: true, DestResourceType: &blob})
		require.False(t, traverser.parallelListing)
		require.False(t, traverser.flatListingEnabled)
		require.False(t, traverser.flatListArrow)
	})
	t.Run("unknown destination is not opted in", func(t *testing.T) {
		traverser := newBlobTraverser("https://example.blob.core.windows.net/c", nil, context.Background(),
			InitResourceTraverserOptions{FlatListing: true, Recursive: true})
		require.False(t, traverser.flatListingEnabled)
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
	// These fixtures describe one serial stream, without range-aware server responses.
	t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, "1")
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
	options := azcore.ClientOptions{PerCallPolicies: []policy.Policy{ste.NewVersionPolicy()}}
	client, err := service.NewClientWithNoCredential(endpoint, &service.ClientOptions{ClientOptions: options})
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

func TestFlatPhotonResponseFormatLogging(t *testing.T) {
	for _, useArrow := range []bool{false, true} {
		t.Run(fmt.Sprintf("arrow=%t", useArrow), func(t *testing.T) {
			listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a/file"}}, "", nil, useArrow)
			logger := &crawlTestLogger{}
			previousLogger := azcopyScanningLogger
			azcopyScanningLogger = logger
			t.Cleanup(func() { azcopyScanningLogger = previousLogger })
			require.NoError(t, listing.Traverse(noPreProccessor, func(StoredObject) error { return nil }, nil))
			require.NotContains(t, logger.text(), "[FlatListPage]")
			require.NotContains(t, logger.text(), "[FlatListHTTP]")
			if useArrow {
				require.Contains(t, logger.text(), `content_type="application/vnd.apache.arrow.stream"`)
			} else {
				require.Contains(t, logger.text(), `content_type="application/xml"`)
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

func newFlatPrefetchTestClient(t *testing.T, transport policy.Transporter) *container.Client {
	t.Helper()
	t.Setenv(enum.EEnvironmentVariable.MoverFlatListParallelism().Name, "1")
	client, err := container.NewClientWithNoCredential("https://example.blob.core.windows.net/c", &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: transport,
		},
	})
	require.NoError(t, err)
	return client
}

func TestFlatListPrefetchOverlapAndBound(t *testing.T) {
	for _, legacyFlag := range []string{"", "false", "true"} {
		for _, eligible := range []bool{false, true} {
			for _, slowConsumer := range []bool{false, true} {
				t.Run(fmt.Sprintf("legacyFlag=%q/eligible=%t/slowConsumer=%t", legacyFlag, eligible, slowConsumer), func(t *testing.T) {
					t.Setenv("MOVER_FLAT_LIST_PREFETCH", legacyFlag)
					captureCrawlLogs(t)
					contentType := "application/xml"
					if eligible {
						contentType = "application/vnd.apache.arrow.stream"
					}
					bodies := make([][]byte, 4)
					for i := range bodies {
						marker := ""
						if i+1 < len(bodies) {
							marker = strconv.Itoa(i + 1)
						}
						if eligible {
							bodies[i] = flatTestArrowPage(t, []string{strconv.Itoa(i)}, "", marker)
						} else {
							bodies[i] = []byte(fmt.Sprintf(`<EnumerationResults><Blobs><Blob><Name>%d</Name><Properties><Last-Modified>Sun, 13 Sep 2020 12:26:40 GMT</Last-Modified><Etag>etag</Etag><Content-Length>1</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob></Blobs><NextMarker>%s</NextMarker></EnumerationResults>`, i, marker))
						}
					}
					synctest.Test(t, func(t *testing.T) {
						var calls, active atomic.Int32
						client := newFlatPrefetchTestClient(t, flatListTestTransport(func(req *http.Request) (*http.Response, error) {
							assert.EqualValues(t, 1, active.Add(1), "pager requests must remain serial")
							defer active.Add(-1)
							i := int(calls.Add(1)) - 1
							if i >= len(bodies) {
								return nil, errors.New("prefetch read past the final page")
							}
							marker := ""
							if i > 0 {
								marker = strconv.Itoa(i)
							}
							assert.Equal(t, marker, req.URL.Query().Get("marker"))
							time.Sleep(100 * time.Millisecond)
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     http.Header{"Content-Type": {contentType}},
								Body:       io.NopCloser(bytes.NewReader(bodies[i])),
								Request:    req,
							}, nil
						}))
						traverser := &blobTraverser{ctx: context.Background(), recursive: true, flatListArrow: eligible}
						prefetch := eligible
						var paths []string
						start := time.Now()
						err := traverser.serialList(client, "c", "", "", noPreProccessor, func(obj StoredObject) error {
							paths = append(paths, obj.relativePath)
							delay := 80 * time.Millisecond
							if slowConsumer && len(paths) == 1 {
								delay = 500 * time.Millisecond
							}
							time.Sleep(delay)
							limit := len(paths)
							if prefetch {
								limit++
							}
							require.LessOrEqual(t, int(calls.Load()), limit, "including in-flight work, at most one page may be ahead")
							if len(paths) == 1 {
								require.EqualValues(t, limit, calls.Load(), "next request must overlap current processing only when enabled")
							}
							return nil
						}, nil)
						require.NoError(t, err)
						require.Equal(t, []string{"0", "1", "2", "3"}, paths)
						require.EqualValues(t, 4, calls.Load())
						want := 720 * time.Millisecond
						if prefetch {
							want = 480 * time.Millisecond
						}
						if slowConsumer {
							want = 1140 * time.Millisecond
							if prefetch {
								want = 880 * time.Millisecond
							}
						}
						require.Equal(t, want, time.Since(start), "fake-clock measurement must demonstrate actual overlap")
						require.Zero(t, active.Load(), "no request may outlive enumeration")
					})
				})
			}
		}
	}
}

func TestFlatListPrefetchDiscardsReadyPage(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelParent=%t", cancelParent), func(t *testing.T) {
			captureCrawlLogs(t)
			body := flatTestArrowPage(t, []string{"private-file"}, "", "more")
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var calls atomic.Int32
				client := newFlatPrefetchTestClient(t, flatListTestTransport(func(req *http.Request) (*http.Response, error) {
					calls.Add(1)
					time.Sleep(time.Millisecond)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": {"application/vnd.apache.arrow.stream"}},
						Body:       io.NopCloser(bytes.NewReader(body)),
						Request:    req,
					}, nil
				}))
				traverser := &blobTraverser{ctx: ctx, recursive: true, flatListArrow: true}
				wantErr := errors.New("processor stopped")
				processed := 0
				err := traverser.serialList(client, "c", "", "", noPreProccessor, func(StoredObject) error {
					processed++
					time.Sleep(10 * time.Millisecond)
					require.EqualValues(t, 2, calls.Load())
					if cancelParent {
						cancel()
						return nil
					}
					return wantErr
				}, nil)
				if cancelParent {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, wantErr)
				}
				require.Equal(t, 1, processed)
				require.EqualValues(t, 2, calls.Load(), "cleanup must not fetch another page")
			})
		})
	}
}

func TestFlatListPrefetchCancelsInFlightBody(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelParent=%t", cancelParent), func(t *testing.T) {
			captureCrawlLogs(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			started, canceled := make(chan struct{}), make(chan struct{})
			listing, calls := newFlatTestListing(t, ctx, [][]string{{"a", "b"}, {"c"}}, "",
				func(page int, w http.ResponseWriter, r *http.Request) bool {
					if page != 1 {
						return true
					}
					w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					close(started)
					<-r.Context().Done()
					close(canceled)
					return false
				})
			wantErr := errors.New("stop scheduling")
			var paths []string
			err := listing.Traverse(noPreProccessor, func(obj StoredObject) error {
				paths = append(paths, obj.relativePath)
				select {
				case <-started:
				case <-ctx.Done():
					return ctx.Err()
				}
				if cancelParent {
					cancel()
					return nil
				}
				return wantErr
			}, nil)
			if cancelParent {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, wantErr, "prefetch cancellation must not replace the processor error")
			}
			select {
			case <-canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("prefetch HTTP request outlived traversal")
			}
			require.Equal(t, []string{"a"}, paths)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestFlatListPrefetchPageErrorAfterEmptyPage(t *testing.T) {
	for _, arrowResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("arrow=%t", arrowResponse), func(t *testing.T) {
			listing, calls := newFlatTestListing(t, context.Background(), [][]string{{"a", "b"}, {}, {"c"}, {}}, "",
				func(page int, w http.ResponseWriter, _ *http.Request) bool {
					if page != 3 {
						return true
					}
					w.Header().Set("x-ms-error-code", "AuthorizationPermissionMismatch")
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `<Error><Code>AuthorizationPermissionMismatch</Code><Message>denied</Message></Error>`)
					return false
				}, arrowResponse)
			var paths []string
			err := listing.Traverse(noPreProccessor, func(obj StoredObject) error {
				paths = append(paths, obj.relativePath)
				return nil
			}, nil)
			var responseErr *azcore.ResponseError
			require.ErrorAs(t, err, &responseErr)
			require.Equal(t, http.StatusForbidden, responseErr.StatusCode)
			require.Equal(t, []string{"a", "b", "c"}, paths)
			require.EqualValues(t, 4, calls.Load())
		})
	}
}

func TestFlatListPrefetchAlreadyCanceled(t *testing.T) {
	captureCrawlLogs(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := newFlatPrefetchTestClient(t, flatListTestTransport(func(*http.Request) (*http.Response, error) {
		t.Error("canceled traversal must not start a request")
		return nil, ctx.Err()
	}))
	traverser := &blobTraverser{ctx: ctx, recursive: true, flatListArrow: true}
	err := traverser.serialList(client, "c", "", "", noPreProccessor, func(StoredObject) error {
		t.Error("canceled traversal must not process a blob")
		return nil
	}, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestFlatListPrefetchExactOutputAndMetadataLifetime(t *testing.T) {
	pages := [][]string{{}, {"__AZURE_STORAGE_MOVER_MARKER"}, {}}
	var want []string
	for _, size := range []int{137, 0, 5000, 61, 0} {
		page := make([]string, size)
		for i := range page {
			page[i] = fmt.Sprintf("dir-%06d/nested/file %% + #.txt", len(want))
			want = append(want, page[i])
		}
		pages = append(pages, page)
	}
	pages = append(pages, []string{"folder-stub/"}, nil)
	for _, arrowResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("arrow=%t", arrowResponse), func(t *testing.T) {
			var referenceObjects []StoredObject
			for _, diagnostics := range []bool{false, true} {
				t.Run(fmt.Sprintf("diagnostics=%t", diagnostics), func(t *testing.T) {
					t.Setenv("MOVER_FLAT_LIST_DIAGNOSTICS", strconv.FormatBool(diagnostics))
					t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", strconv.FormatBool(diagnostics))
					captureCrawlLogs(t)
					listing, calls := newFlatTestListing(t, context.Background(), pages, "root/", nil, arrowResponse)
					listing.traverser.s2sPreserveSourceTags = true
					scanned := 0
					listing.traverser.incrementEnumerationCounter = func(common.EntityType) { scanned++ }
					var objects []StoredObject
					err := listing.Traverse(noPreProccessor, func(obj StoredObject) error {
						objects = append(objects, obj)
						return nil
					}, buildRegexFilters([]string{".*__AZURE_STORAGE_MOVER_MARKER.*", "folder-stub/.*"}, false))
					require.NoError(t, err)
					require.EqualValues(t, len(pages), calls.Load())
					var got []string
					runtime.GC()
					for _, obj := range objects {
						got = append(got, obj.relativePath)
						require.EqualValues(t, 1, obj.size)
						if arrowResponse {
							require.Equal(t, "test", *obj.Metadata["owner"])
							require.Equal(t, common.BlobTags{"kind": "test"}, obj.blobTags)
							require.Equal(t, make([]byte, 16), obj.md5)
						}
					}
					require.Equal(t, want, got, "every surviving key must be delivered exactly once and in order")
					wantScanned := len(want) + 2 // XML fixture has no folder metadata, so its stub reaches filters.
					if arrowResponse {
						wantScanned--
					}
					require.Equal(t, wantScanned, scanned)
					if diagnostics {
						require.Equal(t, referenceObjects, objects, "diagnostics must not change prefetched objects")
					} else {
						referenceObjects = objects
					}
				})
			}
		})
	}
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
