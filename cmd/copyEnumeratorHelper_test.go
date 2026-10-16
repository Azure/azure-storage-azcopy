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
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
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
	format                         string
	attempts, throttles, responses int64
	bytes                          int64
	attemptTime                    time.Duration
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
			w.Header().Set("x-ms-retry-after-ms", "25")
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
			Retry:            policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Second},
			PerRetryPolicies: []policy.Policy{flatListTimingPolicy{}},
			Transport:        flatListDiagnosticTransport{inner: server.Client()},
		},
	})
	require.NoError(t, err)
	timing := flatListPageTiming{details: true}
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
	require.GreaterOrEqual(t, timing.retryGap, 25*time.Millisecond)
	require.Len(t, timing.http, 2)
	require.Equal(t, http.StatusServiceUnavailable, timing.http[0].snapshot().status)
	require.Equal(t, http.StatusOK, timing.http[1].snapshot().status)
	require.Positive(t, timing.http[0].snapshot().bodyBytes)
	require.Positive(t, timing.http[1].snapshot().bodyBytes)
}

func TestFlatListHTTPPhasesAndReuse(t *testing.T) {
	for _, format := range []string{"xml", "arrow", "xml-fallback"} {
		t.Run(format, func(t *testing.T) {
			body := []byte(`<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`)
			contentType := "application/xml"
			requestFormat := container.StorageResponseFormatAuto
			if format != "xml" {
				requestFormat = container.StorageResponseFormatArrow
			}
			if format == "arrow" {
				body = flatTestArrowPage(t, nil, "", "")
				contentType = "application/vnd.apache.arrow.stream"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(25 * time.Millisecond)
				w.Header().Set("Content-Type", contentType)
				w.Header().Set("x-ms-request-id", "request-for-phase-test")
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				time.Sleep(25 * time.Millisecond)
				_, _ = w.Write(body[:len(body)/2])
				w.(http.Flusher).Flush()
				time.Sleep(25 * time.Millisecond)
				_, _ = w.Write(body[len(body)/2:])
			}))
			defer server.Close()
			client, err := container.NewClientWithNoCredential(server.URL+"/c", &container.ClientOptions{
				ClientOptions: azcore.ClientOptions{
					Transport:        flatListDiagnosticTransport{inner: server.Client()},
					PerRetryPolicies: []policy.Policy{flatListTimingPolicy{}},
					Retry:            policy.RetryOptions{MaxRetries: -1},
				},
			})
			require.NoError(t, err)
			var priorTraceCalls atomic.Int32
			for page := 0; page < 2; page++ {
				p := flatListPageTiming{details: true}
				ctx := context.WithValue(context.Background(), flatListTimingKey{}, &p)
				ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
					GotConn: func(httptrace.GotConnInfo) { priorTraceCalls.Add(1) },
				})
				start := time.Now()
				_, err := client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{ResponseFormat: requestFormat}).NextPage(ctx)
				elapsed := time.Since(start)
				require.NoError(t, err)
				require.Len(t, p.http, 1)
				v := p.http[0].snapshot()
				require.Equal(t, int64(len(body)), v.bodyBytes)
				require.Equal(t, contentType, v.contentType)
				require.Equal(t, "request-for-phase-test", v.requestID)
				require.Equal(t, http.StatusOK, v.status)
				require.Equal(t, "HTTP/1.1", v.proto)
				require.GreaterOrEqual(t, v.ttfb, 20*time.Millisecond)
				require.GreaterOrEqual(t, v.bodyRead, 40*time.Millisecond)
				require.LessOrEqual(t, v.transportTime+v.bodyRead, elapsed)
				require.GreaterOrEqual(t, v.bodyWall, v.bodyRead)
				require.True(t, v.bodyClosed)
				require.False(t, v.transportFailed || v.bodyFailed)
				require.Equal(t, page, v.reusedConns)
				require.Equal(t, 1-page, v.newConns)
				if format == "xml" {
					require.GreaterOrEqual(t, p.attemptTime, v.transportTime+v.bodyRead)
				} else {
					require.GreaterOrEqual(t, elapsed-p.attemptTime, v.bodyRead,
						"Arrow requests stream both Arrow and XML-fallback bodies outside the pipeline")
				}
			}
			require.EqualValues(t, 2, priorTraceCalls.Load(), "existing trace callbacks must still run")
		})
	}
}

func TestFlatListHTTPCancelBody(t *testing.T) {
	for _, format := range []string{"xml", "arrow", "xml-fallback"} {
		t.Run(format, func(t *testing.T) {
			contentType := "application/xml"
			body := []byte(`<EnumerationResults>`)
			requestFormat := container.StorageResponseFormatAuto
			if format != "xml" {
				requestFormat = container.StorageResponseFormatArrow
			}
			if format == "arrow" {
				contentType = "application/vnd.apache.arrow.stream"
				body = flatTestArrowPage(t, []string{"file"}, "", "")[:4]
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", contentType)
				_, _ = w.Write(body)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer server.Close()
			client, err := container.NewClientWithNoCredential(server.URL+"/c", &container.ClientOptions{
				ClientOptions: azcore.ClientOptions{
					Transport:        flatListDiagnosticTransport{inner: server.Client()},
					PerRetryPolicies: []policy.Policy{flatListTimingPolicy{}},
					Retry:            policy.RetryOptions{MaxRetries: -1},
				},
			})
			require.NoError(t, err)
			p := flatListPageTiming{details: true}
			ctx = context.WithValue(ctx, flatListTimingKey{}, &p)
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				GotFirstResponseByte: func() { time.AfterFunc(25*time.Millisecond, cancel) },
			})
			_, err = client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{ResponseFormat: requestFormat}).NextPage(ctx)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if format == "arrow" {
				// The SDK's Arrow parser formats the cause without preserving its error chain.
				require.ErrorContains(t, err, context.Canceled.Error())
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Len(t, p.http, 1)
			v := p.http[0].snapshot()
			require.Equal(t, http.StatusOK, v.status)
			require.Positive(t, v.bodyBytes)
			require.True(t, v.bodyFailed)
			require.True(t, v.bodyClosed)
			require.False(t, v.bodyEOF)
		})
	}
}

func TestFlatListHTTPBodyPreservesReadError(t *testing.T) {
	reader, writer := io.Pipe()
	require.NoError(t, writer.CloseWithError(context.Canceled))
	m := &flatListHTTPMetrics{headersAt: time.Now()}
	body := &flatListDiagnosticBody{ReadCloser: reader, metrics: m}
	n, err := body.Read(make([]byte, 1))
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, body.Close())
	v := m.snapshot()
	require.True(t, v.bodyFailed)
	require.True(t, v.bodyClosed)
	require.False(t, v.bodyEOF)
}

type flatListTestTransport func(*http.Request) (*http.Response, error)

func (f flatListTestTransport) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestFlatListHTTPTransportErrorAndPassthrough(t *testing.T) {
	sentinel := errors.New("transport unavailable")
	req, err := http.NewRequest(http.MethodGet, "https://example.com/c?sig=not-for-logs", nil)
	require.NoError(t, err)
	original := req
	transport := flatListDiagnosticTransport{inner: flatListTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Context().Value(flatListHTTPKey{}) == nil {
			require.Same(t, original, r, "uninstrumented requests must be passed through unchanged")
			require.Nil(t, httptrace.ContextClientTrace(r.Context()))
		}
		return nil, sentinel
	})}
	_, err = transport.Do(req)
	require.ErrorIs(t, err, sentinel)
	m := &flatListHTTPMetrics{}
	req = req.WithContext(context.WithValue(req.Context(), flatListHTTPKey{}, m))
	_, err = transport.Do(req)
	require.ErrorIs(t, err, sentinel)
	v := m.snapshot()
	require.True(t, v.transportFailed)
	require.Zero(t, v.status)
	require.Zero(t, v.bodyBytes)
}

func TestFlatListHTTPTraceConcurrentCallbacks(t *testing.T) {
	m := &flatListHTTPMetrics{}
	trace := m.trace()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			trace.GetConn("")
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			trace.DNSStart(httptrace.DNSStartInfo{})
			trace.DNSDone(httptrace.DNSDoneInfo{})
			trace.ConnectStart("tcp", "test")
			trace.ConnectDone("tcp", "test", nil)
			trace.TLSHandshakeStart()
			trace.TLSHandshakeDone(tls.ConnectionState{}, nil)
			trace.WroteRequest(httptrace.WroteRequestInfo{})
			trace.GotFirstResponseByte()
			_ = m.snapshot()
		})
	}
	wg.Wait()
	require.Equal(t, 20, m.snapshot().reusedConns)
}

func TestFlatListPageDiagnosticsOptions(t *testing.T) {
	t.Setenv("MOVER_FLAT_LIST_DIAGNOSTICS", "false")
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "false")
	original := createClientOptions(nil, nil, nil)
	if buildmode.HighPerf() {
		require.Same(t, common.GetGlobalHTTPClient(nil), original.Transport.(*flatListPoolTransport).shared)
	} else {
		require.Same(t, common.GetGlobalHTTPClient(nil), original.Transport)
	}
	require.False(t, flatListDiagnosticsEnabled())
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "true")
	options := createClientOptions(nil, nil, nil)
	require.Equal(t, buildmode.HighPerf(), flatListDiagnosticsEnabled())
	if buildmode.HighPerf() {
		wrapped, ok := options.Transport.(flatListDiagnosticTransport)
		require.True(t, ok)
		require.Same(t, common.GetGlobalHTTPClient(nil), wrapped.inner.(*flatListPoolTransport).shared)
	} else {
		require.Same(t, original.Transport, options.Transport)
	}
	require.Equal(t, original.Retry.MaxRetries, options.Retry.MaxRetries)
	require.Equal(t, original.Retry.RetryDelay, options.Retry.RetryDelay)
}

func TestFlatListDedicatedPoolRouting(t *testing.T) {
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
	require.False(t, flatListDiagnosticsEnabled())
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
				flatListArrow: buildmode.HighPerf(), flatListDiagnostics: flatListDiagnosticsEnabled(),
			}
			require.False(t, traverser.flatListDiagnostics)
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

func TestFlatListPageDiagnosticsLogs(t *testing.T) {
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "true")
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("processorFailure=%t", fail), func(t *testing.T) {
			logger := captureCrawlLogs(t)
			listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"private-file"}, {"other-private-file"}},
				"private-prefix/", nil)
			err := listing.Traverse(noPreProccessor, func(StoredObject) error {
				time.Sleep(5 * time.Millisecond)
				if fail {
					return errors.New("private failure text must not be logged in metrics")
				}
				return nil
			}, nil)
			text := logger.text()
			if fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if !flatListPageDiagnosticsEnabled() {
				for _, tag := range []string{"[FlatListHTTP]", "[FlatListPage]", "[FlatListTiming]"} {
					require.NotContains(t, text, tag)
				}
				return
			}
			if fail {
				require.Contains(t, text, `stage="processing" failed=true`)
				require.Equal(t, 1, strings.Count(text, "[FlatListPage]")-strings.Count(text, `stage="prefetch-discarded"`))
			} else {
				require.Contains(t, text, `stage="complete" failed=false`)
				require.Equal(t, 2, strings.Count(text, "[FlatListPage]"))
			}
			require.Contains(t, text, "blobs=1 attempts=1 observed_http_attempts=1")
			require.Contains(t, text, "detail_log_ms=")
			require.Contains(t, text, "[FlatListHTTP]")
			for _, line := range strings.Split(text, "\n") {
				if strings.Contains(line, "[FlatList") {
					require.NotContains(t, line, "private")
				}
				if !strings.Contains(line, "[FlatListPage]") || strings.Contains(line, `stage="prefetch-discarded"`) {
					continue
				}
				values := make(map[string]float64)
				for _, field := range strings.Fields(line) {
					key, value, ok := strings.Cut(field, "=")
					if ok && strings.HasSuffix(key, "_ms") {
						values[key], err = strconv.ParseFloat(value, 64)
						require.NoError(t, err)
					}
				}
				require.GreaterOrEqual(t, values["processor_ms"], 5.0)
				require.GreaterOrEqual(t, values["processing_ms"], values["processor_ms"])
				require.InDelta(t, values["next_ms"], values["transport_headers_ms"]+values["body_read_ms"]+
					values["between_attempt_ms"]+values["next_other_ms"], 0.003)
			}
		})
	}
	t.Run("nextFailure", func(t *testing.T) {
		logger := captureCrawlLogs(t)
		listing, _ := newFlatTestListing(t, context.Background(), [][]string{{}}, "",
			func(_ int, w http.ResponseWriter, _ *http.Request) bool {
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				fmt.Fprint(w, "invalid Arrow")
				return false
			})
		err := listing.Traverse(noPreProccessor, func(StoredObject) error {
			t.Error("failed decoding must not schedule transfers")
			return nil
		}, nil)
		require.Error(t, err)
		if flatListPageDiagnosticsEnabled() {
			require.Contains(t, logger.text(), `stage="next" failed=true blobs=0 attempts=1 observed_http_attempts=1`)
		} else {
			require.NotContains(t, logger.text(), "[FlatListPage]")
		}
	})
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
			require.Equal(t, !tc.want, traverser.parallelListing)
			require.Equal(t, tc.want, traverser.flatListArrow)
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
	if flatListPageDiagnosticsEnabled() {
		options.Transport = flatListDiagnosticTransport{inner: server.Client()}
		options.PerRetryPolicies = []policy.Policy{flatListTimingPolicy{}}
	}
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

func TestFlatPhotonResponseFormatDiagnostics(t *testing.T) {
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "false")
	for _, useArrow := range []bool{false, true} {
		t.Run(fmt.Sprintf("arrow=%t", useArrow), func(t *testing.T) {
			listing, _ := newFlatTestListing(t, context.Background(), [][]string{{"a/file"}}, "", nil, useArrow)
			logger := &crawlTestLogger{}
			previousLogger := azcopyScanningLogger
			azcopyScanningLogger = logger
			t.Cleanup(func() { azcopyScanningLogger = previousLogger })
			listing.traverser.flatListDiagnostics = true
			require.NoError(t, listing.Traverse(noPreProccessor, func(StoredObject) error { return nil }, nil))
			require.NotContains(t, logger.text(), "[FlatListPage]")
			require.NotContains(t, logger.text(), "[FlatListHTTP]")
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

func newFlatPrefetchTestClient(t *testing.T, transport policy.Transporter) *container.Client {
	t.Helper()
	client, err := container.NewClientWithNoCredential("https://example.blob.core.windows.net/c", &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport:        flatListDiagnosticTransport{inner: transport},
			PerRetryPolicies: []policy.Policy{flatListTimingPolicy{}},
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
					t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "true")
					logger := captureCrawlLogs(t)
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
						traverser := &blobTraverser{ctx: context.Background(), recursive: true, flatListArrow: eligible, flatListDiagnostics: true}
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
						processing := 320 * time.Millisecond
						if slowConsumer {
							processing = 740 * time.Millisecond
						}
						require.Contains(t, logger.text(), fmt.Sprintf("elapsed_ms=%d next_ms=400", want.Milliseconds()))
						require.Contains(t, logger.text(), fmt.Sprintf("prefetch=%t page_wait_ms=%d discarded_pages=0",
							prefetch, (want-processing).Milliseconds()))
					})
				})
			}
		}
	}
}

func TestFlatListPrefetchDiscardsReadyPage(t *testing.T) {
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "true")
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelParent=%t", cancelParent), func(t *testing.T) {
			logger := captureCrawlLogs(t)
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
				traverser := &blobTraverser{ctx: ctx, recursive: true, flatListArrow: true, flatListDiagnostics: true}
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
				if flatListPageDiagnosticsEnabled() {
					require.Contains(t, logger.text(), `stage="prefetch-discarded"`)
					require.Equal(t, 2, strings.Count(logger.text(), "[FlatListHTTP]"))
				}
				require.Contains(t, logger.text(), "attempts=2")
				require.Contains(t, logger.text(), "discarded_pages=1")
			})
		})
	}
}

func TestFlatListPrefetchCancelsInFlightBody(t *testing.T) {
	t.Setenv("MOVER_FLAT_LIST_PAGE_DIAGNOSTICS", "true")
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelParent=%t", cancelParent), func(t *testing.T) {
			logger := captureCrawlLogs(t)
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
			if flatListPageDiagnosticsEnabled() {
				require.Contains(t, logger.text(), `stage="prefetch-discarded" failed=true`)
			}
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
