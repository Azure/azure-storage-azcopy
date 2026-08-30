// Copyright © Microsoft <wastore@microsoft.com>
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

package common

import (
	"fmt"
	"net/http"
	"net/http/httptrace"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// globalHTTPClient is the process-wide HTTP client used by AzCopy. It is configured
// exactly once by InitGlobalHTTPClient at startup (after ConcurrencySettings have
// been computed) and retrieved thereafter by GetGlobalHTTPClient.
var (
	globalHTTPClient     *http.Client
	globalHTTPClientOnce sync.Once
)

const httpTraceTickerInterval = time.Minute * 1

// InitGlobalHTTPClient initializes the process-global HTTP client exactly once with
// the supplied limits. Callers should pass the value derived from the STE
// ConcurrencySettings (e.g. ConcurrencySettings.MaxIdleConnections) so that the HTTP
// transport honors the same concurrency configuration as the rest of AzCopy.
// Subsequent calls are no-ops and the originally supplied values continue to apply.
// The configured client's transport limits are logged later by JobMgr when a job
// first picks it up, where a job-scoped logger is available.
//
// Must be called before GetGlobalHTTPClient. azcopy.NewClient is the canonical caller.
func InitGlobalHTTPClient(maxIdleConnsPerHost int) *http.Client {
	globalHTTPClientOnce.Do(func() {
		globalHTTPClient = buildGlobalHTTPClient(maxIdleConnsPerHost)
	})
	return globalHTTPClient
}

// GetGlobalHTTPClient returns the process-global HTTP client previously configured
// via InitGlobalHTTPClient. Panics if Init has not run yet; this is intentional so
// that an init-order bug fails loudly instead of silently producing a misconfigured
// client (e.g. with the Go default of 2 idle conns per host).
func GetGlobalHTTPClient() *http.Client {
	if globalHTTPClient == nil {
		panic("common.GetGlobalHTTPClient called before InitGlobalHTTPClient; " +
			"InitGlobalHTTPClient must run during process startup (see azcopy.NewClient)")
	}
	return globalHTTPClient
}

func buildGlobalHTTPClient(maxIdleConnsPerHost int) *http.Client {
	const concurrentDialsPerCpu = 10
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                  GlobalProxyLookup,
			MaxConnsPerHost:        concurrentDialsPerCpu * runtime.NumCPU(),
			MaxIdleConns:           0,
			MaxIdleConnsPerHost:    maxIdleConnsPerHost,
			IdleConnTimeout:        180 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     true,
			MaxResponseHeaderBytes: 0,
		},
	}
}

// connStats aggregates connection reuse metrics per label.
type connStats struct {
	total       uint64
	reused      uint64
	idle        uint64
	idleTotalNs uint64 // cumulative idle time in nanoseconds
	putIdle     uint64
	putIdleErr  uint64
}

var connStatsMap sync.Map // map[string]*connStats
var connStatsLoggerOnce sync.Once

var connDumpLogger ILoggerResetable

func incrementConnStats(label string, reused, wasIdle bool, idleNs uint64) {
	v, _ := connStatsMap.LoadOrStore(label, &connStats{})
	s := v.(*connStats)
	atomic.AddUint64(&s.total, 1)
	if reused {
		atomic.AddUint64(&s.reused, 1)
	}
	if wasIdle {
		atomic.AddUint64(&s.idle, 1)
		if idleNs > 0 {
			atomic.AddUint64(&s.idleTotalNs, idleNs)
		}
	}
}

func incrementPutIdleStats(label string, err error) {
	v, _ := connStatsMap.LoadOrStore(label, &connStats{})
	s := v.(*connStats)
	if err != nil {
		atomic.AddUint64(&s.putIdleErr, 1)
	} else {
		atomic.AddUint64(&s.putIdle, 1)
	}
}

func dumpConnStats() {
	connStatsMap.Range(func(k, v interface{}) bool {
		label := k.(string)
		s := v.(*connStats)
		total := atomic.LoadUint64(&s.total)
		reused := atomic.LoadUint64(&s.reused)
		idle := atomic.LoadUint64(&s.idle)
		idleTotalNs := atomic.LoadUint64(&s.idleTotalNs)
		putIdle := atomic.LoadUint64(&s.putIdle)
		putIdleErr := atomic.LoadUint64(&s.putIdleErr)
		avgIdleMs := 0.0
		if idle > 0 {
			avgIdleMs = float64(idleTotalNs) / float64(idle) / 1e6
		}
		msg := fmt.Sprintf("connStats[%s]: total=%d reused=%d idle=%d avgIdleMs=%.2f putIdle=%d putIdleErr=%d", label, total, reused, idle, avgIdleMs, putIdle, putIdleErr)
		if connDumpLogger != nil {
			connDumpLogger.Log(LogError, msg)
		} else {
			fmt.Println(msg)
		}
		return true
	})
}

// NewTracingTransport wraps an existing policy.Transporter and injects an httptrace.ClientTrace to
// collect aggregated connection reuse metrics (per label). A periodic dumper is started once,
// using the logger supplied on the FIRST call; loggers passed to later calls are ignored.
//
// Note: *http.Client satisfies policy.Transporter, so it can be passed directly. Typical usage:
//
//	common.NewTracingTransport(common.GetGlobalHTTPClient(), "createClientOptions", logger)
//
// Connection stats are logged every httpTraceTickerInterval using the captured logger.
func NewTracingTransport(inner policy.Transporter, label string, logger ILoggerResetable) policy.Transporter {
	connStatsLoggerOnce.Do(func() {
		if logger != nil {
			connDumpLogger = logger
		}
		go func() {
			ticker := time.NewTicker(httpTraceTickerInterval)
			defer ticker.Stop()
			for range ticker.C {
				dumpConnStats()
			}
		}()
	})
	return &traceTransport{inner: inner, label: label}
}

// traceTransport implements policy.Transporter using the wrapped transport's Do method.
type traceTransport struct {
	inner policy.Transporter
	label string
}

func (t *traceTransport) Do(req *http.Request) (*http.Response, error) {
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			// Record whether the connection was reused and whether it was idle; also capture IdleTime when available.
			var idleNs uint64 = 0
			if info.WasIdle {
				idleNs = uint64(info.IdleTime.Nanoseconds())
			}
			incrementConnStats(t.label, info.Reused, info.WasIdle, idleNs)
		},
		PutIdleConn: func(err error) {
			// Record when a connection is returned to the idle pool (or why it wasn't)
			incrementPutIdleStats(t.label, err)
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	return t.inner.Do(req)
}
