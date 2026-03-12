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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// GlobalHTTPClient is the process-wide HTTP client used by AzCopy when initialized via InitGlobalHTTPClient.
var (
	GlobalHTTPClient     *http.Client
	globalHTTPClientOnce sync.Once
)

const (
	maxIdleConnsPerHost_MaxValue = 3000
	httpTraceTickerInterval      = time.Minute * 1
)

// GetGlobalHTTPClient initializes and returns the process-global HTTP client exactly once.
// Subsequent calls return the same client. The logger function, if provided on the first call,
// will be invoked with status messages.
func GetGlobalHTTPClient(logger ILoggerResetable) *http.Client {
	globalHTTPClientOnce.Do(func() {
		const concurrentDialsPerCpu = 10
		client := &http.Client{
			Transport: &http.Transport{
				Proxy:                  GlobalProxyLookup,
				MaxConnsPerHost:        concurrentDialsPerCpu * runtime.NumCPU(),
				MaxIdleConns:           0,
				MaxIdleConnsPerHost:    GetMaxIdleConnsPerHost(),
				IdleConnTimeout:        180 * time.Second,
				TLSHandshakeTimeout:    10 * time.Second,
				ExpectContinueTimeout:  1 * time.Second,
				DisableKeepAlives:      false,
				DisableCompression:     true,
				MaxResponseHeaderBytes: 0,
			},
		}
		GlobalHTTPClient = client
		if logger != nil {
			if tr, ok := client.Transport.(*http.Transport); ok {
				logger.Log(LogError, // XDM: This is error level on purpose as we want to make sure it is seen in the logs
					fmt.Sprintf(
						"GetGlobalHTTPClient: initialized %p MaxIdleConnsPerHost=%d MaxConnsPerHost=%d MaxIdleConns=%d",
						client, tr.MaxIdleConnsPerHost, tr.MaxConnsPerHost, tr.MaxIdleConns))
			} else {
				logger.Log(LogError, fmt.Sprintf("GetGlobalHTTPClient: initialized %p", client))
			}
		}
	})
	return GlobalHTTPClient
}

// This is a code duplication of GetMainPoolSize in ste/concurrency.go
// Ideally we should just move concurrency.go to common package. That is a todo for future.
func GetMaxIdleConnsPerHost() int {

	autoTune := true
	envVar := EEnvironmentVariable.ConcurrencyValue()
	envValue := GetEnvironmentVariable(envVar)
	concurrencyValue := maxIdleConnsPerHost_MaxValue

	if envValue != "AUTO" && envValue != "" {
		concurrencyVal, err := strconv.ParseInt(envValue, 10, 0)
		if err == nil {
			concurrencyValue = min(int(concurrencyVal), concurrencyValue)
			autoTune = false
		}
	}

	if autoTune {
		var computedDefaultVal int
		numOfCPUs := runtime.NumCPU()

		if autoTune {
			computedDefaultVal = 4 // deliberately start with a small initial value if we are auto-tuning.  If it's not small enough, then the auto tuning becomes
			// sluggish since, every time it needs to tune downwards, it needs to let a lot of data (num connections * block size) get transmitted,
			// and that is slow over very small links, e.g. 10 Mbps, and produces noticeable time lag when downsizing the connection count.
			// So we start small. (The alternatives, of using small chunk sizes or small file sizes just for the first 200 MB or so, were too hard to orchestrate within the existing app architecture)
		} else if numOfCPUs <= 4 {
			// fix the concurrency value for smaller machines
			computedDefaultVal = 32
		} else if 16*numOfCPUs > 300 {
			// for machines that are extremely powerful, fix to 300 (previously this was to avoid running out of file descriptors, but we have another solution to that now)
			computedDefaultVal = 300
		} else {
			// for moderately powerful machines, compute a reasonable number
			computedDefaultVal = 16 * numOfCPUs
		}

		concurrencyValue = min(computedDefaultVal, concurrencyValue) // we don't expect to ever need more than this, even in small-files cases
	}

	// Set the max idle connections that we allow. If there are any more idle connections
	// than this, they will be closed, and then will result in creation of new connections
	// later if needed. In AzCopy, they almost always will be needed soon after, so better to
	// keep them open.
	// So set this number high so that that will not happen.
	// (Previously, when using Dial instead of DialContext, there was an added benefit of keeping
	// this value high, which was that, without it being high, all the extra dials,
	// to compensate for the closures, were causing a pathological situation
	// where lots and lots of OS threads get created during the creation of new connections
	// (presumably due to some blocking OS call in dial) and the app hits Go's default
	// limit of 10,000 OS threads, and panics and shuts down.  This has been observed
	// on Windows when this value was set to 500 but there were 1000 to 2000 goroutines in the
	// main pool size.  Using DialContext appears to mitigate that issue, so the value
	// we compute here is really just to reduce unneeded make and break of connections)

	// Add a buffer to the concurrencyValue to compute the max idle conns value.
	// This buffer is to allow for some extra idle connections that might be needed
	// in certain scenarios

	maxIdleConnsPerHost := int(float64(concurrencyValue) * 1.2)

	return maxIdleConnsPerHost
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
// collect aggregated connection reuse metrics (per label). A periodic dumper is started once.
//
// Usage: replace the Transport field of an *http.Client with the result of this function.
// Use common.NewTracingTransport(client, "createClientOptions", logger) for http.Trace
// This will log connection stats every minute using the provided logger.
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
	return &traceTransport{inner: inner, label: label, logger: logger}
}

// traceTransport implements policy.Transporter using the wrapped transport's Do method.
type traceTransport struct {
	inner  policy.Transporter
	label  string
	logger ILoggerResetable
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
