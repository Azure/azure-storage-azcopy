package common

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"golang.org/x/sync/semaphore"
)

// GlobalHTTPClient is the process-wide HTTP client used by AzCopy when initialized via InitGlobalHTTPClient.
var (
	GlobalHTTPClient     *http.Client
	globalHTTPClientOnce sync.Once
)

const (
	maxIdleConnsPerHost     = 300
	httpTraceTickerInterval = time.Minute * 1
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
				MaxIdleConnsPerHost:    maxIdleConnsPerHost,
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
				logger.Log(LogError,
					fmt.Sprintf(
						"GetGlobalHTTPClient: initialized %p MaxIdleConnsPerHost=%d MaxConnsPerHost=%d",
						client, tr.MaxIdleConnsPerHost, tr.MaxConnsPerHost))
			} else {
				logger.Log(LogError, fmt.Sprintf("GetGlobalHTTPClient: initialized %p", client))
			}
		}
	})
	return GlobalHTTPClient
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

// Prevents too many dials happening at once, because we've observed that that increases the thread
// count in the app, to several times more than is actually necessary - presumably due to a blocking OS
// call somewhere. It's tidier to avoid creating those excess OS threads.
// Even our change from Dial (deprecated) to DialContext did not replicate the effect of dialRateLimiter.
type dialRateLimiter struct {
	dialer *net.Dialer
	sem    *semaphore.Weighted
}

func newDialRateLimiter(dialer *net.Dialer) *dialRateLimiter {
	const concurrentDialsPerCpu = 196 // exact value doesn't matter too much, but too low will be too slow, and too high will reduce the beneficial effect on thread count
	return &dialRateLimiter{
		dialer,
		semaphore.NewWeighted(concurrentDialsPerCpu),
	}
}

func (d *dialRateLimiter) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	err := d.sem.Acquire(context.Background(), 1)
	if err != nil {
		return nil, err
	}
	defer d.sem.Release(1)

	return d.dialer.DialContext(ctx, network, address)
}
