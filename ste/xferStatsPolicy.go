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

package ste

import (
	"bytes"
	"context"
	"crypto/tls"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"time"
)

type PipelineNetworkStats struct {
	atomicOperationCount       int64
	atomicNetworkErrorCount    int64
	atomic503CountThroughput   int64
	atomic503CountIOPS         int64
	atomic503CountUnknown      int64 // counts 503's when we don't know the reason
	atomicE2ETotalMilliseconds int64 // should this be nanoseconds?  Not really needed, given typical minimum operation lengths that we observe
	atomicStartSeconds         int64

	// Latency breakdown: connection pool wait vs actual wire time
	atomicConnWaitTotalMs int64 // time waiting for a connection from http.Transport pool (GetConn → GotConn)
	atomicWireTotalMs     int64 // time on the wire after connection acquired (GotConn → response complete)
	atomicDNSTotalMs      int64 // DNS resolution time
	atomicTLSTotalMs      int64 // TLS handshake time
	atomicConnNewCount    int64 // number of new connections created (not reused)
	atomicConnReusedCount int64 // number of reused connections

	nocopy         common.NoCopy
	tunerInterface ConcurrencyTuner
}

func newPipelineNetworkStats(tunerInterface ConcurrencyTuner) *PipelineNetworkStats {
	s := &PipelineNetworkStats{tunerInterface: tunerInterface}
	atomic.StoreInt64(&s.atomicStartSeconds, time.Now().Unix())
	return s
}

func (s *PipelineNetworkStats) getStartSeconds() int64 {
	return atomic.LoadInt64(&s.atomicStartSeconds)
}

func (s *PipelineNetworkStats) recordRetry(responseBody string) {
	if strings.Contains(responseBody, "gress is over the account limit") { // maybe Ingress or Egress
		atomic.AddInt64(&s.atomic503CountThroughput, 1)
	} else if strings.Contains(responseBody, "Operations per second is over the account limit") {
		atomic.AddInt64(&s.atomic503CountIOPS, 1)
	} else {
		atomic.AddInt64(&s.atomic503CountUnknown, 1) // we don't know what caused this 503 (that can happen)
	}
}

func (s *PipelineNetworkStats) OperationsPerSecond() int {
	s.nocopy.Check()
	elapsed := time.Since(time.Unix(s.getStartSeconds(), 0)).Seconds()
	if elapsed > 0 {
		return int(float64(atomic.LoadInt64(&s.atomicOperationCount)) / elapsed)
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) NetworkErrorPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomicNetworkErrorCount)) / ops
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) TotalServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountThroughput)+
			atomic.LoadInt64(&s.atomic503CountIOPS)+
			atomic.LoadInt64(&s.atomic503CountUnknown)) / ops
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) GetTotalRetries() int64 {
	s.nocopy.Check()
	return atomic.LoadInt64(&s.atomic503CountThroughput) +
		atomic.LoadInt64(&s.atomic503CountIOPS) +
		atomic.LoadInt64(&s.atomic503CountUnknown)
}

func (s *PipelineNetworkStats) IOPSServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountIOPS)) / ops
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) ThroughputServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountThroughput)) / ops
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) OtherServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountUnknown)) / ops
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) AverageE2EMilliseconds() int {
	s.nocopy.Check()
	ops := atomic.LoadInt64(&s.atomicOperationCount)
	if ops > 0 {
		return int(atomic.LoadInt64(&s.atomicE2ETotalMilliseconds) / ops)
	} else {
		return 0
	}
}

func (s *PipelineNetworkStats) AverageConnWaitMs() int {
	ops := atomic.LoadInt64(&s.atomicOperationCount)
	if ops > 0 {
		return int(atomic.LoadInt64(&s.atomicConnWaitTotalMs) / ops)
	}
	return 0
}

func (s *PipelineNetworkStats) AverageWireMs() int {
	ops := atomic.LoadInt64(&s.atomicOperationCount)
	if ops > 0 {
		return int(atomic.LoadInt64(&s.atomicWireTotalMs) / ops)
	}
	return 0
}

func (s *PipelineNetworkStats) AverageDNSMs() int {
	ops := atomic.LoadInt64(&s.atomicOperationCount)
	if ops > 0 {
		return int(atomic.LoadInt64(&s.atomicDNSTotalMs) / ops)
	}
	return 0
}

func (s *PipelineNetworkStats) AverageTLSMs() int {
	ops := atomic.LoadInt64(&s.atomicOperationCount)
	if ops > 0 {
		return int(atomic.LoadInt64(&s.atomicTLSTotalMs) / ops)
	}
	return 0
}

func (s *PipelineNetworkStats) ConnNewCount() int64 {
	return atomic.LoadInt64(&s.atomicConnNewCount)
}

func (s *PipelineNetworkStats) ConnReusedCount() int64 {
	return atomic.LoadInt64(&s.atomicConnReusedCount)
}

// transparentlyReadBody reads the response body, and then (because body is read-once-only) replaces it with
// a new body that will return the same content to anyone else who reads it.
// This looks like a fairly common approach in Go, e.g. https://stackoverflow.com/a/23077519
// Our implementation here returns a string, so is only sensible for bodies that we know to be short - e.g. bodies of error responses.
func transparentlyReadBody(r *http.Response) string {
	if r.Body == http.NoBody {
		return ""
	}
	buf, _ := io.ReadAll(r.Body)                // error responses are short fragments of XML, so safe to read all
	_ = r.Body.Close()                          // must close the real body
	r.Body = io.NopCloser(bytes.NewReader(buf)) // replace it with something that will read the same data we just read

	return string(buf) // copy to string
}

var pipelineNetworkStatsContextKey = contextKey{"pipelineNetworkStats"}

// withPipelineNetworkStats returns a context that contains a pipeline network stats. The retryNotificationPolicy
// will then invoke the pipeline network stats object when necessary
func withPipelineNetworkStats(ctx context.Context, stats *PipelineNetworkStats) context.Context {
	return context.WithValue(ctx, pipelineNetworkStatsContextKey, stats)
}

type statsPolicy struct {
}

func (s statsPolicy) Do(req *policy.Request) (*http.Response, error) {
	start := time.Now()

	// Add httptrace to measure connection pool wait vs wire time
	var connWaitStart, gotConnTime, dnsStart, tlsStart time.Time
	var connWaitMs, dnsMs, tlsMs int64
	var wasReused bool

	trace := &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			connWaitStart = time.Now()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			gotConnTime = time.Now()
			connWaitMs = gotConnTime.Sub(connWaitStart).Milliseconds()
			wasReused = info.Reused
		},
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			if !dnsStart.IsZero() {
				dnsMs = time.Since(dnsStart).Milliseconds()
			}
		},
		TLSHandshakeStart: func() {
			tlsStart = time.Now()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if !tlsStart.IsZero() {
				tlsMs = time.Since(tlsStart).Milliseconds()
			}
		},
	}

	raw := req.Raw()
	*raw = *raw.WithContext(httptrace.WithClientTrace(raw.Context(), trace))

	response, err := req.Next()

	// Grab the notification callback out of the context and, if its there, call it
	stats, ok := req.Raw().Context().Value(pipelineNetworkStatsContextKey).(*PipelineNetworkStats)
	if ok && stats != nil {
		e2eMs := int64(time.Since(start).Seconds() * 1000)
		atomic.AddInt64(&stats.atomicOperationCount, 1)
		atomic.AddInt64(&stats.atomicE2ETotalMilliseconds, e2eMs)

		// Record latency breakdown
		atomic.AddInt64(&stats.atomicConnWaitTotalMs, connWaitMs)
		if !gotConnTime.IsZero() {
			wireMs := int64(time.Since(gotConnTime).Seconds() * 1000)
			atomic.AddInt64(&stats.atomicWireTotalMs, wireMs)
		}
		atomic.AddInt64(&stats.atomicDNSTotalMs, dnsMs)
		atomic.AddInt64(&stats.atomicTLSTotalMs, tlsMs)
		if wasReused {
			atomic.AddInt64(&stats.atomicConnReusedCount, 1)
		} else {
			atomic.AddInt64(&stats.atomicConnNewCount, 1)
		}

		if err != nil && !isContextCancelledError(err) {
			// no response from server
			atomic.AddInt64(&stats.atomicNetworkErrorCount, 1)
		}

		// always look at retries, even if not started, because concurrency tuner needs to know about them
		// TODO should we also count status 500?  It is mentioned here as timeout:https://docs.microsoft.com/en-us/azure/storage/common/storage-scalability-targets
		if response != nil && response.StatusCode == http.StatusServiceUnavailable {
			stats.tunerInterface.recordRetry() // always tell the tuner
			// To find out why the server was busy we need to look at the response
			responseBodyText := transparentlyReadBody(response)
			stats.recordRetry(responseBodyText)
		}
	}

	return response, err
}

func newStatsPolicy() policy.Policy {
	return statsPolicy{}
}
