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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type pipelineNetworkStats struct {
	atomicOperationCount       int64
	atomicNetworkErrorCount    int64
	atomic503CountThroughput   int64
	atomic503CountIOPS         int64
	atomic503CountUnknown      int64 // counts 503's when we don't know the reason
	atomicE2ETotalMilliseconds int64 // should this be nanoseconds?  Not really needed, given typical minimum operation lengths that we observe
	atomicStartSeconds         int64
	nocopy                     common.NoCopy
	tunerInterface             ConcurrencyTuner
}

func newPipelineNetworkStats(tunerInterface ConcurrencyTuner) *pipelineNetworkStats {
	s := &pipelineNetworkStats{tunerInterface: tunerInterface}
	tunerWillCallUs := tunerInterface.RequestCallbackWhenStable(s.start) // we want to start gather stats after the tuner has reached a stable value. No point in gathering them earlier
	if !tunerWillCallUs {
		// assume tuner is inactive, and start ourselves now
		s.start()
	}
	return s
}

// start starts the gathering of stats
func (s *pipelineNetworkStats) start() {
	atomic.StoreInt64(&s.atomicStartSeconds, time.Now().Unix())
}

func (s *pipelineNetworkStats) getStartSeconds() int64 {
	return atomic.LoadInt64(&s.atomicStartSeconds)
}

func (s *pipelineNetworkStats) IsStarted() bool {
	return s.getStartSeconds() > 0
}

func (s *pipelineNetworkStats) recordRetry(responseBody string) {
	if strings.Contains(responseBody, "gress is over the account limit") { // maybe Ingress or Egress
		atomic.AddInt64(&s.atomic503CountThroughput, 1)
	} else if strings.Contains(responseBody, "Operations per second is over the account limit") {
		atomic.AddInt64(&s.atomic503CountIOPS, 1)
	} else {
		atomic.AddInt64(&s.atomic503CountUnknown, 1) // we don't know what caused this 503 (that can happen)
	}
}

func (s *pipelineNetworkStats) OperationsPerSecond() int {
	s.nocopy.Check()
	if !s.IsStarted() {
		return 0
	}
	elapsed := time.Since(time.Unix(s.getStartSeconds(), 0)).Seconds()
	if elapsed > 0 {
		return int(float64(atomic.LoadInt64(&s.atomicOperationCount)) / elapsed)
	} else {
		return 0
	}
}

func (s *pipelineNetworkStats) NetworkErrorPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomicNetworkErrorCount)) / ops
	} else {
		return 0
	}
}

func (s *pipelineNetworkStats) TotalServerBusyPercentage() float32 {
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

func (s *pipelineNetworkStats) GetTotalRetries() int64 {
	s.nocopy.Check()
	return atomic.LoadInt64(&s.atomic503CountThroughput) +
		atomic.LoadInt64(&s.atomic503CountIOPS) +
		atomic.LoadInt64(&s.atomic503CountUnknown)
}

func (s *pipelineNetworkStats) IOPSServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountIOPS)) / ops
	} else {
		return 0
	}
}

func (s *pipelineNetworkStats) ThroughputServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountThroughput)) / ops
	} else {
		return 0
	}
}

func (s *pipelineNetworkStats) OtherServerBusyPercentage() float32 {
	s.nocopy.Check()
	ops := float32(atomic.LoadInt64(&s.atomicOperationCount))
	if ops > 0 {
		return 100 * float32(atomic.LoadInt64(&s.atomic503CountUnknown)) / ops
	} else {
		return 0
	}
}

func (s *pipelineNetworkStats) AverageE2EMilliseconds() int {
	s.nocopy.Check()
	ops := atomic.LoadInt64(&s.atomicOperationCount)
	if ops > 0 {
		return int(atomic.LoadInt64(&s.atomicE2ETotalMilliseconds) / ops)
	} else {
		return 0
	}
}

type xferStatsPolicy struct {
	next  pipeline.Policy
	stats *pipelineNetworkStats
}

// Do accumulates stats for each call
func (p *xferStatsPolicy) Do(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
	start := time.Now()

	resp, err := p.next.Do(ctx, request)

	if p.stats != nil {
		if p.stats.IsStarted() {
			atomic.AddInt64(&p.stats.atomicOperationCount, 1)
			atomic.AddInt64(&p.stats.atomicE2ETotalMilliseconds, int64(time.Since(start).Seconds()*1000))

			if err != nil && !isContextCancelledError(err) {
				// no response from server
				atomic.AddInt64(&p.stats.atomicNetworkErrorCount, 1)
			}
		}

		// always look at retries, even if not started, because concurrency tuner needs to know about them
		if resp != nil {
			// TODO should we also count status 500?  It is mentioned here as timeout:https://docs.microsoft.com/en-us/azure/storage/common/storage-scalability-targets
			if rr := resp.Response(); rr != nil && rr.StatusCode == http.StatusServiceUnavailable {
				p.stats.tunerInterface.recordRetry() // always tell the tuner
				if p.stats.IsStarted() {             // but only count it here, if we have started
					// To find out why the server was busy we need to look at the response
					responseBodyText := transparentlyReadBody(rr)
					p.stats.recordRetry(responseBodyText)
				}
			}
		}
	}

	return resp, err
}

// transparentlyReadBody reads the response body, and then (because body is read-once-only) replaces it with
// a new body that will return the same content to anyone else who reads it.
// This looks like a fairly common approach in Go, e.g. https://stackoverflow.com/a/23077519
// Our implementation here returns a string, so is only sensible for bodies that we know to be short - e.g. bodies of error responses.
func transparentlyReadBody(r *http.Response) string {
	if r.Body == http.NoBody {
		return ""
	}
	buf, _ := ioutil.ReadAll(r.Body)                // error responses are short fragments of XML, so safe to read all
	_ = r.Body.Close()                              // must close the real body
	r.Body = ioutil.NopCloser(bytes.NewReader(buf)) // replace it with something that will read the same data we just read

	return string(buf) // copy to string
}

func newXferStatsPolicyFactory(accumulator *pipelineNetworkStats) pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		r := xferStatsPolicy{next: next, stats: accumulator}
		return r.Do
	})
}
