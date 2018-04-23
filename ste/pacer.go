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

package ste

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
)

type pacer struct {
	bytesAvailable          int64
	availableBytesPerPeriod int64
	lastUpdatedTimestamp    int64
}

// this function returns a pacer which limits the number bytes allowed to go out every second
// it does so by issuing tickets (bytes allowed) periodically
func newPacer(bytesPerSecond int64) (p *pacer) {
	p = &pacer{bytesAvailable: 0,
		availableBytesPerPeriod: bytesPerSecond * int64(PacerTimeToWaitInMs) / 1000,
		lastUpdatedTimestamp:    time.Now().UnixNano()}

	// the pace runs in a separate goroutine for as long as the transfer engine is running
	go func() {
		for {
			// surrender control until time to wait has elapsed
			for targetTime := time.Now().Add(time.Millisecond * time.Duration(PacerTimeToWaitInMs)); time.Now().Before(targetTime); {
				runtime.Gosched()
			}

			atomic.StoreInt64(&p.bytesAvailable, p.availableBytesPerPeriod)
			// if too many tickets were issued (2x the intended), we should scale back
			//if atomic.AddInt64(&p.bytesAvailable, p.availableBytesPerPeriod) > 2*p.availableBytesPerPeriod {
			//	atomic.AddInt64(&p.bytesAvailable, -p.availableBytesPerPeriod)
			//}
		}
	}()

	return
}

// NewPacerPolicyFactory creates a factory that can create telemetry policy objects
// which add telemetry information to outgoing HTTP requests.
func NewPacerPolicyFactory(p *pacer) pipeline.Factory {
	return pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
		return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
			resp, err := next.Do(ctx, request)
			if err == nil {
				// Reducing the pacer's rate limit by 10 s for every 503 error.
				p.updateTargetRate(
					(resp.Response().StatusCode != http.StatusServiceUnavailable) &&
						(resp.Response().StatusCode != http.StatusInternalServerError))
			}
			return resp, err
		}
	})
}

// this function is called by goroutines to request right to send a certain amount of bytes
func (p *pacer) requestRightToSend(bytesToSend int64) {

	// attempt to take off the desired number of tickets until success (total number of tickets is not negative)
	for atomic.AddInt64(&p.bytesAvailable, -bytesToSend) < 0 {

		// put tickets back if attempt was unsuccessful
		atomic.AddInt64(&p.bytesAvailable, bytesToSend)
		time.Sleep(time.Millisecond * 1)
	}
}

func (p *pacer) updateTargetRate(increase bool) {
	lastCheckedTimestamp := atomic.LoadInt64(&p.lastUpdatedTimestamp)
	//lastCheckedTime := time.Unix(0,lastCheckedTimestamp)
	if time.Now().Sub(time.Unix(0, lastCheckedTimestamp)) < (time.Second * 3) {
		return
	}
	if atomic.CompareAndSwapInt64(&p.lastUpdatedTimestamp, lastCheckedTimestamp, time.Now().UnixNano()) {
		atomic.StoreInt64(&p.availableBytesPerPeriod, int64(common.Ifffloat64(increase, 1.1, 0.9)*float64(p.availableBytesPerPeriod)))
	}
}

// this struct wraps the ReadSeeker which contains the data to be sent over the network
type requestBodyPacer struct {
	requestBody io.ReadSeeker // Seeking is required to support retries
	p           *pacer
}

// get a ReadSeeker wrapper for the given request body, bound to the given pacer
func newRequestBodyPacer(requestBody io.ReadSeeker, p *pacer) io.ReadSeeker {
	if p == nil {
		panic("pr must not be nil")
	}
	return &requestBodyPacer{requestBody: requestBody, p: p}
}

// read blocks until tickets are obtained
func (rbp *requestBodyPacer) Read(p []byte) (int, error) {
	rbp.p.requestRightToSend(int64(len(p)))
	return rbp.requestBody.Read(p)
}

// no behavior change for seek
func (rbp *requestBodyPacer) Seek(offset int64, whence int) (offsetFromStart int64, err error) {
	return rbp.requestBody.Seek(offset, whence)
}

// requestBodyPacer supports Close but the underlying stream may not; if it does, Close will close it.
func (rbp *requestBodyPacer) Close() error {
	if c, ok := rbp.requestBody.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
