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
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// rateLimitTokenBucketPacer meters two independent dimensions:
//   - bandwidth (bytes/s) via the embedded tokenBucketPacer (unchanged behavior)
//   - IOPS      (ops/s)   via a second tokenBucketPacer reused as an op counter
//
// It satisfies ioPacer (so pacerAcquire routes to AcquireIO) and common.IOPSPacer
// (so the same instance can be handed to enumeration traversers as their
// ScanPacer). All legacy pacer/PacerAdmin methods are inherited from the embedded
// bandwidth bucket.
type rateLimitTokenBucketPacer struct {
	*tokenBucketPacer                   // bandwidth dimension (also provides GetTotalTraffic, UndoRequest, ...)
	iopsBucket        *tokenBucketPacer // IOPS dimension (rate = ops/s; the "bytes" arg is treated as ops)

	// Last reported wait counts, so traces can show throttling in this interval
	// rather than only the cumulative total.
	atomicLastIopsWaits int64
	atomicLastBwWaits   int64
}

const azureFilesIopsBurstSeconds float32 = 1

// waitDelta reports how many new waits a bucket has recorded since the previous
// call, which is what distinguishes throttling happening now from throttling
// that happened earlier in the job.
func waitDelta(bucket *tokenBucketPacer, lastSeen *int64) (total, delta int64) {
	total = atomic.LoadInt64(&bucket.atomicWaitCount)
	return total, total - atomic.SwapInt64(lastSeen, total)
}

// NewRateLimitTokenBucketPacer builds a pacer that caps both bandwidth and IOPS.
// opsPerSecond == 0 makes the IOPS dimension unlimited (a plain bandwidth cap),
// which is how a job that has not opted into IOPS pacing behaves.
func NewRateLimitTokenBucketPacer(bytesPerSecond, opsPerSecond int64) *rateLimitTokenBucketPacer {
	return newRateLimitTokenBucketPacer(bytesPerSecond, opsPerSecond, maxSecondsToOverpopulateBucket)
}

func newRateLimitTokenBucketPacer(bytesPerSecond, opsPerSecond int64, iopsBurstSeconds float32) *rateLimitTokenBucketPacer {
	common.LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Initializing with bytesPerSecond=%d, opsPerSecond=%d", bytesPerSecond, opsPerSecond), common.LogDebug)
	return &rateLimitTokenBucketPacer{
		tokenBucketPacer: NewTokenBucketPacer(bytesPerSecond, 0),
		iopsBucket:       newTokenBucketPacer(opsPerSecond, 0, iopsBurstSeconds),
	}
}

// AcquireIO reserves IOPS first (cheap, small counts), then bandwidth. If the
// bandwidth reservation cannot be met it rolls back the IOP reservation so the
// two dimensions stay consistent (reserve both or reserve none).
func (d *rateLimitTokenBucketPacer) AcquireIO(ctx context.Context, bytes, ops int64) error {
	if ops > 0 {
		remaining, err := d.iopsBucket.requestTokens(ctx, ops)
		if err != nil {
			common.LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Failed to acquire IOPS tokens: ops=%d, err=%v", ops, err), common.LogError)
			return err
		}
		totalWaits, newWaits := waitDelta(d.iopsBucket, &d.atomicLastIopsWaits)
		common.LogToJobLogWithPrefix(fmt.Sprintf(
			"rateLimitTokenBucketPacer: IOPS tokens requested=%d available=%d remaining=%d target=%d/sec waits=%d (+%d)",
			ops, remaining+ops, remaining, d.iopsBucket.targetBytesPerSecond(), totalWaits, newWaits), common.LogDebug)
	}
	if bytes > 0 {
		remaining, err := d.tokenBucketPacer.requestTokens(ctx, bytes)
		if err != nil {
			common.LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Failed to acquire bandwidth tokens: bytes=%d, err=%v", bytes, err), common.LogError)
			if ops > 0 {
				d.iopsBucket.UndoRequest(ops) // give the IOP token back
			}
			return err
		}
		totalWaits, newWaits := waitDelta(d.tokenBucketPacer, &d.atomicLastBwWaits)
		common.LogToJobLogWithPrefix(fmt.Sprintf(
			"rateLimitTokenBucketPacer: bandwidth tokens requested=%d available=%d remaining=%d target=%d/sec waits=%d (+%d)",
			bytes, remaining+bytes, remaining, d.tokenBucketPacer.targetBytesPerSecond(), totalWaits, newWaits), common.LogDebug)
	}
	return nil
}

// UpdateTargetIOPS adjusts the IOPS rate independently of bandwidth (used by the
// Azure Files reactive AIMD / proactive equal-share controller).
func (d *rateLimitTokenBucketPacer) UpdateTargetIOPS(opsPerSecond int64) {
	common.LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Updating Target IOPS: opsPerSecond=%d", opsPerSecond), common.LogInfo)
	d.iopsBucket.UpdateTargetBytesPerSecond(opsPerSecond)
}

// UpdateTargetBytesPerSecond overrides the embedded bucket's method only so the
// bandwidth target change is traced alongside the IOPS one.
func (d *rateLimitTokenBucketPacer) UpdateTargetBytesPerSecond(bytesPerSecond int64) {
	common.LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Updating Target Bandwidth: bytesPerSecond=%d", bytesPerSecond), common.LogInfo)
	d.tokenBucketPacer.UpdateTargetBytesPerSecond(bytesPerSecond)
}

func (d *rateLimitTokenBucketPacer) Close() error {
	_ = d.iopsBucket.Close()
	return d.tokenBucketPacer.Close()
}

var (
	_ ioPacer           = (*rateLimitTokenBucketPacer)(nil)
	_ PacerAdmin        = (*rateLimitTokenBucketPacer)(nil)
	_ common.IOPSPacer  = (*rateLimitTokenBucketPacer)(nil)
	_ common.RateLimitSink = (*rateLimitTokenBucketPacer)(nil) // the dual-mode controller drives this pacer's IOPS + bandwidth targets
)
