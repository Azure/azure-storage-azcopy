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
}

// NewRateLimitTokenBucketPacer builds a pacer that caps both bandwidth and IOPS.
// opsPerSecond == 0 makes the IOPS dimension unlimited (a plain bandwidth cap),
// which is how a job that has not opted into IOPS pacing behaves.
func NewRateLimitTokenBucketPacer(bytesPerSecond, opsPerSecond int64) *rateLimitTokenBucketPacer {
	LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Initializing with bytesPerSecond=%d, opsPerSecond=%d", bytesPerSecond, opsPerSecond), LogDebug)
	return &rateLimitTokenBucketPacer{
		tokenBucketPacer: NewTokenBucketPacer(bytesPerSecond, 0),
		iopsBucket:       NewTokenBucketPacer(opsPerSecond, 0),
	}
}

// AcquireIO reserves IOPS first (cheap, small counts), then bandwidth. If the
// bandwidth reservation cannot be met it rolls back the IOP reservation so the
// two dimensions stay consistent (reserve both or reserve none).
func (d *rateLimitTokenBucketPacer) AcquireIO(ctx context.Context, bytes, ops int64) error {
	if ops > 0 {
		if err := d.iopsBucket.RequestTrafficAllocation(ctx, ops); err != nil {
			LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Failed to acquire IOPS tokens: ops=%d, err=%v", ops, err), LogError)
			return err
		}
	}
	if bytes > 0 {
		if err := d.tokenBucketPacer.RequestTrafficAllocation(ctx, bytes); err != nil {
			LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Failed to acquire bandwidth tokens: bytes=%d, err=%v", bytes, err), LogError)
			if ops > 0 {
				d.iopsBucket.UndoRequest(ops) // give the IOP token back
			}
			return err
		}
	}
	return nil
}

// UpdateTargetIOPS adjusts the IOPS rate independently of bandwidth (used by the
// Azure Files reactive AIMD / proactive equal-share controller).
func (d *rateLimitTokenBucketPacer) UpdateTargetIOPS(opsPerSecond int64) {
	LogToJobLogWithPrefix(fmt.Sprintf("rateLimitTokenBucketPacer: Updating Target IOPS: opsPerSecond=%d", opsPerSecond), LogInfo)
	d.iopsBucket.UpdateTargetBytesPerSecond(opsPerSecond)
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
