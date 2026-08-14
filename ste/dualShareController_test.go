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
	"testing"
)

// TestShareScopedPacer_ChargesBothBandwidthAndIops verifies that a successful
// AcquireIO charges the global bandwidth pacer and the per-share IOPS pacer.
func TestShareScopedPacer_ChargesBothBandwidthAndIops(t *testing.T) {
	global := NewTokenBucketPacer(1024*1024, 0)
	defer func() { _ = global.Close() }()
	sharePacer := NewDualTokenBucketPacer(0, 0) // unlimited per-share dims
	defer func() { _ = sharePacer.Close() }()

	scoped := newShareScopedPacer(global, sharePacer).(*shareScopedPacer)

	if err := scoped.AcquireIO(context.Background(), 100, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := global.GetTotalTraffic(); got != 100 {
		t.Fatalf("global bandwidth: want 100 bytes, got %d", got)
	}
	if got := sharePacer.iopsBucket.GetTotalTraffic(); got != 1 {
		t.Fatalf("per-share IOPS: want 1 op, got %d", got)
	}
}

// TestShareScopedPacer_EnforcesGlobalCap verifies the global --cap-mbps pacer
// still rejects an over-cap request, and that the per-share IOPS bucket is not
// charged when the global reservation fails first.
func TestShareScopedPacer_EnforcesGlobalCap(t *testing.T) {
	global := NewTokenBucketPacer(1000, 0) // tiny cap
	defer func() { _ = global.Close() }()
	sharePacer := NewDualTokenBucketPacer(0, 0)
	defer func() { _ = sharePacer.Close() }()

	scoped := newShareScopedPacer(global, sharePacer).(*shareScopedPacer)

	if err := scoped.AcquireIO(context.Background(), 5000 /* > cap */, 1); err == nil {
		t.Fatal("expected global cap to reject an over-cap request")
	}
	if got := sharePacer.iopsBucket.GetTotalTraffic(); got != 0 {
		t.Fatalf("per-share IOPS must not be charged when global cap fails first, got %d", got)
	}
}

// TestShareScopedPacer_NilShareReturnsGlobal verifies the wrapper is a no-op
// (returns the global pacer unchanged) when there is no share control.
func TestShareScopedPacer_NilShareReturnsGlobal(t *testing.T) {
	global := NewTokenBucketPacer(1000, 0)
	defer func() { _ = global.Close() }()
	if p := newShareScopedPacer(global, nil); p != pacer(global) {
		t.Fatal("expected the global pacer to be returned unchanged when share is nil")
	}
}
