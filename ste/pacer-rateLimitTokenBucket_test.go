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

// TestDualPacer_AcquireIO_RollsBackIopsOnBandwidthFailure verifies the
// "reserve both or reserve none" guarantee: when the IOPS reservation succeeds
// but the bandwidth reservation fails, the IOP token is returned so the two
// dimensions never drift.
func TestDualPacer_AcquireIO_RollsBackIopsOnBandwidthFailure(t *testing.T) {
	// Bandwidth target is smaller than the requested byte count, so the
	// bandwidth reservation fails immediately (request size > pacer target).
	// The IOPS target is generous, so its reservation succeeds first.
	p := NewRateLimitTokenBucketPacer(1000, 1000)
	defer func() { _ = p.Close() }()

	err := p.AcquireIO(context.Background(), 5000 /* bytes */, 1 /* ops */)
	if err == nil {
		t.Fatal("expected an error when bandwidth reservation exceeds the pacer target")
	}

	// The IOP token that was reserved before the bandwidth failure must have
	// been returned via UndoRequest, leaving the IOPS dimension at zero issued.
	if issued := p.iopsBucket.GetTotalTraffic(); issued != 0 {
		t.Fatalf("expected IOPS reservation to be rolled back (0 issued), got %d", issued)
	}
}

// TestDualPacer_AcquireIO_BothDimensionsCharged verifies that a successful
// AcquireIO charges both the bandwidth and IOPS buckets.
func TestDualPacer_AcquireIO_BothDimensionsCharged(t *testing.T) {
	p := NewRateLimitTokenBucketPacer(1024*1024, 1000)
	defer func() { _ = p.Close() }()

	if err := p.AcquireIO(context.Background(), 100 /* bytes */, 1 /* ops */); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := p.GetTotalTraffic(); got != 100 {
		t.Fatalf("bandwidth dimension: want 100 bytes issued, got %d", got)
	}
	if got := p.iopsBucket.GetTotalTraffic(); got != 1 {
		t.Fatalf("IOPS dimension: want 1 op issued, got %d", got)
	}
}

// TestDualPacer_AcquireIO_UnlimitedIopsWhenZero verifies that a zero IOPS rate
// makes the IOPS dimension unlimited (a plain bandwidth cap), matching the
// null-pacer semantics.
func TestDualPacer_AcquireIO_UnlimitedIopsWhenZero(t *testing.T) {
	p := NewRateLimitTokenBucketPacer(1024*1024, 0 /* unlimited IOPS */)
	defer func() { _ = p.Close() }()

	// A huge op count must still be admitted because IOPS is unlimited.
	if err := p.AcquireIO(context.Background(), 100, 1_000_000); err != nil {
		t.Fatalf("unexpected error with unlimited IOPS: %v", err)
	}
}

// TestDualPacer_AcquireIO_MetadataOpChargesIopsOnly verifies that a zero-byte
// metadata operation charges only the IOPS dimension.
func TestDualPacer_AcquireIO_MetadataOpChargesIopsOnly(t *testing.T) {
	p := NewRateLimitTokenBucketPacer(1024*1024, 1000)
	defer func() { _ = p.Close() }()

	if err := p.AcquireIO(context.Background(), 0 /* bytes */, 1 /* ops */); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := p.GetTotalTraffic(); got != 0 {
		t.Fatalf("bandwidth dimension: want 0 bytes issued for a metadata op, got %d", got)
	}
	if got := p.iopsBucket.GetTotalTraffic(); got != 1 {
		t.Fatalf("IOPS dimension: want 1 op issued, got %d", got)
	}
}

// TestDualPacer_AcquireIO_DataOpChargesBandwidthOnly verifies that a zero-op
// data body charges only the bandwidth dimension.
func TestDualPacer_AcquireIO_DataOpChargesBandwidthOnly(t *testing.T) {
	p := NewRateLimitTokenBucketPacer(1024*1024, 1000)
	defer func() { _ = p.Close() }()

	if err := p.AcquireIO(context.Background(), 100 /* bytes */, 0 /* ops */); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := p.GetTotalTraffic(); got != 100 {
		t.Fatalf("bandwidth dimension: want 100 bytes issued, got %d", got)
	}
	if got := p.iopsBucket.GetTotalTraffic(); got != 0 {
		t.Fatalf("IOPS dimension: want 0 ops issued, got %d", got)
	}
}
