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

// init injects the concrete per-share pacer constructor into common, so the
// per-share registry (which lives in common alongside the RateLimitController)
// can build real dual token-bucket pacers without common ever importing ste.
// The pacer starts fully unlimited (0,0); the controller drives it thereafter.
func init() {
	common.RegisterSharePacerFactory(func() common.SharePacer {
		return NewRateLimitTokenBucketPacer(0, 0)
	})
}

// shareScopedPacer routes pacing for a single Azure Files destination share.
// Bandwidth is charged against BOTH the global job pacer (preserving the user's
// --cap-mbps aggregate) and the per-share pacer (the share's dynamically
// computed bandwidth), while IOPS is charged only against the per-share pacer.
// A caller must satisfy both, so the effective rate is the min of the two.
//
// It embeds the global pacer so it inherits every pacer/PacerAdmin method
// (RequestTrafficAllocation, UpdateTargetBytesPerSecond, UndoRequest, Close,
// GetTotalTraffic, ...) unchanged, and adds AcquireIO so pacerAcquire routes
// through it.
type shareScopedPacer struct {
	pacer
	share common.IOPSPacer
}

// newShareScopedPacer wraps global with per-share scoping. When share is nil it
// returns the global pacer unchanged, so non-Files paths are untouched.
func newShareScopedPacer(global pacer, share common.IOPSPacer) pacer {
	if share == nil {
		return global
	}
	return &shareScopedPacer{pacer: global, share: share}
}

func (s *shareScopedPacer) AcquireIO(ctx context.Context, bytes, ops int64) error {
	// Global aggregate bandwidth cap first (preserves --cap-mbps semantics).
	if bytes > 0 {
		if err := s.pacer.RequestTrafficAllocation(ctx, bytes); err != nil {
			return err
		}
	}
	// Per-share dynamic bandwidth + IOPS.
	if err := s.share.AcquireIO(ctx, bytes, ops); err != nil {
		if bytes > 0 {
			s.pacer.UndoRequest(bytes) // roll back the global reservation to stay consistent
		}
		return err
	}
	return nil
}

var _ ioPacer = (*shareScopedPacer)(nil)
