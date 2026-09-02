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

import "context"

// IOPSPacer meters operations/second (and optionally bytes) for callers that
// must respect a shared storage IOPS budget. It lives in common so that both the
// enumeration traversers (cmd) and the transfer engine (ste) can draw from the
// same limiter instance without introducing a package dependency cycle.
//
// The enumeration path uses the ops-only form via ScanPacerAcquire; the STE data
// plane uses the bytes+ops form. A nil IOPSPacer means "unlimited", so callers
// that have no cap configured behave exactly as before.
type IOPSPacer interface {
	// AcquireIO blocks until `ops` IOPS tokens (and `bytes` bandwidth tokens, if
	// >0) are available, or until ctx is cancelled (in which case it returns
	// ctx.Err()).
	AcquireIO(ctx context.Context, bytes, ops int64) error
}

// ScanPacerAcquire is the single helper used by enumeration call sites to charge
// IOPS for metadata operations (List, GetProperties). It is nil-safe: when no
// scan pacer is configured it is a no-op, preserving today's behavior.
func ScanPacerAcquire(ctx context.Context, p IOPSPacer, ops int64) error {
	if p == nil || ops <= 0 {
		return nil
	}
	return p.AcquireIO(ctx, 0, ops) // enumeration is metadata: 0 bytes, N IOPs
}
