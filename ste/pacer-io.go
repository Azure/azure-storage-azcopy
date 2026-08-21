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

// ioPacer optionally extends pacer with an IOPS (operations/second) dimension.
// A pacer that does NOT implement ioPacer is treated as IOPS-unlimited, so all
// existing (bandwidth-only) pacers keep working unchanged. It reuses the
// common.IOPSPacer method so the same limiter instance can be shared with the
// enumeration traversers (which meter the IOPS dimension only).
type ioPacer interface {
	pacer
	common.IOPSPacer // AcquireIO(ctx, bytes, ops) error
}

// pacerAcquire is the single call site abstraction used by senders/downloaders.
// It charges bandwidth and/or IOPS when the underlying pacer supports IOPS, and
// gracefully degrades to bandwidth-only otherwise. This is the function called
// from the Azure Files data plane today; Blob call sites can adopt it later with
// no interface or signature changes.
//
//	ops == 0   -> bandwidth-only (e.g. a streamed data body already metered)
//	bytes == 0 -> metadata op (GetProperties/SetProperties): charge IOPS only
func pacerAcquire(ctx context.Context, p pacer, bytes, ops int64) error {
	if p == nil {
		return nil // unpaced callers
	}
	if iop, ok := p.(ioPacer); ok {
		return iop.AcquireIO(ctx, bytes, ops)
	}
	// Legacy / bandwidth-only pacer: meter bytes, ignore IOPS.
	if bytes > 0 {
		return p.RequestTrafficAllocation(ctx, bytes)
	}
	return nil
}

// sourceSharePacer returns the per-share IOPS pacer for an Azure Files source
// URL, so reads from the source draw on the same budget the enumeration scan
// pacer uses. It returns nil when share-scoped pacing is off or the URL is not
// Azure Files, which callers treat as unlimited.
func sourceSharePacer(sourceURL string) common.IOPSPacer {
	if common.GetEnvironmentVariable(common.EEnvironmentVariable.EnableAzFilesProactiveStats()) != "true" {
		return nil
	}
	return common.GetShareScanPacer(sourceURL)
}
