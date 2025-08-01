// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

type JobLifecycleHandler interface {
	OnStart(ctx JobContext)

	OnScanProgress(progress ScanProgress) // only called during sync jobs
	OnTransferProgress(progress TransferProgress)
}

type JobContext struct {
	LogPath   string
	JobID     JobID
	IsCleanup bool
}

type ScanProgress struct {
	Source             uint64   // Files Scanned at Source
	Destination        uint64   // Files Scanned at Destination (only applicable for sync jobs)
	TransferThroughput *float64 // Throughput (if first part has been ordered)
}

type TransferProgress struct {
	ListJobSummaryResponse
	DeleteTotalTransfers     uint32 `json:",string"` // (only applicable for sync jobs)
	DeleteTransfersCompleted uint32 `json:",string"` // (only applicable for sync jobs)
}
