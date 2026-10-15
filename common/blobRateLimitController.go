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

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

type blobRateLimitControl struct {
	pacer SharePacer
	ctrl  ResourceController
	stop  context.CancelFunc
}

var (
	blobRateLimitControlsMu sync.Mutex
	blobRateLimitControls   = map[string]*blobRateLimitControl{}
	blobRateLimitControlsWg sync.WaitGroup
)

// GetOrCreateBlobPacer returns the reactive-only pacer for the destination Blob
// account in blobURL. It returns nil for non-Blob URLs.
func GetOrCreateBlobPacer(blobURL string, workers int64) SharePacer {
	control := getOrCreateBlobRateLimitControl(blobURL, workers)
	if control == nil {
		return nil
	}
	return control.pacer
}

func getOrCreateBlobRateLimitControl(rawURL string, workers int64) *blobRateLimitControl {
	blobKey := BlobAccountKeyFromRawURL(rawURL)
	if blobKey == "" {
		return nil
	}

	blobRateLimitControlsMu.Lock()
	if control, ok := blobRateLimitControls[blobKey]; ok {
		blobRateLimitControlsMu.Unlock()
		return control
	}

	if workers < 1 {
		workers = 1
	}
	pacer := sharePacerFactory()
	statsSource := stubResourceStatsSource{}
	ctrl := controllerFactory(ELocation.Blob(), pacer, statsSource, workers)
	traceBlobRateLimitWiringOnce(pacer, blobKey)

	ctx, cancel := context.WithCancel(context.Background())
	control := &blobRateLimitControl{pacer: pacer, ctrl: ctrl, stop: cancel}
	blobRateLimitControls[blobKey] = control
	blobRateLimitControlsMu.Unlock()

	if _, err := ctrl.Refresh(); err != nil {
		traceShare(LogError, "priming Blob controller failed for %s: %v", blobKey, err)
	}
	traceShare(LogInfo, "Blob controller initialized for %s: mode=%s workers=%d primedRates(%s)",
		blobKey, ctrl.Mode(), workers, describeRates(ctrl))

	startBlobRateLimitPoller(ctx, blobKey, ctrl)
	return control
}

func traceBlobRateLimitWiringOnce(pacer SharePacer, blobKey string) {
	_, pacerIsNull := pacer.(nullSharePacer)
	TraceShareOnce(fmt.Sprintf("blob-wiring:%t", pacerIsNull), LogInfo,
		"Blob wiring for %s: pacer=%T (registered=%t)%s",
		blobKey, pacer, sharePacerFactoryRegistered,
		ternaryStr(pacerIsNull,
			" -- INERT: no-op pacer in use",
			" -- reactive-only: awaiting Blob 429/503 responses"))
}

func startBlobRateLimitPoller(ctx context.Context, blobKey string, ctrl ResourceController) {
	blobRateLimitControlsWg.Add(1)
	go func() {
		defer blobRateLimitControlsWg.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				traceShare(LogError, "Blob poll loop for %s stopped after panic: %v", blobKey, recovered)
			}
		}()
		pollResourceStats(ctx, blobKey, ctrl)
	}()
}

// GetBlobPacer returns an existing Blob account pacer without creating one.
// Pipeline policies use this to avoid enabling destination pacing for unrelated
// source-account requests.
func GetBlobPacer(blobURL string) IOPSPacer {
	blobKey := BlobAccountKeyFromRawURL(blobURL)
	if blobKey == "" {
		return nil
	}
	blobRateLimitControlsMu.Lock()
	control := blobRateLimitControls[blobKey]
	blobRateLimitControlsMu.Unlock()
	if control == nil {
		return nil
	}
	return control.pacer
}

// HandleBlobResponse feeds a classified Blob response into an existing
// destination-account controller.
func HandleBlobResponse(blobKey string, kind ThrottleKind, retryAfterSec float64) {
	if blobKey == "" {
		return
	}
	blobRateLimitControlsMu.Lock()
	control := blobRateLimitControls[blobKey]
	blobRateLimitControlsMu.Unlock()
	if control != nil {
		control.ctrl.HandleResponse(kind, retryAfterSec)
	}
}

// BlobAccountKeyFromRawURL derives the stable account host used to share one
// controller across all destination containers and blobs in that account.
func BlobAccountKeyFromRawURL(raw string) string {
	parts, err := blob.ParseURL(raw)
	if err != nil || !strings.Contains(strings.ToLower(parts.Host), ".blob.") {
		return ""
	}
	return strings.ToLower(parts.Host)
}

// StopBlobRateLimitControls stops all destination Blob account controllers.
func StopBlobRateLimitControls() {
	blobRateLimitControlsMu.Lock()
	stopped := len(blobRateLimitControls)
	for key, control := range blobRateLimitControls {
		control.stop()
		_ = control.pacer.Close()
		delete(blobRateLimitControls, key)
	}
	blobRateLimitControlsMu.Unlock()

	blobRateLimitControlsWg.Wait()
	if stopped > 0 {
		traceShare(LogInfo, "stopped %d Blob rate-limit control(s)", stopped)
	}
}
