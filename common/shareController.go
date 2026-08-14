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
	"net/url"
	"strings"
	"sync"
	"time"
)

// GetShareStatsPollInterval is how often each per-share controller polls the
// authoritative GetShareStats signal.
const GetShareStatsPollInterval = 30 * time.Second

// SharePacer is everything a per-share limiter must be: the DualRateSink the
// controller drives, the IOPSPacer callers acquire from, and a Closer. The ste
// dualTokenBucketPacer implements it and is injected via RegisterSharePacerFactory
// so this package (common) never depends on ste.
type SharePacer interface {
	DualRateSink
	IOPSPacer
	Close() error
}

// nullSharePacer is the default SharePacer used until ste registers the real
// token-bucket-backed one. It enforces nothing, so the per-share machinery is a
// safe no-op in any binary/test that has not wired the concrete pacer.
type nullSharePacer struct{}

func (nullSharePacer) UpdateTargetBytesPerSecond(int64)              {}
func (nullSharePacer) UpdateTargetIOPS(int64)                        {}
func (nullSharePacer) AcquireIO(context.Context, int64, int64) error { return nil }
func (nullSharePacer) Close() error                                  { return nil }

// TODO: GetShareStats API integration needed in this(depends on Veda changes)

// sharePacerFactory builds the per-share pacer. ste replaces it via
// RegisterSharePacerFactory with one that returns a real dual token-bucket pacer.
var sharePacerFactory func() SharePacer = func() SharePacer { return nullSharePacer{} }

// RegisterSharePacerFactory injects the concrete per-share pacer constructor
// (called once from ste's init). Passing nil is ignored.
func RegisterSharePacerFactory(f func() SharePacer) {
	if f != nil {
		sharePacerFactory = f
	}
}

// shareStatsSourceFactory builds the poll-based stats adapter for a share key.
// Until the real SDK-backed source is injected via RegisterResourceStatsSourceFactory
// it returns a stub reporting no limits (unlimited) and no throttles, keeping
// every per-share controller inert (no behavior change).
var shareStatsSourceFactory func(shareKey string) ResourceStatsSource = func(string) ResourceStatsSource {
	return stubResourceStatsSource{}
}

// RegisterResourceStatsSourceFactory injects the real poll-based stats adapter
// (e.g. Azure Files GetShareStats mapped onto ResourceStats). Passing nil is
// ignored.
func RegisterResourceStatsSourceFactory(f func(shareKey string) ResourceStatsSource) {
	if f != nil {
		shareStatsSourceFactory = f
	}
}

// stubResourceStatsSource is the placeholder stats adapter. Zero limits mean
// "unlimited" on both dimensions, and zero throttle counters keep the controller
// in proactive mode driving the pacer to 0 (null) rates.
type stubResourceStatsSource struct{}

func (stubResourceStatsSource) PollStats() (ResourceStats, error) {
	return ResourceStats{}, nil // all zero => unlimited, no throttles
}

// shareControl bundles the per-share pacer (the DualRateSink the controller
// drives) with its DualResourceController and the poll loop's cancel func.
type shareControl struct {
	pacer SharePacer
	ctrl  ResourceController
	stop  context.CancelFunc
}

// ControllerFactory builds the per-resource ResourceController for a resource
// key. It is swappable so a future Blob path can register NewBlobController
// without changing the registry or the core engine.
type ControllerFactory func(sink DualRateSink, source ResourceStatsSource, workers int64) ResourceController

// controllerFactory defaults to the AzureFiles strategy (the only active path
// today). Register a different factory to change how per-resource controllers
// are built.
var controllerFactory ControllerFactory = func(sink DualRateSink, source ResourceStatsSource, workers int64) ResourceController {
	return NewAzureFilesController(sink, source, workers, DefaultDualResourceConfig())
}

// RegisterControllerFactory injects the per-resource controller constructor
// (e.g. NewBlobController for a Blob path). Passing nil is ignored.
func RegisterControllerFactory(f ControllerFactory) {
	if f != nil {
		controllerFactory = f
	}
}

var (
	shareControlsMu sync.Mutex
	shareControls   = map[string]*shareControl{}
)

// getOrCreateShareControl returns the per-share control for shareKey, lazily
// creating it (and starting its GetShareStats poll loop) on first use. workers
// is the equal-share denominator used in proactive mode.
func getOrCreateShareControl(shareKey string, workers int64) *shareControl {
	if shareKey == "" {
		return nil
	}
	shareControlsMu.Lock()
	defer shareControlsMu.Unlock()

	if sc, ok := shareControls[shareKey]; ok {
		return sc
	}

	if workers < 1 {
		workers = 1
	}
	pacer := sharePacerFactory()
	ctrl := controllerFactory(pacer, shareStatsSourceFactory(shareKey), workers)

	ctx, cancel := context.WithCancel(context.Background())
	sc := &shareControl{pacer: pacer, ctrl: ctrl, stop: cancel}
	shareControls[shareKey] = sc

	// Prime the baseline and start the authoritative ~30s poll loop.
	_, _ = ctrl.Refresh()
	go pollShareStats(ctx, ctrl)

	return sc
}

// pollShareStats drives the authoritative stats refresh on a ticker,
// sleeping any backoff the controller requests.
func pollShareStats(ctx context.Context, ctrl ResourceController) {
	ticker := time.NewTicker(GetShareStatsPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if backoff, err := ctrl.Refresh(); err == nil && backoff > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
		}
	}
}

// GetOrCreateSharePacer returns the per-share pacer for shareURL (creating the
// share's dual-resource controller if needed), or nil for non-Files URLs so
// callers can fall back to their default pacer unchanged.
func GetOrCreateSharePacer(shareURL string, workers int64) SharePacer {
	sc := getOrCreateShareControl(ShareKeyFromRawURL(shareURL), workers)
	if sc == nil {
		return nil
	}
	return sc.pacer
}

// GetShareScanPacer returns the per-share IOPS pacer for the Azure Files share
// identified by shareURL, creating the share's dual-resource controller if
// needed. Enumeration (metadata) IOPS is then charged against the same
// per-share budget as the data plane. Returns nil for non-Files URLs so
// enumeration there stays unmetered.
func GetShareScanPacer(shareURL string) IOPSPacer {
	sc := getOrCreateShareControl(ShareKeyFromRawURL(shareURL), 1)
	if sc == nil {
		return nil
	}
	return sc.pacer
}

// HandleShareResponse feeds a single pre-classified HTTP throttle outcome into
// the per-share controller's fast reactive path. It is a no-op when no controller
// has been created for the share (e.g. non-Files traffic). The caller classifies
// the response (service-specific) into a ThrottleKind so common stays neutral.
func HandleShareResponse(shareKey string, kind ThrottleKind, retryAfterSec float64) {
	if shareKey == "" {
		return
	}
	shareControlsMu.Lock()
	sc := shareControls[shareKey]
	shareControlsMu.Unlock()
	if sc != nil {
		sc.ctrl.HandleResponse(kind, retryAfterSec)
	}
}

// ShareKeyFromRawURL derives a stable per-share identity (account host + share
// name) from an Azure Files URL, ignoring SAS/query and path beyond the share.
// It returns "" for non-Files URLs so callers can cheaply skip them.
func ShareKeyFromRawURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	if !strings.Contains(strings.ToLower(u.Host), ".file.") {
		return "" // only Azure Files endpoints carry share-level IOPS/bandwidth limits
	}
	seg := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(seg) == 0 || seg[0] == "" {
		return ""
	}
	return strings.ToLower(u.Host) + "/" + seg[0]
}
