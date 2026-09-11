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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
)

// GetShareStatsPollInterval is how often each per-share controller polls the
// authoritative GetShareStats signal.
const GetShareStatsPollInterval = 30 * time.Second

// SharePacer is everything a per-share limiter must be: the RateLimitSink the
// controller drives, the IOPSPacer callers acquire from, and a Closer. The ste
// rateLimitTokenBucketPacer implements it and is injected via RegisterSharePacerFactory
// so this package (common) never depends on ste.
type SharePacer interface {
	RateLimitSink
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

// traceShare emits a share-controller diagnostic to the job log. Nil-safe, so
// calls made before the job logger exists are simply dropped. The level check
// comes first because this runs on the per-share-control hot path.
func traceShare(level LogLevel, format string, a ...any) {
	if AzcopyCurrentJobLogger == nil || !AzcopyCurrentJobLogger.ShouldLog(level) {
		return
	}
	LogToJobLogWithPrefix("[sharecontroller] "+fmt.Sprintf(format, a...), level)
}

var (
	traceOnceMu     sync.Mutex
	traceOnceCounts = map[string]int64{}
)

// TraceShareOnce logs only the first occurrence of key, counting the rest for the
// shutdown summary. Needed because the opt-out call sites run once per traverser
// (i.e. per directory) and once per transfer.
func TraceShareOnce(key string, level LogLevel, format string, a ...any) {
	traceOnceMu.Lock()
	n := traceOnceCounts[key] + 1
	traceOnceCounts[key] = n
	traceOnceMu.Unlock()

	if n == 1 {
		traceShare(level, format, a...)
	}
}

// logTraceOnceSummary reports how often each suppressed reason actually fired.
func logTraceOnceSummary() {
	traceOnceMu.Lock()
	counts := make(map[string]int64, len(traceOnceCounts))
	for k, v := range traceOnceCounts {
		counts[k] = v
	}
	traceOnceMu.Unlock()

	for k, v := range counts {
		traceShare(LogInfo, "opt-out summary: %s occurred %d time(s)", k, v)
	}
}

// Records whether the concrete implementations were injected, so a share that
// silently ends up inert can be told apart from one that is genuinely unlimited.
var (
	sharePacerFactoryRegistered  bool
	statsSourceFactoryRegistered bool
)

// RegisterSharePacerFactory injects the concrete per-share pacer constructor
// (called once from ste's init). Passing nil is ignored.
func RegisterSharePacerFactory(f func() SharePacer) {
	if f != nil {
		sharePacerFactory = f
		sharePacerFactoryRegistered = true
	}
}

// shareStatsSourceFactory builds the poll-based stats adapter for a share key.
// shareURL carries the scheme, host, share name and any SAS; tokenCred supplies
// OAuth instead, since the stats poller issues a raw HTTP request.
// Until the real SDK-backed source is injected via RegisterResourceStatsSourceFactory
// it returns a stub reporting no limits (unlimited) and no throttles, keeping
// every per-share controller inert (no behavior change).
var shareStatsSourceFactory func(shareKey, shareURL string, tokenCred azcore.TokenCredential) ResourceStatsSource = func(string, string, azcore.TokenCredential) ResourceStatsSource {
	return stubResourceStatsSource{}
}

// RegisterResourceStatsSourceFactory injects the real poll-based stats adapter
// (e.g. Azure Files GetShareStats mapped onto ResourceStats). Passing nil is
// ignored.
func RegisterResourceStatsSourceFactory(f func(shareKey, shareURL string, tokenCred azcore.TokenCredential) ResourceStatsSource) {
	if f == nil {
		GetLifecycleMgr().Info("resource stats source factory not registered: factory is nil, shares stay unlimited")
		traceShare(LogDebug, "resource stats source factory not registered: factory is nil, shares stay unlimited")
		return
	}
	shareStatsSourceFactory = f
	statsSourceFactoryRegistered = true
	GetLifecycleMgr().Info("registered resource stats source factory")
	traceShare(LogDebug, "registered resource stats source factory")
}

// stubResourceStatsSource is the placeholder stats adapter. Zero limits mean
// "unlimited" on both dimensions, and zero throttle counters keep the controller
// in proactive mode driving the pacer to 0 (null) rates.
type stubResourceStatsSource struct{}

func (stubResourceStatsSource) PollStats() (ResourceStats, error) {
	return ResourceStats{}, nil // all zero => unlimited, no throttles
}

// shareControl bundles the per-share pacer (the RateLimitSink the controller
// drives) with its RateLimitController and the poll loop's cancel func.
type shareControl struct {
	pacer SharePacer
	ctrl  ResourceController
	stop  context.CancelFunc
}

// ControllerFactory builds the per-resource ResourceController for a resource
// key. It is swappable so a future Blob path can register NewBlobController
// without changing the registry or the core engine.
type ControllerFactory func(sink RateLimitSink, source ResourceStatsSource, workers int64) ResourceController

// controllerFactory defaults to the AzureFiles strategy (the only active path
// today). Register a different factory to change how per-resource controllers
// are built.
var controllerFactory ControllerFactory = func(sink RateLimitSink, source ResourceStatsSource, workers int64) ResourceController {
	return NewAzureFilesController(sink, source, int(workers), DefaultRateLimitConfig())
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
	shareControlsWg sync.WaitGroup
)

// Fallback OAuth credentials for the GetShareStats poll, keyed by account host.
// Needed because the transfer-side callers (the Azure Files sender, the source
// info provider) have no credential to hand down: it is consumed when the
// service clients are built and never stored on the job part manager.
var (
	shareStatsCredsMu sync.RWMutex
	shareStatsCreds   = map[string]azcore.TokenCredential{}
)

// RegisterShareStatsCredential records the credential to poll host with when a
// share control is created without an explicit one. Nil/empty input is ignored,
// and the first registration for an account wins.
func RegisterShareStatsCredential(host string, tokenCred azcore.TokenCredential) {
	if host == "" || tokenCred == nil {
		return
	}
	host = strings.ToLower(host)

	// Service clients are rebuilt per traverser, so skip the write lock on the
	// overwhelmingly common repeat registration.
	shareStatsCredsMu.RLock()
	_, exists := shareStatsCreds[host]
	shareStatsCredsMu.RUnlock()
	if exists {
		return
	}

	shareStatsCredsMu.Lock()
	_, exists = shareStatsCreds[host]
	if !exists {
		shareStatsCreds[host] = tokenCred
	}
	shareStatsCredsMu.Unlock()

	if !exists {
		traceShare(LogDebug, "registered share stats credential for host %q", host)
	}
}

// GetShareCredential returns the credential registered for shareKey's account,
// or nil when none was (e.g. a SAS-authenticated job).
func GetShareCredential(shareKey string) azcore.TokenCredential {
	host, _, _ := strings.Cut(shareKey, "/")
	shareStatsCredsMu.RLock()
	defer shareStatsCredsMu.RUnlock()
	return shareStatsCreds[strings.ToLower(host)]
}

// getOrCreateShareControl returns the per-share control for rawURL, lazily
// creating it (and starting its GetShareStats poll loop) on first use. Both the
// share key and the SAS-bearing share URL are derived from rawURL. workers is
// the equal-share denominator used in proactive mode. Returns nil for non-Files
// URLs so callers can fall back to their default pacer unchanged.
func getOrCreateShareControl(rawURL string, workers int64) *shareControl {
	shareKey, reason := classifyShareKey(rawURL)
	if shareKey == "" {
		// The job logger redacts SAS, so rawURL is safe to include here.
		TraceShareOnce("nokey:"+reason, LogInfo,
			"no share control, per-share rate limiting disabled for this resource: %s (%s)", reason, rawURL)
		return nil
	}
	shareControlsMu.Lock()

	if sc, ok := shareControls[shareKey]; ok {
		shareControlsMu.Unlock()		
		return sc
	}

	if workers < 1 {
		workers = 1
	}
	pacer := sharePacerFactory()
	statsSource := shareStatsSourceFactory(shareKey, ShareURLFromRawURL(rawURL), GetShareCredential(shareKey))
	ctrl := controllerFactory(pacer, statsSource, workers)

	traceWiringOnce(pacer, statsSource, shareKey)

	ctx, cancel := context.WithCancel(context.Background())
	sc := &shareControl{pacer: pacer, ctrl: ctrl, stop: cancel}
	shareControls[shareKey] = sc
	shareControlsMu.Unlock()

	// Primed before the poller starts, otherwise the pacer sits at its unlimited
	// (0,0) target until the first tick and the job's opening window is unpaced.
	if _, err := ctrl.Refresh(); err != nil {
		traceShare(LogError, "priming poll failed for %s: %v", shareKey, err)
	}
	traceShare(LogInfo, "ShareController initialized for %s: mode=%s workers=%d primedRates(%s)",
		shareKey, ctrl.Mode(), workers, describeRates(ctrl))

	go startShareStatsPoller(ctx, shareKey, ctrl)
	return sc
}

// traceWiringOnce reports whether the concrete pacer and stats source were
// injected. Without this a share wired to the no-op stubs is indistinguishable
// from one the service reports as genuinely unlimited.
func traceWiringOnce(pacer SharePacer, statsSource ResourceStatsSource, shareKey string) {
	_, pacerIsNull := pacer.(nullSharePacer)
	_, sourceIsStub := statsSource.(stubResourceStatsSource)

	TraceShareOnce(fmt.Sprintf("wiring:%t:%t", pacerIsNull, sourceIsStub), LogInfo,
		"wiring for %s: sharePacer=%T (registered=%t) statsSource=%T (registered=%t)%s",
		shareKey, pacer, sharePacerFactoryRegistered, statsSource, statsSourceFactoryRegistered,
		ternaryStr(pacerIsNull || sourceIsStub,
			" -- INERT: no-op stub in use, this share will never be paced", ""))
}

func ternaryStr(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

// startShareStatsPoller runs the share's poll loop on a tracked goroutine. A
// panic is contained to this share rather than taking down the process.
func startShareStatsPoller(ctx context.Context, shareKey string, ctrl ResourceController) {
	shareControlsWg.Add(1)
	go func() {
		defer shareControlsWg.Done()
		defer func() {
			if r := recover(); r != nil {
				traceShare(LogError, "poll loop for %s stopped after panic, this share will never update again: %v", shareKey, r)
			}
		}()
		pollShareStats(ctx, shareKey, ctrl)
	}()
}

// StopShareControls cancels every share's poll loop, releases its pacer, and
// waits for the goroutines to exit. Idempotent.
func StopShareControls() {
	shareControlsMu.Lock()
	stopped := len(shareControls)
	for key, sc := range shareControls {
		sc.stop()
		_ = sc.pacer.Close()
		delete(shareControls, key)
	}
	shareControlsMu.Unlock()

	shareControlsWg.Wait()
	if stopped > 0 {
		traceShare(LogInfo , "stopped %d share control(s)", stopped)
	}
	logTraceOnceSummary()
}

// controllerRates reads the currently enforced targets when the controller
// exposes them. 0 means unlimited, i.e. nothing is actually being paced.
func controllerRates(ctrl ResourceController) (iops, bw int64, ok bool) {
	if r, ok := ctrl.(interface {
		IopsRate() int64
		BandwidthRate() int64
	}); ok {
		return r.IopsRate(), r.BandwidthRate(), true
	}
	return 0, 0, false
}

// describeRates renders the enforced targets, keeping "controller does not report
// rates" distinct from "rates are genuinely 0 (unlimited)".
func describeRates(ctrl ResourceController) string {
	iops, bw, ok := controllerRates(ctrl)
	if !ok {
		return fmt.Sprintf("unknown: %T does not expose IopsRate/BandwidthRate", ctrl)
	}
	if iops == 0 && bw == 0 {
		return "iops=0 bw=0 -- both UNLIMITED, nothing is being paced"
	}
	return fmt.Sprintf("iops=%d bw=%d", iops, bw)
}

// pollShareStats drives the authoritative stats refresh on a ticker,
// sleeping any backoff the controller requests.
func pollShareStats(ctx context.Context, shareKey string, ctrl ResourceController) {
	traceShare(LogDebug, "poll loop started for %s (interval=%s)", shareKey, GetShareStatsPollInterval)
	ticker := time.NewTicker(GetShareStatsPollInterval)
	defer ticker.Stop()
	tick := 0
	consecutiveFailures := 0
	for {
		select {
		case <-ctx.Done():
			traceShare(LogDebug, "poll loop stopped for %s after %d tick(s)", shareKey, tick)
			return
		case <-ticker.C:
			tick++
			backoff, err := ctrl.Refresh()
			if err != nil {
				consecutiveFailures++
				// Escalate on the first failure and on every 10th after that: a poll
				// that always fails leaves the pacer frozen at its primed targets.
				if consecutiveFailures == 1 || consecutiveFailures%10 == 0 {
					traceShare(LogError, "poll #%d failed for %s (%d consecutive): %v",
						tick, shareKey, consecutiveFailures, err)
				}
				continue
			}
			if consecutiveFailures > 0 {
				traceShare(LogInfo, "poll for %s recovered after %d consecutive failure(s)", shareKey, consecutiveFailures)
				consecutiveFailures = 0
			}
			traceShare(LogDebug, "poll #%d for %s: mode=%s rates(%s) backoff=%s",
				tick, shareKey, ctrl.Mode(), describeRates(ctrl), backoff)
			if backoff > 0 {
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
	sc := getOrCreateShareControl(shareURL, workers)
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
	sc := getOrCreateShareControl(shareURL, 1)
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
	key, _ := classifyShareKey(raw)
	return key
}

// Reasons a URL yields no share key. Reported so a job that silently opts out of
// per-share rate limiting can be told apart from one that is genuinely not Files.
const (
	shareKeyParseFailed  = "URL is not parseable as an Azure Files URL"
	shareKeyNoShareName  = "URL has no share segment (account- or service-level URL)"
	shareKeyNotFilesHost = `host is not an Azure Files endpoint (no ".file." label)`
)

// classifyShareKey returns the share key, or "" plus the reason it was rejected.
func classifyShareKey(raw string) (key, reason string) {
	parts, err := file.ParseURL(raw)
	if err != nil {
		return "", shareKeyParseFailed
	}
	if parts.ShareName == "" {
		return "", shareKeyNoShareName
	}
	if !strings.Contains(strings.ToLower(parts.Host), ".file.") {
		return "", shareKeyNotFilesHost // only Azure Files endpoints carry share-level IOPS/bandwidth limits
	}
	return strings.ToLower(parts.Host) + "/" + parts.ShareName, ""
}

// ShareURLFromRawURL returns the share-scoped URL (scheme, host, share name and
// any SAS) for an Azure Files URL. The stats poller issues a raw HTTP request
// with no credential of its own, so the SAS must be preserved here. Returns ""
// for non-Files URLs.
func ShareURLFromRawURL(raw string) string {
	parts, err := file.ParseURL(raw)
	if err != nil || parts.ShareName == "" {
		return ""
	}
	if !strings.Contains(strings.ToLower(parts.Host), ".file.") {
		return ""
	}
	// GetShareStats rejects sharesnapshot; throttling limits belong to the live share.
	parts.ShareSnapshot = ""
	parts.DirectoryOrFilePath = ""
	return parts.String()
}
