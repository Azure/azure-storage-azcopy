package common

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// azfileShareStatsSource implements ResourceStatsSource by polling the Azure
// Files GetShareStats API with x-ms-file-return-throttling-stats: true. It
// maps the XML ShareThrottlingStats response onto ResourceStats for consumption
// by the RateLimitController.
//
// Lifecycle: one instance per share key. Created by the factory registered via
// RegisterResourceStatsSourceFactory (in shareController.go on the throttle
// branch). The shareController poll loop calls PollStats() every 30s; this
// source does NOT run its own ticker.
type azfileShareStatsSource struct {
	provider *ShareStatsProvider

	mu         sync.Mutex
	prevSample *shareStatsSample // previous raw counters for delta/reset detection
}

// NewAzfileShareStatsSource creates a ResourceStatsSource for the given share.
// shareURL must include any SAS token required for auth. httpClient should be
// the global AzCopy HTTP client.
func NewAzfileShareStatsSource(shareURL string, httpClient *http.Client, logger ILogger) ResourceStatsSource {
	return newAzfileShareStatsSource(shareURL, httpClient, logger, nil)
}

// newAzfileShareStatsSource additionally takes the credential to use when shareURL
// carries no SAS.
func newAzfileShareStatsSource(shareURL string, httpClient *http.Client, logger ILogger, tokenCred azcore.TokenCredential) ResourceStatsSource {
	LogToJobLogWithPrefix(fmt.Sprintf("Initializing azfileShareStatsSource for %s", shareURL), LogInfo)
	return &azfileShareStatsSource{
		provider: NewShareStatsProvider(shareURL, httpClient, logger, tokenCred),
	}
}

// PollStats calls GetShareStats, maps the response to ResourceStats, and tracks
// cumulative counters across calls for delta detection by the controller.
//
// On the first call, it stores the baseline sample and returns the share limits
// with zero throttle counters (so the controller starts in proactive mode).
//
// On subsequent calls it returns:
//   - IopsLimit and BandwidthLimitBytesPerSec from the latest response
//   - IopsThrottleCount = raw cumulative IopsThrottledRequestCount
//   - BandwidthThrottleCount = raw cumulative EgressThrottledBytes
//
// The controller does its own delta computation, so we pass through the raw
// cumulative counters. Counter resets (detected via StartTime change) reset
// our baseline so the controller sees a zero delta for that interval.
func (s *azfileShareStatsSource) PollStats() (ResourceStats, error) {
	resp, err := s.provider.FetchStats(context.Background())
	if err != nil {
		return ResourceStats{}, fmt.Errorf("GetShareStats poll: %w", err)
	}

	if resp.ThrottlingStats == nil {
		// Non-premium share or header not honored — return unlimited/no throttles.
		TraceShareOnce("nothrottlingstats", LogError,
			"GetShareStats returned no ShareThrottlingStats block: share reports UNLIMITED and will never be paced. "+
				"Expected on a standard (non-provisioned) share, or when x-ms-version=%q is too old to honor "+
				"x-ms-file-return-throttling-stats",
			enum.EEnvironmentVariable.DefaultServiceApiVersion().Get())
		return ResourceStats{}, nil
	}

	ts := resp.ThrottlingStats

	s.mu.Lock()
	defer s.mu.Unlock()

	sample := &shareStatsSample{
		timestamp:                 time.Now(),
		startTime:                 ts.StartTime,
		endTime:                   ts.EndTime,
		totalEgressBytes:          ts.TotalEgressBytes,
		egressThrottledBytes:      ts.EgressThrottledBytes,
		iopsThrottledRequestCount: ts.IopsThrottledRequestCount,
		iopsLimit:                 ts.IopsLimit,
		bandwidthLimitMiBps:       ts.BandwidthLimitMiBps,
		burstIosAvailable:         ts.BurstIosAvailable,
		burstIosLimit:             ts.BurstIosLimit,
		throttlingAvailable:       true,
	}
	
	if s.prevSample == nil {
		// First poll only establishes the baseline; the controller's own `primed`
		// flag discards this interval's delta.
		s.prevSample = sample

		return ResourceStats{
			IopsLimit:                 ts.IopsLimit,
			BandwidthLimitBytesPerSec: ts.BandwidthLimitMiBps * 1024 * 1024,
			IopsThrottleCount:         ts.IopsThrottledRequestCount,
			BandwidthThrottleCount:    ts.EgressThrottledBytes,
		}, nil
	}

	// A changed StartTime means the service-side aggregator restarted and zeroed
	// its counters, so rebaseline rather than reporting a huge negative delta.
	if !s.prevSample.startTime.Equal(sample.startTime) {
		s.prevSample = sample
		return ResourceStats{
			IopsLimit:                 ts.IopsLimit,
			BandwidthLimitBytesPerSec: ts.BandwidthLimitMiBps * 1024 * 1024,
			IopsThrottleCount:         0,
			BandwidthThrottleCount:    0,
		}, nil
	}
	samplingPeriodInSecs := sample.endTime.Sub(s.prevSample.endTime).Seconds()
	
	if samplingPeriodInSecs >= 150 {
		LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] unexpected poll interval: %.1f seconds", samplingPeriodInSecs), LogError)
	}
	LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] current  %s", sample), LogDebug)
	LogToJobLogWithPrefix(fmt.Sprintf("[sharestats] previous %s", s.prevSample), LogDebug)

	s.prevSample = sample

	// Counters stay cumulative here; RateLimitController.Refresh computes the deltas.
	return ResourceStats{
		IopsLimit:                 ts.IopsLimit,
		BandwidthLimitBytesPerSec: ts.BandwidthLimitMiBps * 1024 * 1024,
		IopsThrottleCount:         ts.IopsThrottledRequestCount,
		BandwidthThrottleCount:    ts.EgressThrottledBytes,
	}, nil
}

// ShareStatsSourceFactory returns a factory function suitable for registration
// via RegisterResourceStatsSourceFactory. shareURL carries the SAS needed to
// authenticate the raw GetShareStats request; when it is empty the URL is
// rebuilt from the key, which only works for anonymously readable shares.
// tokenCred is used instead when the job authenticates with OAuth.
//
// Registered from jobsAdmin.MainSTE when Azure Files proactive stats are enabled.
func ShareStatsSourceFactory(httpClient *http.Client, logger ILogger) func(shareKey, shareURL string, tokenCred azcore.TokenCredential) ResourceStatsSource {		
	return func(shareKey, shareURL string, tokenCred azcore.TokenCredential) ResourceStatsSource {
		LogToJobLogWithPrefix(fmt.Sprintf("Initializing ShareStatsSourceFactory"), LogInfo)
		if shareURL == "" {
			shareURL = "https://" + shareKey
		}
		return newAzfileShareStatsSource(shareURL, httpClient, logger, tokenCred)
	}
}
