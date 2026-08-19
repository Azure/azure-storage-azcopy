package ste

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ResourceStats is the storage-service-agnostic view of a throttled resource's
// limits and cumulative throttle counters. This mirrors common.ResourceStats
// from the throttle branch (common/dualResourceController.go). When that branch
// merges, this type should be replaced with common.ResourceStats.
type ResourceStats struct {
	IopsLimit                 int64 // ops/sec; 0 = unlimited
	BandwidthLimitBytesPerSec int64 // bytes/sec; 0 = unlimited
	IopsThrottleCount         int64 // cumulative IOPS throttle counter
	BandwidthThrottleCount    int64 // cumulative bandwidth throttle counter
}

// ResourceStatsSource abstracts the authoritative, poll-based limits/throttle
// signal. This mirrors common.ResourceStatsSource from the throttle branch.
// The shareController poll loop calls PollStats() every 30s.
type ResourceStatsSource interface {
	PollStats() (ResourceStats, error)
}

// azfileShareStatsSource implements ResourceStatsSource by polling the Azure
// Files GetShareStats API with x-ms-file-return-throttling-stats: true. It
// maps the XML ShareThrottlingStats response onto ResourceStats for consumption
// by the DualResourceController.
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
func NewAzfileShareStatsSource(shareURL string, httpClient *http.Client, logger common.ILogger) ResourceStatsSource {
	return &azfileShareStatsSource{
		provider: NewShareStatsProvider(shareURL, httpClient, logger),
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

	if (s.prevSample == nil) {
		// First call: store baseline and return limits with zero throttle counters.
		s.prevSample = sample

		return ResourceStats{
			IopsLimit:                 ts.IopsLimit,
			BandwidthLimitBytesPerSec: ts.BandwidthLimitMiBps * 1024 * 1024,
			IopsThrottleCount:         ts.IopsThrottledRequestCount,
			BandwidthThrottleCount:    ts.EgressThrottledBytes,
		}, nil
	} else {
		// Detect counter reset: if StartTime changed, the aggregator restarted
		// and all cumulative counters were zeroed. Reset our baseline so the
		// controller sees a zero delta for this interval instead of a huge negative.
		if !s.prevSample.startTime.Equal(sample.startTime) {
			s.prevSample = sample
			return ResourceStats{
				IopsLimit:                 ts.IopsLimit,
				BandwidthLimitBytesPerSec: ts.BandwidthLimitMiBps * 1024 * 1024,
				IopsThrottleCount:         0, // reset baseline
				BandwidthThrottleCount:    0,
			}, nil
		}

		prevEndTime := s.prevSample.endTime
		curEndTime := sample.endTime
		timeDelta := curEndTime.Sub(prevEndTime).Seconds()		
		if (timeDelta > 20 && timeDelta < 60) {
			fmt.Sprintf("GetShareStats poll interval: %.1f seconds", timeDelta)			
		}
		currentIopsThrottleCount := sample.iopsThrottledRequestCount - s.prevSample.iopsThrottledRequestCount
		currentBandwidthThrottleCount := sample.egressThrottledBytes - s.prevSample.egressThrottledBytes
		s.prevSample = sample
		return ResourceStats{
			IopsLimit:                 ts.IopsLimit,
			BandwidthLimitBytesPerSec: ts.BandwidthLimitMiBps * 1024 * 1024,
			IopsThrottleCount:         currentIopsThrottleCount,
			BandwidthThrottleCount:    currentBandwidthThrottleCount,
		}, nil	
	}	
}

// ShareStatsSourceFactory returns a factory function suitable for registration
// via common.RegisterResourceStatsSourceFactory. It constructs an
// azfileShareStatsSource per share key using the provided HTTP client and logger.
//
// Integration (in ste/dualShareController.go init, once the throttle branch merges):
//
//	common.RegisterResourceStatsSourceFactory(
//	    ShareStatsSourceFactory(common.GlobalHTTPClient, logger),
//	)
func ShareStatsSourceFactory(httpClient *http.Client, logger common.ILogger) func(shareKey string) ResourceStatsSource {
	return func(shareKey string) ResourceStatsSource {
		// shareKey is "account.file.core.windows.net/sharename" (no scheme).
		// Reconstruct a full HTTPS URL for the raw HTTP call.
		shareURL := "https://" + shareKey
		return NewAzfileShareStatsSource(shareURL, httpClient, logger)
	}
}
