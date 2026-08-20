package common

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger is a minimal ILogger for testing.
type sourceTestLogger struct{}

func (sourceTestLogger) ShouldLog(_ LogLevel) bool { return false }
func (sourceTestLogger) Log(_ LogLevel, _ string)  {}
func (sourceTestLogger) Panic(err error)           { panic(err) }

// ============================================================================
// XML parsing
// ============================================================================

func TestXMLParsing_WithThrottlingBlock(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<ShareStats>
  <ShareUsageBytes>8189134192</ShareUsageBytes>
  <ShareThrottlingStats>
    <StartTime>2026-01-01T00:00:00Z</StartTime>
    <EndTime>2026-01-01T00:30:00Z</EndTime>
    <TotalRequestCount>50000</TotalRequestCount>
    <TotalIngressBytes>1073741824</TotalIngressBytes>
    <TotalEgressBytes>2147483648</TotalEgressBytes>
    <IopsThrottledRequestCount>100</IopsThrottledRequestCount>
    <IngressThrottledBytes>5242880</IngressThrottledBytes>
    <EgressThrottledBytes>10485760</EgressThrottledBytes>
    <IopsLimit>20000</IopsLimit>
    <BandwidthLimitMiBps>1000</BandwidthLimitMiBps>
    <BurstIosAvailable>5000</BurstIosAvailable>
    <BurstIosLimit>10000</BurstIosLimit>
  </ShareThrottlingStats>
</ShareStats>`

	var resp ShareStatsResponse
	err := xml.Unmarshal([]byte(body), &resp)
	require.NoError(t, err)
	require.NotNil(t, resp.ThrottlingStats)
	assert.Equal(t, int64(20000), resp.ThrottlingStats.IopsLimit)
	assert.Equal(t, int64(1000), resp.ThrottlingStats.BandwidthLimitMiBps)
	assert.Equal(t, int64(100), resp.ThrottlingStats.IopsThrottledRequestCount)
	assert.Equal(t, int64(10485760), resp.ThrottlingStats.EgressThrottledBytes)
}

func TestXMLParsing_WithoutThrottlingBlock(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<ShareStats>
  <ShareUsageBytes>100</ShareUsageBytes>
</ShareStats>`

	var resp ShareStatsResponse
	err := xml.Unmarshal([]byte(body), &resp)
	require.NoError(t, err)
	assert.Nil(t, resp.ThrottlingStats)
}

// ============================================================================
// PollStats — first call (baseline)
// ============================================================================

func TestPollStats_FirstCall_ReturnsLimitsWithZeroThrottles(t *testing.T) {
	server := newFakeShareStatsServer(t, "2026-01-01T00:00:00Z", 20000, 500, 0, 0)
	defer server.Close()

	src := NewAzfileShareStatsSource(server.URL+"/myshare", server.Client(), sourceTestLogger{})
	stats, err := src.PollStats()
	require.NoError(t, err)

	assert.Equal(t, int64(20000), stats.IopsLimit)
	assert.Equal(t, int64(500*1024*1024), stats.BandwidthLimitBytesPerSec)
	assert.Equal(t, int64(0), stats.IopsThrottleCount)
	assert.Equal(t, int64(0), stats.BandwidthThrottleCount)
}

// ============================================================================
// PollStats — second call (cumulative counters returned)
// ============================================================================

func TestPollStats_SecondCall_ReturnsCumulativeCounters(t *testing.T) {
	callCount := int32(0)
	startTime := "2026-01-01T00:00:00Z"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		iopsThrottled := int64(n) * 50
		egressThrottled := int64(n) * 1000

		body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<ShareStats>
  <ShareUsageBytes>1000</ShareUsageBytes>
  <ShareThrottlingStats>
    <StartTime>%s</StartTime>
    <EndTime>%s</EndTime>
    <TotalRequestCount>%d</TotalRequestCount>
    <TotalIngressBytes>0</TotalIngressBytes>
    <TotalEgressBytes>%d</TotalEgressBytes>
    <IopsThrottledRequestCount>%d</IopsThrottledRequestCount>
    <IngressThrottledBytes>0</IngressThrottledBytes>
    <EgressThrottledBytes>%d</EgressThrottledBytes>
    <IopsLimit>20000</IopsLimit>
    <BandwidthLimitMiBps>500</BandwidthLimitMiBps>
    <BurstIosAvailable>3000</BurstIosAvailable>
    <BurstIosLimit>10000</BurstIosLimit>
  </ShareThrottlingStats>
</ShareStats>`, startTime, time.Now().UTC().Format(time.RFC3339),
			n*1000, n*100000, iopsThrottled, egressThrottled)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	defer server.Close()

	src := NewAzfileShareStatsSource(server.URL+"/myshare", server.Client(), sourceTestLogger{})

	// First call — baseline.
	_, err := src.PollStats()
	require.NoError(t, err)

	// Second call — should have cumulative counters from the second response.
	stats, err := src.PollStats()
	require.NoError(t, err)

	assert.Equal(t, int64(20000), stats.IopsLimit)
	assert.Equal(t, int64(500*1024*1024), stats.BandwidthLimitBytesPerSec)
	// Second call: iopsThrottled = 2*50 = 100, egressThrottled = 2*1000 = 2000
	assert.Equal(t, int64(100), stats.IopsThrottleCount)
	assert.Equal(t, int64(2000), stats.BandwidthThrottleCount)
}

// ============================================================================
// PollStats — counter reset (StartTime changes)
// ============================================================================

func TestPollStats_CounterReset_ZerosThrottleCounts(t *testing.T) {
	callCount := int32(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		// On the second call, change StartTime to simulate aggregator restart.
		startTime := "2026-01-01T00:00:00Z"
		if n >= 2 {
			startTime = "2026-01-01T01:00:00Z"
		}

		body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<ShareStats>
  <ShareUsageBytes>1000</ShareUsageBytes>
  <ShareThrottlingStats>
    <StartTime>%s</StartTime>
    <EndTime>%s</EndTime>
    <TotalRequestCount>1000</TotalRequestCount>
    <TotalIngressBytes>0</TotalIngressBytes>
    <TotalEgressBytes>50000</TotalEgressBytes>
    <IopsThrottledRequestCount>500</IopsThrottledRequestCount>
    <IngressThrottledBytes>0</IngressThrottledBytes>
    <EgressThrottledBytes>9999</EgressThrottledBytes>
    <IopsLimit>20000</IopsLimit>
    <BandwidthLimitMiBps>500</BandwidthLimitMiBps>
    <BurstIosAvailable>3000</BurstIosAvailable>
    <BurstIosLimit>10000</BurstIosLimit>
  </ShareThrottlingStats>
</ShareStats>`, startTime, time.Now().UTC().Format(time.RFC3339))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	defer server.Close()

	src := NewAzfileShareStatsSource(server.URL+"/myshare", server.Client(), sourceTestLogger{})

	// First call — baseline.
	_, err := src.PollStats()
	require.NoError(t, err)

	// Second call — StartTime changed → counter reset.
	stats, err := src.PollStats()
	require.NoError(t, err)

	assert.Equal(t, int64(20000), stats.IopsLimit)
	assert.Equal(t, int64(500*1024*1024), stats.BandwidthLimitBytesPerSec)
	// After reset, throttle counts should be zero.
	assert.Equal(t, int64(0), stats.IopsThrottleCount)
	assert.Equal(t, int64(0), stats.BandwidthThrottleCount)
}

// ============================================================================
// PollStats — no throttling block (non-premium share)
// ============================================================================

func TestPollStats_NoThrottlingBlock_ReturnsZeroLimits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<ShareStats><ShareUsageBytes>100</ShareUsageBytes></ShareStats>`))
	}))
	defer server.Close()

	src := NewAzfileShareStatsSource(server.URL+"/myshare", server.Client(), sourceTestLogger{})
	stats, err := src.PollStats()
	require.NoError(t, err)

	// All zeros = unlimited, no throttles.
	assert.Equal(t, int64(0), stats.IopsLimit)
	assert.Equal(t, int64(0), stats.BandwidthLimitBytesPerSec)
	assert.Equal(t, int64(0), stats.IopsThrottleCount)
	assert.Equal(t, int64(0), stats.BandwidthThrottleCount)
}

// ============================================================================
// PollStats — HTTP error
// ============================================================================

func TestPollStats_HTTPError_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	src := NewAzfileShareStatsSource(server.URL+"/myshare", server.Client(), sourceTestLogger{})
	_, err := src.PollStats()
	assert.Error(t, err)
}

// ============================================================================
// PollStats — verifies x-ms-file-return-throttling-stats header is sent
// ============================================================================

func TestPollStats_SendsThrottlingHeader(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("x-ms-file-return-throttling-stats")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<ShareStats><ShareUsageBytes>1</ShareUsageBytes></ShareStats>`))
	}))
	defer server.Close()

	src := NewAzfileShareStatsSource(server.URL+"/myshare", server.Client(), sourceTestLogger{})
	_, _ = src.PollStats()
	assert.Equal(t, "true", gotHeader)
}

// ============================================================================
// Factory
// ============================================================================

func TestShareStatsSourceFactory_CreatesSource(t *testing.T) {
	server := newFakeShareStatsServer(t, "2026-01-01T00:00:00Z", 10000, 200, 0, 0)
	defer server.Close()

	// The factory reconstructs the URL from the share key. For testing we need
	// the server URL, so we strip the scheme that the factory will re-add.
	key := server.URL[len("http://"):] + "/myshare"
	factory := ShareStatsSourceFactory(server.Client(), sourceTestLogger{})

	src := factory(key)
	require.NotNil(t, src)

	// Since the test server uses http://, but the factory prepends https://,
	// this would fail to connect. Instead just verify the factory returns a
	// non-nil source (the type assertion proves it's our implementation).
	_, ok := src.(*azfileShareStatsSource)
	assert.True(t, ok)
}

// ============================================================================
// Helpers
// ============================================================================

func newFakeShareStatsServer(t *testing.T, startTime string, iopsLimit, bwLimitMiBps, iopsThrottled, egressThrottled int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<ShareStats>
  <ShareUsageBytes>1000</ShareUsageBytes>
  <ShareThrottlingStats>
    <StartTime>%s</StartTime>
    <EndTime>%s</EndTime>
    <TotalRequestCount>1000</TotalRequestCount>
    <TotalIngressBytes>0</TotalIngressBytes>
    <TotalEgressBytes>500000</TotalEgressBytes>
    <IopsThrottledRequestCount>%d</IopsThrottledRequestCount>
    <IngressThrottledBytes>0</IngressThrottledBytes>
    <EgressThrottledBytes>%d</EgressThrottledBytes>
    <IopsLimit>%d</IopsLimit>
    <BandwidthLimitMiBps>%d</BandwidthLimitMiBps>
    <BurstIosAvailable>3000</BurstIosAvailable>
    <BurstIosLimit>10000</BurstIosLimit>
  </ShareThrottlingStats>
</ShareStats>`, startTime, time.Now().UTC().Format(time.RFC3339),
			iopsThrottled, egressThrottled, iopsLimit, bwLimitMiBps)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
}
