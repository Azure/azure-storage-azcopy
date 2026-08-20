package common

import (
	"encoding/xml"
	"time"
)

// ShareThrottlingStats represents the throttling statistics block returned
// by the GetShareStats API when x-ms-file-return-throttling-stats: true is sent.
// These fields are only available for Premium File Shares (service version >= 2020-08-04).
type ShareThrottlingStats struct {
	XMLName                  xml.Name  `xml:"ShareThrottlingStats"`
	StartTime                time.Time `xml:"StartTime"`
	EndTime                  time.Time `xml:"EndTime"`
	TotalRequestCount        int64     `xml:"TotalRequestCount"`
	TotalIngressBytes        int64     `xml:"TotalIngressBytes"`
	TotalEgressBytes         int64     `xml:"TotalEgressBytes"`
	IopsThrottledRequestCount int64    `xml:"IopsThrottledRequestCount"`
	IngressThrottledBytes    int64     `xml:"IngressThrottledBytes"`
	EgressThrottledBytes     int64     `xml:"EgressThrottledBytes"`
	IopsLimit                int64     `xml:"IopsLimit"`
	BandwidthLimitMiBps      int64     `xml:"BandwidthLimitMiBps"`
	BurstIosAvailable        int64     `xml:"BurstIosAvailable"`
	BurstIosLimit            int64     `xml:"BurstIosLimit"`
}

// ShareStatsResponse represents the full XML response body from GetShareStats.
type ShareStatsResponse struct {
	XMLName              xml.Name              `xml:"ShareStats"`
	ShareUsageBytes      int64                 `xml:"ShareUsageBytes"`
	ThrottlingStats      *ShareThrottlingStats `xml:"ShareThrottlingStats"`
}

// ShareStatsSnapshot holds computed delta-based metrics from two consecutive polls.
// It is the public output of the proactive stats poller.
type ShareStatsSnapshot struct {
	// Timestamp when this snapshot was computed.
	Timestamp time.Time

	// Whether the throttling stats block was present in the response.
	ThrottlingStatsAvailable bool

	// Current effective limits from the share.
	ShareBandwidthLimitMiBps int64
	IopsLimit                int64
	BurstIosAvailable        int64
	BurstIosLimit            int64

	// Computed average achieved egress throughput over the poll interval.
	// Derived from delta(TotalEgressBytes) / deltaT.
	AvgAchievedEgressBps float64

	// Computed average throttled egress rate over the poll interval.
	// Derived from delta(EgressThrottledBytes) / deltaT.
	AvgThrottledEgressBps float64

	// Delta of IOPS throttled request count in the poll interval.
	DeltaIopsThrottledRequestCount int64

	// Delta of egress throttled bytes in the poll interval.
	DeltaEgressThrottledBytes int64

	// Whether this snapshot is stale (poll failures exceeded 2 intervals).
	IsStale bool
}

// shareStatsSample is an internal structure holding raw counters from one poll.
type shareStatsSample struct {
	timestamp                 time.Time
	startTime                 time.Time // From ShareThrottlingStats.StartTime; used to detect counter resets.
	endTime                   time.Time // From ShareThrottlingStats.EndTime; used to measure the poll interval.
	totalEgressBytes          int64
	egressThrottledBytes      int64
	iopsThrottledRequestCount int64
	iopsLimit                 int64
	bandwidthLimitMiBps       int64
	burstIosAvailable         int64
	burstIosLimit             int64
	throttlingAvailable       bool
}
