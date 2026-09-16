//go:build telemetrylive && telemetryperf && (windows || linux)

package azcopy

import "time"

type telemetryCLIProcessMetrics struct {
	WallSeconds              float64
	CPUSeconds               float64
	PeakWorkingSetBytes      uint64
	PeakCommitBytes          uint64 `json:",omitempty"`
	AverageWorkingSetBytes   float64
	AverageCommitBytes       float64 `json:",omitempty"`
	AverageAnonymousRSSBytes float64 `json:",omitempty"`
	PeakAnonymousRSSBytes    uint64  `json:",omitempty"`
	MemoryObservedSeconds    float64
	MemorySamples            int
	ResidentBreakdown        *telemetryResidentBreakdown `json:",omitempty"`
	ResidentSamples          []telemetryResidentSample   `json:",omitempty"`
}

type telemetryResidentBreakdown struct {
	TotalBytes          uint64
	PrivateRegionBytes  uint64
	ImageRegionBytes    uint64
	MappedRegionBytes   uint64
	UnknownRegionBytes  uint64
	ShareableImageBytes uint64
	ImageBytesByFile    map[string]uint64
}

type telemetryResidentSample struct {
	ElapsedSeconds       float64
	TotalBytes           uint64
	PrivateRegionBytes   uint64 `json:",omitempty"`
	ImageRegionBytes     uint64 `json:",omitempty"`
	MappedRegionBytes    uint64 `json:",omitempty"`
	WorkingSetBytes      uint64
	PrivateCommitBytes   uint64 `json:",omitempty"`
	ExecutableImageBytes uint64 `json:",omitempty"`
	AnonymousRSSBytes    uint64 `json:",omitempty"`
	FileRSSBytes         uint64 `json:",omitempty"`
	PSSBytes             uint64 `json:",omitempty"`
	PrivateRSSBytes      uint64 `json:",omitempty"`
	SwapBytes            uint64 `json:",omitempty"`
}

type telemetryMemoryAverage struct {
	lastElapsed        time.Duration
	lastWorkingSet     float64
	lastCommit         float64
	workingSetIntegral float64
	commitIntegral     float64
	observedSeconds    float64
	hasSample          bool
}

func (average *telemetryMemoryAverage) add(elapsed time.Duration, workingSet, commit uint64) {
	if average.hasSample {
		if elapsed <= average.lastElapsed {
			return
		}
		seconds := (elapsed - average.lastElapsed).Seconds()
		average.workingSetIntegral += (average.lastWorkingSet + float64(workingSet)) * 0.5 * seconds
		average.commitIntegral += (average.lastCommit + float64(commit)) * 0.5 * seconds
		average.observedSeconds += seconds
	}
	average.lastElapsed = elapsed
	average.lastWorkingSet = float64(workingSet)
	average.lastCommit = float64(commit)
	average.hasSample = true
}
