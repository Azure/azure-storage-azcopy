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
