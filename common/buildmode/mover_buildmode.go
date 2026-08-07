//go:build mover
// +build mover

package buildmode

import (
	"os"
	"sync"
)

var IsMover = true

// High-performance mover profile. These values are consulted ONLY when MOVER_HIGH_PERF is set; every
// call site is guarded by `buildmode.IsMover && buildmode.HighPerf()`. When MOVER_HIGH_PERF is unset,
// the mover binary runs the original mover-default code path (env vars + generic defaults) unchanged.
const (
	highPerfTransportShards       = 64
	highPerfConcurrencyValue      = 10000
	highPerfConcurrentFiles       = 512
	highPerfConcurrentSchedulers  = 64
	highPerfConcurrentScan        = 32
	highPerfShuffleEnabled        = true
	highPerfShuffleThresholdParts = 1
	highPerfTransferChannelSize   = 200000
	highPerfChunkChannelSize      = 200000
	highPerfSyncMaxGoroutines     = 300000
)

var (
	highPerfOnce sync.Once
	highPerfVal  bool
)

// HighPerf reports whether the high-performance mover profile is active (MOVER_HIGH_PERF=true|1).
// Resolved once per process; the env var is not expected to change mid-run.
func HighPerf() bool {
	highPerfOnce.Do(func() {
		switch os.Getenv("MOVER_HIGH_PERF") {
		case "true", "TRUE", "True", "1":
			highPerfVal = true
		}
	})
	return highPerfVal
}

// The functions below return the high-perf profile values. They are consulted only on the high-perf
// path (callers gate on buildmode.HighPerf()); the mover-default path never calls them.

func ConcurrencyValue() int      { return highPerfConcurrencyValue }
func TransportShards() int       { return highPerfTransportShards }
func ConcurrentFiles() int       { return highPerfConcurrentFiles }
func ConcurrentSchedulers() int  { return highPerfConcurrentSchedulers }
func ConcurrentScan() int        { return highPerfConcurrentScan }
func ShuffleEnabled() bool       { return highPerfShuffleEnabled }
func ShuffleThresholdParts() int { return highPerfShuffleThresholdParts }
func TransferChannelSize() int   { return highPerfTransferChannelSize }
func ChunkChannelSize() int      { return highPerfChunkChannelSize }
func SyncMaxGoroutines() int     { return highPerfSyncMaxGoroutines }
