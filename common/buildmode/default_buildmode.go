//go:build !mover
// +build !mover

package buildmode

// IsMover always returns false when 'mover' build tag is not defined.
var IsMover = false

// Non-mover stubs. These keep shared call sites compiling in !mover builds.
// They are never executed because every call site checks buildmode.IsMover first.
func HighPerf() bool             { return false }
func ConcurrencyValue() int      { return 0 }
func TransportShards() int       { return 0 }
func ConcurrentFiles() int       { return 0 }
func ConcurrentSchedulers() int  { return 0 }
func ConcurrentScan() int        { return 0 }
func ShuffleEnabled() bool       { return false }
func ShuffleThresholdParts() int { return 0 }
func TransferChannelSize() int   { return 0 }
func ChunkChannelSize() int      { return 0 }
func SyncMaxGoroutines() int     { return 0 }
