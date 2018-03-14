package common

import (
	"sync/atomic"
	"time"
)

func NewCountPerSecond() CountPerSecond {
	cps := countPerSecond{}
	cps.Reset()
	return &cps
}

// CountPerSecond ...
type CountPerSecond interface {
	// Add atomically adds delta to *addr and returns the new value.
	// To subtract a signed positive constant value c, do Add(^uint64(c-1)).
	Add(delta uint64) uint64	// Pass 0 to get the current count value
	LatestRate() float64
	Reset()
}

type countPerSecond struct {
	nocopy NoCopy
	start int64 // Unix time allowing atomic update: Seconds since 1/1/1970
	count uint64
}

func (cps *countPerSecond) Add(delta uint64) uint64 {
	cps.nocopy.Check()
	return atomic.AddUint64(&cps.count, delta)
}

func (cps *countPerSecond) LatestRate() float64 {
	cps.nocopy.Check()
	dur := time.Now().Sub(time.Unix(cps.start, 0))
	if dur <= 0 {
		dur = 1
	}
	return float64(atomic.LoadUint64(&cps.count)) / dur.Seconds()
}

func (cps *countPerSecond) Reset() {
	cps.nocopy.Check()
	atomic.StoreInt64(&cps.start, time.Now().Unix())
	atomic.StoreUint64(&cps.count, 0)
}
