package pacer

import (
	"math"
	"sync"
	"sync/atomic"
)

var _ BandwidthRecorder = &bandwidthRecorder{}

type bandwidthRecorder struct {
	lock sync.RWMutex

	control chan bandwidthRecorderMessage

	hardLimit    atomic.Int64
	totalTraffic atomic.Int64
	buckets      *bucketRotator
}

func NewBandwidthRecorder(hardLimit int64, observationSeconds uint64) BandwidthRecorder {
	out := &bandwidthRecorder{
		lock:         sync.RWMutex{},
		control:      make(chan bandwidthRecorderMessage),
		hardLimit:    atomic.Int64{},
		totalTraffic: atomic.Int64{},
		buckets:      newBucketRotator(observationSeconds),
	}

	out.hardLimit.Store(hardLimit)

	return out
}

func (b *bandwidthRecorder) GetTotalTraffic() int64 {
	return b.totalTraffic.Load()
}

type BandwidthRecorderConfig struct {
	ObservationPeriodSeconds uint
	HardLimit                int
}

func (b *bandwidthRecorder) StartObservation() {
	b.control <- bandwidthRecorderMessage{messageType: pacerMessageStart}
}

func (b *bandwidthRecorder) PauseObservation() {
	b.control <- bandwidthRecorderMessage{messageType: pacerMessagePause}
}

func (b *bandwidthRecorder) RecordBytes(count int) {
	b.totalTraffic.Add(int64(count))
	b.buckets.AddToCurrentValue(uint64(max(count, 0)))
}

func (b *bandwidthRecorder) SetObservationPeriod(seconds uint) {
	b.buckets.SetSize(seconds)
}

func (b *bandwidthRecorder) ObservationPeriod() (seconds uint) {
	return b.buckets.Size()
}

func (b *bandwidthRecorder) Bandwidth() (bytesPerSecond int64, fullAverage bool) {
	return int64(min(b.buckets.GetAverage(), math.MaxInt64)), b.buckets.AverageReady()
}

func (b *bandwidthRecorder) HardLimit() (requested bool, bytesPerSecond int64) {
	return b.hardLimit.Load() != 0, b.hardLimit.Load()
}

func (b *bandwidthRecorder) RequestHardLimit(bytesPerSecond int64) {
	b.hardLimit.Store(bytesPerSecond)
}

func (b *bandwidthRecorder) ClearHardLimit() {
	b.RequestHardLimit(0)
}

type bucketRotator struct {
	bucketLock      *sync.RWMutex
	availableValues uint
	buckets         []atomic.Uint64
	currentBucket   uint
}

func newBucketRotator(size uint64) *bucketRotator {
	size++

	return &bucketRotator{
		bucketLock:    &sync.RWMutex{},
		buckets:       make([]atomic.Uint64, size),
		currentBucket: 0,
	}
}

func (b *bucketRotator) Rotate() {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()

	b.availableValues = min(b.Size(), b.availableValues+1)
	b.currentBucket = (b.currentBucket + 1) % uint(len(b.buckets))
	b.buckets[b.currentBucket].Store(0)
}

func (b *bucketRotator) AddToCurrentValue(val uint64) {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	b.buckets[b.currentBucket].Add(val)
}

func (b *bucketRotator) AverageReady() bool {
	return b.availableValues == b.Size() && // we must have a full set of buckets,
		b.Size() > 0 // and a size > 0
}

func (b *bucketRotator) GetAverage() uint64 {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	// if we have nothing to average, we have 0. oops!
	if b.availableValues == 0 {
		return 0
	}

	var sum uint64
	for idx := range int(b.availableValues) {
		// idx is not actually a raw index, but needs to be turned into one. It is, instead, the distance, in the reverse direction of rotations, from the current index.
		idx++                            // first, since the index we're reading is always going to be one back (the current value isn't realized), we need to step back one more
		idx = int(b.currentBucket) - idx // step backwards
		if idx < 0 {
			idx += len(b.buckets) // wrap around
		}

		// then, add the value of that bucket.
		sum += b.buckets[idx].Load()
	}

	var divisor = uint64(b.availableValues)

	return sum / divisor
}

func (b *bucketRotator) SetSize(size uint) {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()

	size += 1 // 1 for current element, there should never be 0 elements

	if size > uint(len(b.buckets)) {
		// insert between us and the tail
		var out []atomic.Uint64
		diff := size - uint(len(b.buckets))

		out = b.buckets[:b.currentBucket+1]
		out = append(out, make([]atomic.Uint64, diff)...)
		out = append(out, b.buckets[b.currentBucket+1:]...)

		b.buckets = out
	} else if size < uint(len(b.buckets)) {
		// We want to trim items from the list. Immediately after the write head is "stale", so we want to cut the least recent items.
		staleEnd := b.buckets[b.currentBucket+1:]
		freshEnd := b.buckets[:b.currentBucket]
		removing := uint(len(b.buckets)) - size

		var out []atomic.Uint64
		if removing < uint(len(staleEnd)) {
			out = append(freshEnd, b.buckets[b.currentBucket])
			out = append(out, staleEnd[removing:]...)
		} else {
			removing -= uint(len(staleEnd))

			out = append(freshEnd[removing:], b.buckets[b.currentBucket])
			b.currentBucket -= removing
		}

		b.availableValues = min(b.availableValues, uint(len(out)-1))
		b.buckets = out
	}
}

func (b *bucketRotator) Size() uint {
	return uint(len(b.buckets)) - 1
}
