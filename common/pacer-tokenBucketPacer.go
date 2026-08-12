// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Pacer is used by callers whose activity must be controlled to a certain pace
type Pacer interface {

	// RequestTrafficAllocation blocks until the caller is allowed to process byteCount bytes.
	RequestTrafficAllocation(ctx context.Context, byteCount int64) error

	UpdateTargetBytesPerSecond(newTarget int64)

	// UndoRequest reverses a previous request to process n bytes.  Is used when
	// the caller did not need all of the allocation they previously requested
	// e.g. when they asked for enough for a big buffer, but never filled it, they would
	// call this method to return the unused portion.
	UndoRequest(byteCount int64)

	Close() error
}

type PacerAdmin interface {
	Pacer

	// GetTotalTraffic returns the cumulative count of all traffic that has been processed
	GetTotalTraffic() int64
}

const (
	// How long to sleep in the loop that puts tokens into the bucket
	bucketFillSleepDuration = time.Duration(float32(time.Second) * 0.1)

	// How long to sleep when reading from the bucket and finding there's not enough tokens
	bucketDrainSleepDuration = time.Duration(float32(time.Second) * 0.333)

	// Controls the max amount by which the contents of the token bucket can build up, unused.
	maxSecondsToOverpopulateBucket = 2.5 // had 5, when doing coarse-grained pacing. TODO: find best all-round value, or parameterize

	// DeadBandDuration is the minimum time between target-rate updates that pacers built on
	// TokenBucketPacer will honour. Shared here because it's used both by TokenBucketPacer's own
	// pacerBody (for live --cap-mbps updates) and by ste's autoTokenBucketPacer (for AIMD peak-decrease
	// throttling), which embeds TokenBucketPacer directly.
	// TODO: review this rather generous value.  Might not be needed if we can pace the internal retry efforts inside the retryPolices, because we (presumably) won't get such big flurries of 503s if we do that
	DeadBandDuration = 20 * time.Second
)

// TokenBucketPacer allows us to control the pace of an activity, using a basic token bucket algorithm.
// The target rate is fixed, but can be modified at any time through UpdateTargetBytesPerSecond
type TokenBucketPacer struct {
	atomicTokenBucket          int64
	atomicTargetBytesPerSecond int64
	atomicGrandTotal           int64
	atomicWaitCount            int64
	expectedBytesPerRequest    int64
	newTargetBytesPerSecond    chan int64
	done                       chan struct{}
}

func NewTokenBucketPacer(bytesPerSecond int64, expectedBytesPerCoarseRequest int64) *TokenBucketPacer {
	p := &TokenBucketPacer{atomicTokenBucket: bytesPerSecond / 4, // seed it immediately with part-of-a-second's worth, to avoid a sluggish start
		atomicTargetBytesPerSecond: bytesPerSecond,
		expectedBytesPerRequest:    int64(expectedBytesPerCoarseRequest),
		done:                       make(chan struct{}),
		newTargetBytesPerSecond:    make(chan int64),
	}

	go p.pacerBody()

	return p
}

// RequestTrafficAllocation function is called by goroutines to request right to send a certain amount of bytes.
// It controls their rate by blocking until they are allowed to proceed
func (p *TokenBucketPacer) RequestTrafficAllocation(ctx context.Context, byteCount int64) error {
	targetBytes := p.TargetBytesPerSecond()
	//if targetBytesIsZero, we have a null pacer, we just track GrandTotal
	if targetBytes == 0 {
		atomic.AddInt64(&p.atomicGrandTotal, byteCount)
		return nil
	}

	// Wait until p.TargetBytesPerSecond() is positive or zero
	// A negative value indicates the pacer is temporarily disabled/throttled
	// Prevents token allocation errors and CPU waste from busy-waiting on invalid targets
	for p.TargetBytesPerSecond() < 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second * 5): // sleep for five seconds
		}
	}

	if targetBytes < byteCount {
		return errors.New("request size greater than pacer target. ensure --block-size-mb is smaller than --cap-mbps and retry the transfer")
	}

	// block until tokens are available
	for atomic.AddInt64(&p.atomicTokenBucket, -byteCount) < 0 {

		// by taking our desired count we've moved below zero, which means our allocation is not available
		// right now, so put back what we asked for, and then wait
		atomic.AddInt64(&p.atomicTokenBucket, byteCount)

		// vary the wait amount, to reduce risk of any kind of pulsing or synchronization effect, without the perf and
		// and threadsafety issues of actual random numbers
		totalWaitsSoFar := atomic.AddInt64(&p.atomicWaitCount, 1)
		modifiedSleepDuration := time.Duration(float32(bucketDrainSleepDuration) * (float32(totalWaitsSoFar%10) + 5) / 10) // 50 to 150% of bucketDrainSleepDuration

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(modifiedSleepDuration):
			// keep looping
		}

		// If we've updated target to a NULL pacer, we'll return immediately
		if p.TargetBytesPerSecond() == 0 {
			atomic.AddInt64(&p.atomicGrandTotal, byteCount)
			return nil
		}

	}

	// record what we issued
	atomic.AddInt64(&p.atomicGrandTotal, byteCount)

	return nil
}

// UndoRequest allows a caller to return unused tokens
func (p *TokenBucketPacer) UndoRequest(byteCount int64) {
	if byteCount > 0 {
		atomic.AddInt64(&p.atomicTokenBucket, byteCount) // put them back in the bucket
		atomic.AddInt64(&p.atomicGrandTotal, -byteCount) // deduct them from all-time issued count
	}
}

func (p *TokenBucketPacer) Close() error {
	close(p.done)
	return nil
}

func (p *TokenBucketPacer) pacerBody() {
	lastTime := time.Now()

	lastTargetUpdateTime := time.Now()
	newTarget := p.TargetBytesPerSecond()
	for {

		select {
		case <-p.done:
			return
		case newTarget = <-p.newTargetBytesPerSecond:
		default:
		}

		/*check if we have to update target rate */
		if newTarget != p.TargetBytesPerSecond() && time.Since(lastTargetUpdateTime) >= DeadBandDuration {
			p.SetTargetBytesPerSecondImmediate(newTarget)
			lastTargetUpdateTime = time.Now()
		}

		currentTarget := atomic.LoadInt64(&p.atomicTargetBytesPerSecond)
		time.Sleep(bucketFillSleepDuration)
		elapsedSeconds := time.Since(lastTime).Seconds()
		bytesToRelease := int64(float64(currentTarget) * elapsedSeconds)
		newTokenCount := atomic.AddInt64(&p.atomicTokenBucket, bytesToRelease)

		// If the backlog of unsent bytes is now too great, then trim it back down.
		// Why don't we want a big backlog? Because it limits our ability to accurately control the speed.
		maxAllowedUnsentBytes := int64(float32(currentTarget) * maxSecondsToOverpopulateBucket)
		if maxAllowedUnsentBytes < p.expectedBytesPerRequest {
			maxAllowedUnsentBytes = p.expectedBytesPerRequest // just in case we are very coarse grained at a very slow speed
		}
		if newTokenCount > maxAllowedUnsentBytes {
			AtomicMorphInt64(&p.atomicTokenBucket, func(currentVal int64) (newVal int64, _ interface{}) {
				newVal = currentVal
				if currentVal > maxAllowedUnsentBytes {
					newVal = maxAllowedUnsentBytes
				}
				return
			})
		}

		lastTime = time.Now()
	}
}

// TargetBytesPerSecond returns the current target rate. Exported (rather than staying a private
// getter) because ste's autoTokenBucketPacer embeds *TokenBucketPacer directly and reads this
// across the package boundary.
func (p *TokenBucketPacer) TargetBytesPerSecond() int64 {
	return atomic.LoadInt64(&p.atomicTargetBytesPerSecond)
}

// SetTargetBytesPerSecondImmediate sets the target rate right away, bypassing the dead-band
// debounce that UpdateTargetBytesPerSecond applies. Named distinctly from UpdateTargetBytesPerSecond
// (the debounced, channel-based public API) to avoid confusion between the two. Exported because
// ste's autoTokenBucketPacer, which embeds *TokenBucketPacer directly, calls this for its own
// AIMD-driven rate changes, which must take effect immediately rather than being debounced.
func (p *TokenBucketPacer) SetTargetBytesPerSecondImmediate(value int64) {
	atomic.StoreInt64(&p.atomicTargetBytesPerSecond, value)
}

func (p *TokenBucketPacer) UpdateTargetBytesPerSecond(value int64) {
	p.newTargetBytesPerSecond <- value
}

func (p *TokenBucketPacer) GetTotalTraffic() int64 {
	return atomic.LoadInt64(&p.atomicGrandTotal)
}
