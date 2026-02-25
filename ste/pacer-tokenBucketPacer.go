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

package ste

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

//type PacerAdmin interface {
//	pacer
//
//	// GetTotalTraffic returns the cumulative count of all traffic that has been processed
//	GetTotalTraffic() int64
//}

const (
	// How long to sleep in the loop that puts tokens into the bucket
	bucketFillSleepDuration = time.Duration(float32(time.Second) * 0.1)

	// How long to sleep when reading from the bucket and finding there's not enough tokens
	bucketDrainSleepDuration = time.Duration(float32(time.Second) * 0.333)

	// Controls the max amount by which the contents of the token bucket can build up, unused.
	maxSecondsToOverpopulateBucket = 2.5 // had 5, when doing coarse-grained pacing. TODO: find best all-round value, or parameterize
)

// tokenBucketPacer allows us to control the pace of an activity, using a basic token bucket algorithm.
// The target rate is fixed, but can be modified at any time through SetTargetBytesPerSecond
type tokenBucketPacer struct {
	atomicTokenBucket          int64
	atomicTargetBytesPerSecond int64
	atomicGrandTotal           int64
	atomicWaitCount            int64
	expectedBytesPerRequest    int64
	newTargetBytesPerSecond    chan int64
	done                       chan struct{}
}

func NewTokenBucketPacer(bytesPerSecond int64, expectedBytesPerCoarseRequest int64) *tokenBucketPacer {
	p := &tokenBucketPacer{atomicTokenBucket: bytesPerSecond / 4, // seed it immediately with part-of-a-second's worth, to avoid a sluggish start
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
func (p *tokenBucketPacer) RequestTrafficAllocation(ctx context.Context, byteCount int64) error {
	targetBytes := p.targetBytesPerSecond()
	//if targetBytesIsZero, we have a null pacer, we just track GrandTotal
	if targetBytes == 0 {
		atomic.AddInt64(&p.atomicGrandTotal, byteCount)
		return nil
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
		if p.targetBytesPerSecond() == 0 {
			atomic.AddInt64(&p.atomicGrandTotal, byteCount)
			return nil
		}

	}

	// record what we issued
	atomic.AddInt64(&p.atomicGrandTotal, byteCount)

	return nil
}

// UndoRequest allows a caller to return unused tokens
func (p *tokenBucketPacer) UndoRequest(byteCount int64) {
	if byteCount > 0 {
		atomic.AddInt64(&p.atomicTokenBucket, byteCount) // put them back in the bucket
		atomic.AddInt64(&p.atomicGrandTotal, -byteCount) // deduct them from all-time issued count
	}
}

func (p *tokenBucketPacer) Close() error {
	close(p.done)
	return nil
}

func (p *tokenBucketPacer) pacerBody() {
	lastTime := time.Now()

	lastTargetUpdateTime := time.Now()
	newTarget := p.targetBytesPerSecond()
	for {

		select {
		case <-p.done:
			return
		case newTarget = <-p.newTargetBytesPerSecond:
		default:
		}

		/*check if we have to update target rate */
		if newTarget != p.targetBytesPerSecond() && time.Since(lastTargetUpdateTime) >= deadBandDuration {
			p.setTargetBytesPerSecond(newTarget)
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
			common.AtomicMorphInt64(&p.atomicTokenBucket, func(currentVal int64) (newVal int64, _ interface{}) {
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

func (p *tokenBucketPacer) targetBytesPerSecond() int64 {
	return atomic.LoadInt64(&p.atomicTargetBytesPerSecond)
}

func (p *tokenBucketPacer) setTargetBytesPerSecond(value int64) {
	atomic.StoreInt64(&p.atomicTargetBytesPerSecond, value)
}

func (p *tokenBucketPacer) UpdateTargetBytesPerSecond(value int64) {
	p.newTargetBytesPerSecond <- value
}

func (p *tokenBucketPacer) GetTotalTraffic() int64 {
	return atomic.LoadInt64(&p.atomicGrandTotal)
}
