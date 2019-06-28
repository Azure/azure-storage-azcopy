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
	"github.com/Azure/azure-storage-azcopy/common"
	"sync/atomic"
	"time"
)

// pacerConsumer is used by callers whose activity must be controlled to a certain pace
type pacerConsumer interface {
	RequestRightToSend(ctx context.Context, bytesToSend int64) error
	Close() error
}

const (
	// How long to sleep in the loop that puts tokens into the bucket
	bucketFillSleepDuration = time.Duration(float32(time.Second) * 0.1)

	// How long to sleep when reading from the bucket and finding there's not enough tokens
	bucketDrainSleepDuration = time.Duration(float32(time.Second) * 0.5)

	// Controls the max amount by which the contents of the token bucket can build up, unused.
	maxSecondsToOverpopulateBucket = 5 // suitable for coarse grained, but not for fine-grained pacing
)

// tokenBucketPacer allows us to control the pace of an activity, using a basic token bucket algorithm.
// The target rate is fixed, but can be modified at any time through SetTargetBytesPerSecond
type tokenBucketPacer struct {
	atomicTokenBucket          int64
	atomicTargetBytesPerSecond int64
	expectedBytesPerRequest    int64
	done                       chan struct{}
}

func newTokenBucketPacer(ctx context.Context, bytesPerSecond int64, expectedBytesPerRequest uint32) *tokenBucketPacer {
	p := &tokenBucketPacer{atomicTokenBucket: int64(expectedBytesPerRequest), // seed it immediately with enough to satisfy one request
		atomicTargetBytesPerSecond: bytesPerSecond,
		expectedBytesPerRequest:    int64(expectedBytesPerRequest),
		done:                       make(chan struct{}),
	}

	// the pacer runs in a separate goroutine for as long as the ctx lasts
	go p.pacerBody(ctx)

	return p
}

// RequestRightToSend function is called by goroutines to request right to send a certain amount of bytes.
// It controls their rate by blocking until they are allowed to proceed
func (p *tokenBucketPacer) RequestRightToSend(ctx context.Context, bytesToSend int64) error {
	for atomic.AddInt64(&p.atomicTokenBucket, -bytesToSend) < 0 {
		// by taking our desired count we've moved below zero, which means our allocation is not available
		// right now, so put back what we asked for, and wait
		atomic.AddInt64(&p.atomicTokenBucket, bytesToSend)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(bucketDrainSleepDuration):
			// keep looping
		}
	}
	return nil
}

func (p *tokenBucketPacer) Close() error {
	close(p.done)
	return nil
}

func (p *tokenBucketPacer) pacerBody(ctx context.Context) {
	lastTime := time.Now()
	for {

		select {
		case <-ctx.Done(): // TODO: review use of context here. Alternative is just to insist that user calls Close when done
			return
		case <-p.done:
			return
		default:
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
