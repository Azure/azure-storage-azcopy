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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// autopacer is a request pacer that automatically tunes based upon the frequency of retries.
// autopacer paces at the chunk-level, NOT at the read level, and NOT at the
// If you don't know exactly what you want and why it's this, take a look at pacer.Interface instead.
// autopacer remains in a state in which the pacer has been merged into the interface, as a way to ease transition to
// pacer.Interface by avoiding fixing what isn't broken.
type autopacer interface {
	// RequestTrafficAllocation blocks until the caller is allowed to process byteCount bytes.
	RequestTrafficAllocation(ctx context.Context, byteCount int64) error

	UpdateTargetBytesPerSecond(newTarget int64)

	// UndoRequest reverses a previous request to process n bytes.  Is used when
	// the caller did not need all of the allocation they previously requested
	// e.g. when they asked for enough for a big buffer, but never filled it, they would
	// call this method to return the unused portion.
	UndoRequest(byteCount int64)

	Close() error

	retryNotificationReceiver
}

// autoTokenBucketPacer is a pacer which automatically seeks the right rate, based on retry (503)
// statuses received from the target service.
type autoTokenBucketPacer struct {
	*tokenBucketPacer
	lastPeakBytesPerSecond  float32
	lastPeakTime            time.Time
	done                    chan struct{}
	atomicRetriesInInterval int32
	logger                  common.ILogger
	logPrefix               string // search for this in log, to easily all the logging output
}

const (
	tuningIntervalDuration = time.Second

	deadBandDuration = 20 * time.Second // TODO: review this rather generous value.  Might not be needed if we can pace the internal retry efforts inside the retryPolices, because we (presumably) won't get such big flurries of 503s if we do that

	decreaseFactor = 0.65

	// These increase factors were prototyped in a Excel sheet, then tweaked after live testing.
	// In "steady state" we would expect one decrease every few minutes, with most of the time spent close to the optimal rate.
	// (And yes, the shape of the resulting speed curve is at least superficially similar to that used by modern TCP variants,
	// for the same reason, which is that we want slow change when near the last-known "best" level.)
	fastRecoveryFactor = 0.1
	probingFactor      = 0.015
	stableZoneFactor   = probingFactor / 10
	stableZoneStart    = 0.95
	stableZoneEnd      = 1.05

	pageBlobThroughputTunerString = "Page blob throughput tuner"

	maxPacerGbps           = 100
	maxPacerBytesPerSecond = maxPacerGbps * 1000 * 1000 * 1000 / 8
)

var (
	shouldPacePageBlobs bool
	shouldPaceOncer     sync.Once
)

func newPageBlobAutoPacer(bytesPerSecond int64, expectedBytesPerRequest int64, isFair bool, logger common.ILogger) autopacer {

	shouldPaceOncer.Do(func() {
		raw := common.GetEnvironmentVariable(common.EEnvironmentVariable.PacePageBlobs())
		shouldPacePageBlobs = strings.ToLower(raw) != "false"
	})

	if shouldPacePageBlobs {
		return newAutoPacer(bytesPerSecond, expectedBytesPerRequest, isFair, logger, pageBlobThroughputTunerString)
	}
	return NewNullAutoPacer()
}

func newAutoPacer(bytesPerSecond int64, expectedBytesPerRequest int64, isFair bool, logger common.ILogger, logPrefix string) autopacer {

	// TODO support an additive increase approach, if/when we use this pacer for account throughput as a whole?
	//     Why is fairness important there - because there may be other instances of AzCopy hitting the same account,
	//     so we need cross-pacer fairness.  AIMD gives that, but we are not currently using AIMD because
	//     it requires extra work on our part to figure out what the additive value should be.
	//     So as at mid-March 2019, we are cheating and using multiplicative increase instead, which is fine
	//     for cases where we don't have two pacers competing to control access to the same resource
	if isFair {
		panic("Fair pacing requires additive increase (AIMD), which is not yet supported by this pacer")
	}

	a := &autoTokenBucketPacer{
		tokenBucketPacer:       NewTokenBucketPacer(bytesPerSecond, expectedBytesPerRequest),
		lastPeakBytesPerSecond: float32(bytesPerSecond),
		done:                   make(chan struct{}),
		logger:                 logger,
		logPrefix:              logPrefix,
	}

	go a.rateTunerBody()

	return a
}

func (a *autoTokenBucketPacer) Close() error {
	close(a.done)
	return a.tokenBucketPacer.Close()
}

// RetryCallback records the fact that a retry has happened
func (a *autoTokenBucketPacer) RetryCallback() {
	a.logger.Log(common.LogInfo, fmt.Sprintf("%s: ServerBusy (503) recorded", a.logPrefix))
	atomic.AddInt32(&a.atomicRetriesInInterval, 1)
}

func (a *autoTokenBucketPacer) rateTunerBody() {
	for {
		select {
		case <-a.done:
			return
		case <-time.After(tuningIntervalDuration):
			// continue looping
		}

		retriesInCompletedInterval := atomic.SwapInt32(&a.atomicRetriesInInterval, 0)
		if retriesInCompletedInterval > 0 {
			a.decreaseRate()
			a.logRate()
		} else {
			a.increaseRate()
			a.logRate()
		}
	}
}

func (a *autoTokenBucketPacer) decreaseRate() {
	if time.Since(a.lastPeakTime) < deadBandDuration {
		return // don't do another decrease so soon, since doing so would cause us to overreact
	}
	existingRate := float32(a.targetBytesPerSecond())
	a.lastPeakBytesPerSecond = existingRate
	a.lastPeakTime = time.Now()
	newRate := existingRate * decreaseFactor
	a.tokenBucketPacer.setTargetBytesPerSecond(int64(newRate))
}

func (a *autoTokenBucketPacer) increaseRate() {
	existingRate := float32(a.targetBytesPerSecond())
	var newRate float32
	switch {
	case existingRate < stableZoneStart*a.lastPeakBytesPerSecond:
		// fast increase when below previous peak, to get us back there (if possible) quickly, with minimal loss of throughput
		newRate = existingRate + fastRecoveryFactor*(a.lastPeakBytesPerSecond-existingRate)
	case existingRate < stableZoneEnd*a.lastPeakBytesPerSecond:
		// change slowly when near last peak, since maybe that peak really does represent the best we can do
		newRate = existingRate * (1 + stableZoneFactor)
	default:
		// medium-pace increase if above last peak. Because if we've actually managed to get this far above it,
		// with no need to decrease, that indicates that last peak was probably wrong
		// (i.e. lower than where we should be now) so move reasonably quickly to find a new peak
		newRate = existingRate * (1 + probingFactor)
	}
	// Next line enforces a max because otherwise, if we are constrained by something else (e.g. disk or network)
	// we just keep increasing our rate for ever. And if that other constraint is temporary and goes away,
	// then suddenly well be at a crazy high rate that takes too long to step back down to reality (and or get
	// integer overflow issues).
	if newRate < maxPacerBytesPerSecond {
		a.tokenBucketPacer.setTargetBytesPerSecond(int64(newRate))
	}
}

func (a *autoTokenBucketPacer) logRate() {
	a.logger.Log(common.LogInfo, fmt.Sprintf("%s: Target Mbps %d", a.logPrefix, (a.targetBytesPerSecond()*8)/(1000*1000)))
}
