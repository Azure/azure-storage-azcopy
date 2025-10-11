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
	"math/rand"
	"sync/atomic"
	"time"
)

// The percentage of a CacheLimiter's Limit that is considered
// the strict limit.
var cacheLimiterStrictLimitPercentage = float32(0.75)

// Rationale for the level of the strict limit: as at Jan 2018, we are using 0.75 of the total as the strict
// limit, leaving the other 0.25 of the total accessible under the "relaxed" limit.
// That last 25% gets use for two things: in downloads it is used for things where we KNOW there's
// no backlogging of new chunks behind slow ones (i.e. these "good" cases are allowed to proceed without
// interruption) and for uploads its used for re-doing the prefetches when we do retries (i.e. so these are
// not blocked by other chunks using up RAM).
// TODO: now that cacheLimiter is used for multiple purposes, the hard-coding of the distinction between
//   relaxed and strict limits is less appropriate. Refactor to make it a configuration param of the instance?

type Predicate func() bool

// Used to limit the amounts of things. E.g. amount of in-flight data in RAM, to keep it an an acceptable level.
// Also used for number of open files (since that's limited on Linux).
// In the case of RAM usage, for downloads, network is producer and disk is consumer, while for uploads the roles are reversed.
// In either case, if the producer is faster than the consumer, this CacheLimiter is necessary
// prevent unbounded RAM usage.
type CacheLimiter interface {
	TryAdd(count int64, useRelaxedLimit bool) (added bool)
	WaitUntilAdd(ctx context.Context, count int64, useRelaxedLimit Predicate) error
	Remove(count int64)
	Limit() int64
	StrictLimit() int64
}

type cacheLimiter struct {
	value int64
	limit int64
}

func NewCacheLimiter(limit int64) CacheLimiter {
	return &cacheLimiter{limit: limit}
}

// TryAddBytes tries to add a memory allocation within the limit.  Returns true if it could be (and was) added
func (c *cacheLimiter) TryAdd(count int64, useRelaxedLimit bool) (added bool) {
	lim := c.limit

	// Above the "strict" limit, there's a bit of extra room, which we use
	// for high-priority things (i.e. things we deem to be allowable under a relaxed (non-strict) limit)
	strict := !useRelaxedLimit
	if strict {
		lim = c.StrictLimit()
	}

	if atomic.AddInt64(&c.value, count) <= lim {
		return true
	}
	// else, we are over the limit, so immediately subtract back what we've added, and return false
	atomic.AddInt64(&c.value, -count)
	return false
}

// / WaitUntilAddBytes blocks until it completes a successful call to TryAddBytes
func (c *cacheLimiter) WaitUntilAdd(ctx context.Context, count int64, useRelaxedLimit Predicate) error {
	for {
		// Proceed if there's room in the cache
		if c.TryAdd(count, useRelaxedLimit()) {
			return nil
		}

		// else wait and repeat
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(2 * float32(time.Second) * rand.Float32())):
			// Duration of delay is somewhat arbitrary. Don't want to use anything very tiny (e.g. milliseconds) because that
			// just adds CPU load for no real benefit.  Is this value too big?  Probably not, because even at 10 Gbps,
			// it would take longer than this to fill or drain our full memory allocation.

			// Nothing to do, just loop around again
			// The wait is randomized to prevent the establishment of repetitive oscillations in cache size
			// Average wait is quite long (2 seconds) since context where we're using this does not require any timing more fine-grained
		}
	}
}

func (c *cacheLimiter) Remove(count int64) {
	negativeDelta := -count
	atomic.AddInt64(&c.value, negativeDelta)
}

func (c *cacheLimiter) Limit() int64 {
	return c.limit
}

func (c *cacheLimiter) StrictLimit() int64 {
	return int64(float32(c.limit) * cacheLimiterStrictLimitPercentage)
}
