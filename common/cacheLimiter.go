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
	"sync/atomic"
)

// Used to limit the amount of in-flight data in RAM, to keep it an an acceptable level.
// For downloads, network is producer and disk is consumer, while for uploads the roles are reversed.
// In either case, if the producer is faster than the consumer, this CacheLimiter is necessary
// prevent unbounded RAM usage
type CacheLimiter interface {
	AddIfBelowStrictLimit(count int64) (added bool)
	AddIfBelowRelaxedLimit(count int64) (added bool)
	RemoveBytes(count int64)
}

type cacheLimiter struct {
	value int64
	limit int64
}

func NewCacheLimiter(limit int64) CacheLimiter {
	return &cacheLimiter{limit: limit}
}

func (c *cacheLimiter) RemoveBytes(count int64) {
	negativeDelta := -count
	atomic.AddInt64(&c.value, negativeDelta)
}

func (c *cacheLimiter) AddIfBelowStrictLimit(count int64) (added bool) {
	return c.tryAdd(count, true)
}

func (c *cacheLimiter) AddIfBelowRelaxedLimit(count int64) (added bool) {
	return c.tryAdd(count, false)
}

func (c *cacheLimiter) tryAdd(count int64, strict bool) (added bool) {
	lim := c.limit
	if strict {
		lim = lim / 2
	}

	if atomic.AddInt64(&c.value, count) <= lim {
		return true
	}
	// else, we are over the limit, so immediately subtract back what we've added, and return false
	atomic.AddInt64(&c.value, -count)
	return false
}
