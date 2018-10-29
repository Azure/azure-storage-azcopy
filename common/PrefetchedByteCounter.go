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
	"time"
)

/// Used to limit the amount of pre-fetched data in RAM, to keep it an a suitable level
/// Similar to SendLimiter, but for now the "waiting" and the "adding" are separate in this class
/// TODO: review whether "waiting" and "adding" should remain separate
type PrefetchedByteCounter interface {
	Add(increment int64)
	WaitUntilBelowLimit()
}

type countWithLimit struct {
	value int64
	limit int64
}

func NewPrefetchedByteCounter(limit int64) PrefetchedByteCounter{
	return &countWithLimit{
		limit: limit	}
}

func (c *countWithLimit) Add(increment int64) {
	atomic.AddInt64(&c.value, increment)
}

func (c *countWithLimit) WaitUntilBelowLimit() {
	for atomic.LoadInt64(&c.value) > c.limit {
		// TODO: check context here, for cancellation?
		// TODO: consider a non-sleep-based solution
		time.Sleep(time.Second/2)
	}
}


