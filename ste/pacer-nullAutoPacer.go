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
	"sync/atomic"
)

// nullAutoPacer is a no-op auto pacer. For use in code which may or may not need pacing. When not needed,
// just use an instance of this type
type nullAutoPacer struct {
	atomicGrandTotal int64
}

func NewNullAutoPacer() *nullAutoPacer {
	return &nullAutoPacer{}
}

func (a *nullAutoPacer) Close() error {
	return nil
}

func (a *nullAutoPacer) RetryCallback() {
	// noop
}

func (a *nullAutoPacer) RequestTrafficAllocation(ctx context.Context, byteCount int64) error {
	atomic.AddInt64(&a.atomicGrandTotal, byteCount) // we track total aggregate throughput, even though we don't do any actual pacing
	return nil
}

func (a *nullAutoPacer) UndoRequest(byteCount int64) {
	atomic.AddInt64(&a.atomicGrandTotal, -byteCount)
}

func (a *nullAutoPacer) GetTotalTraffic() int64 {
	return atomic.LoadInt64(&a.atomicGrandTotal)
}

func (a *nullAutoPacer) UpdateTargetBytesPerSecond(_ int64) {

}
