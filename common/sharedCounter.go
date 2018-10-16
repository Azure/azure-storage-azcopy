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
	"sync"
)

type SharedCounter struct {
	value int64
	cond *sync.Cond  // TODO: replace with something cancllelabe, by context
}

func NewSharedCount () *SharedCounter {
	return &SharedCounter{
		value: 0,
		cond: sync.NewCond(&sync.Mutex{})}
}

func (c *SharedCounter) Add(increment int64) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	c.value += increment
	c.cond.Broadcast()  // let all waiting goroutines know that the value has changed
}


func (c *SharedCounter) WaitUntilLessThan(threshold int64) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	for {
		ok := c.value < threshold
		if ok {
			return
		}
		c.cond.Wait()   // serves as a "hole" in the lock. Blocks until Broadcast is called
	}
}


