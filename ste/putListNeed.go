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

import "sync/atomic"

const (
	putListNotNeeded   = -1
	putListNeedUnknown = 0
	putListNeeded      = 1
)

// TODO: do we want to keep using atomic, just to be sure this is safe no matter how or where its used?
//
//	Or do we want to rely on the assumption, which is correct as at Jan 2018, that its only
//	used in the single-threaded chunk creation loop and in the epilogue. For now, we are erring on the side of safety.
func setPutListNeed(target *int32, value int32) {
	previous := atomic.SwapInt32(target, value)
	if previous != putListNeedUnknown && previous != value {
		panic("'put list' need cannot be set twice")
	}
}

func getPutListNeed(storage *int32) int32 {
	return atomic.LoadInt32(storage)
}
