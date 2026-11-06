// Copyright © Microsoft <wastore@microsoft.com>
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

package cmd

import "sync"

// Traversers send scan errors to the caller's error channel without
// blocking and drop them (logging only) when it is full, so a slow or absent
// reader never stalls enumeration. A caller that decides from those errors
// whether a job may complete must see every one: it registers its channel
// with RequireErrorDelivery, and sends to that channel then wait for room
// instead. Such a caller must keep draining the channel until enumeration has
// returned.
var errorChannelsRequiringDelivery sync.Map // chan<- TraverserErrorItemInfo -> struct{}

// RequireErrorDelivery makes scan error sends to ch wait for room instead of
// dropping errors, until the returned func is called.
func RequireErrorDelivery(ch chan<- TraverserErrorItemInfo) (release func()) {
	errorChannelsRequiringDelivery.Store(ch, struct{}{})
	return func() { errorChannelsRequiringDelivery.Delete(ch) }
}

// sendScanError sends item on ch, waiting for room if ch requires delivery
// (see RequireErrorDelivery). It reports whether item was sent.
func sendScanError(ch chan<- TraverserErrorItemInfo, item TraverserErrorItemInfo) bool {
	if _, ok := errorChannelsRequiringDelivery.Load(ch); ok {
		ch <- item
		return true
	}
	select {
	case ch <- item:
		return true
	default:
		return false
	}
}
