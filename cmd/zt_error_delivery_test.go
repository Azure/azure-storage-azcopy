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

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fullErrorChannel returns a channel with no room left.
func fullErrorChannel() chan TraverserErrorItemInfo {
	ch := make(chan TraverserErrorItemInfo, 1)
	ch <- ErrorBlobInfo{Error: errors.New("queued")}
	return ch
}

// checkDeliveryWaits checks that send, writing an error "delivered" to the
// full channel ch, waits for room and then delivers it.
func checkDeliveryWaits(a *assert.Assertions, name string, ch chan TraverserErrorItemInfo, send func()) {
	done := make(chan struct{})
	go func() { send(); close(done) }()
	select {
	case <-done:
		a.Fail(name + ": send to a full channel requiring delivery did not wait")
		return
	case <-time.After(50 * time.Millisecond):
	}
	<-ch // make room: the queued error
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		a.Fail(name + ": send did not complete once there was room")
		return
	}
	got := (<-ch).ErrorMessage()
	if a.Error(got, name) {
		a.True(strings.HasSuffix(got.Error(), "delivered"), "%s: delivered %v, want the sent error", name, got)
	}
	ch <- ErrorBlobInfo{Error: errors.New("queued")}
}

// Errors to a full channel are dropped unless the channel requires
// delivery, in which case the sender waits for the reader.
func TestSendScanErrorDelivery(t *testing.T) {
	a := assert.New(t)
	ch := fullErrorChannel()
	a.False(sendScanError(ch, ErrorBlobInfo{Error: errors.New("dropped")}), "send to a full channel that does not require delivery")

	release := RequireErrorDelivery(ch)
	checkDeliveryWaits(a, "sendScanError", ch, func() {
		sendScanError(ch, ErrorBlobInfo{Error: errors.New("delivered")})
	})
	checkDeliveryWaits(a, "blob traverser", ch, func() {
		(&blobTraverser{errorChannel: ch}).writeToBlobErrorChannel(ErrorBlobInfo{Error: errors.New("delivered")})
	})
	checkDeliveryWaits(a, "flat range", ch, func() {
		bt := &blobTraverser{errorChannel: ch, rawURL: "https://a.blob.core.windows.net/c"}
		bounds := flatListRange{start: "a", end: "b"}
		a.True(bt.reportFlatRangeError(bounds, bt.flatRangeError(bounds, errors.New("delivered"))),
			"flat range error requiring delivery was not reported")
	})

	release()
	a.False(sendScanError(ch, ErrorBlobInfo{Error: errors.New("dropped")}), "send to a full channel after release")
}
