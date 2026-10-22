package cmd

import (
	"errors"
	"testing"
	"time"
)

// fullErrorChannel returns a channel with no room left.
func fullErrorChannel() chan TraverserErrorItemInfo {
	ch := make(chan TraverserErrorItemInfo, 1)
	ch <- ErrorBlobInfo{Error: errors.New("queued")}
	return ch
}

// checkDeliveryWaits checks that send, writing an error "delivered" to the
// full channel ch, waits for room and then delivers it.
func checkDeliveryWaits(t *testing.T, name string, ch chan TraverserErrorItemInfo, send func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { send(); close(done) }()
	select {
	case <-done:
		t.Fatalf("%s: send to a full channel requiring delivery did not wait", name)
	case <-time.After(50 * time.Millisecond):
	}
	<-ch // make room: the queued error
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: send did not complete once there was room", name)
	}
	if got := (<-ch).ErrorMessage(); got == nil || got.Error() != "delivered" {
		t.Fatalf("%s: delivered %v, want the sent error", name, got)
	}
	ch <- ErrorBlobInfo{Error: errors.New("queued")}
}

// TestSendScanErrorDelivery: errors to a full channel are dropped unless the
// channel requires delivery, in which case the sender waits for the reader.
func TestSendScanErrorDelivery(t *testing.T) {
	ch := fullErrorChannel()
	if sendScanError(ch, ErrorBlobInfo{Error: errors.New("dropped")}) {
		t.Fatal("send to a full channel that does not require delivery succeeded")
	}

	release := RequireErrorDelivery(ch)
	checkDeliveryWaits(t, "sendScanError", ch, func() {
		sendScanError(ch, ErrorBlobInfo{Error: errors.New("delivered")})
	})
	checkDeliveryWaits(t, "blob traverser", ch, func() {
		(&blobTraverser{errorChannel: ch}).writeToBlobErrorChannel(ErrorBlobInfo{Error: errors.New("delivered")})
	})

	release()
	if sendScanError(ch, ErrorBlobInfo{Error: errors.New("dropped")}) {
		t.Fatal("send to a full channel after release succeeded")
	}
}
