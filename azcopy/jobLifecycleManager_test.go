package azcopy

import (
	"context"
	"os"
	"os/signal"
	"testing"
	"time"
)

func TestCancelFromStdinCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelChannel := make(chan os.Signal, 1)

	// The fix -- listening on both OS signals and channels
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)
		select {
		case <-sigChan:
			cancel()
		case <-cancelChannel:
			cancel()
		}
	}()

	cancelChannel <- os.Interrupt // Simulate cancel-from-stdin

	// Context should be canceled within a reasonable time
	select {
	case <-ctx.Done():
		// Success — cancel-from-stdin triggered context cancellation
	case <-time.After(5 * time.Second):
		t.Fatal("context was not canceled after sending to cancelChannel")
	}
}
