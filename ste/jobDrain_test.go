package ste

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestJobWorkTrackerWaitsForActualWork(t *testing.T) {
	var tracker jobWorkTracker
	tracker.add(3) // A scheduler, a transfer epilogue and a completion report.
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { done <- tracker.wait(ctx) }()
	tracker.done(2)
	select {
	case err := <-done:
		t.Fatalf("drain returned with a completion report still active: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	tracker.done(1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestJobWorkTrackerTimeoutDoesNotDiscardWork(t *testing.T) {
	var tracker jobWorkTracker
	tracker.add(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tracker.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pending work did not respect the cleanup deadline: %v", err)
	}
	tracker.done(1)
	if err := tracker.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestJobWorkTrackerAccountsForChildReports(t *testing.T) {
	var tracker jobWorkTracker
	tracker.add(1)
	tracker.add(1) // Completion reporting starts before the transfer returns.
	tracker.done(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tracker.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("the child completion report was not included in the barrier")
	}
	tracker.done(1)
	if err := tracker.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
