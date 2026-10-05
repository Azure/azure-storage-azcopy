package ste

import (
	"context"
	"sync"
)

// jobWorkTracker accounts for queued transfers, scheduling loops, part-created
// messages and completion reports independently of a potentially terminal JobStatus.
type jobWorkTracker struct {
	mu      sync.Mutex
	pending uint64
	idle    chan struct{}
}

func (w *jobWorkTracker) add(count uint64) {
	if count == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending == 0 {
		w.idle = make(chan struct{})
	}
	w.pending += count
}

func (w *jobWorkTracker) done(count uint64) {
	if count == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if count > w.pending {
		panic("job drain completion exceeds registered work")
	}
	w.pending -= count
	if w.pending == 0 {
		close(w.idle)
	}
}

// wait requires the caller to have joined every producer that can queue new parts.
func (w *jobWorkTracker) wait(ctx context.Context) error {
	for {
		w.mu.Lock()
		if w.pending == 0 {
			w.mu.Unlock()
			return nil
		}
		idle := w.idle
		w.mu.Unlock()
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
