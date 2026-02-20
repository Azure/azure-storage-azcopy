package pacer

import (
	"context"
	"time"
)

const (
	pacerMessageStart = 1
	pacerMessagePause = 2
)

type bandwidthRecorderMessage struct {
	messageType uint
	params      []any
}

type bandwidthRecorderWorkerState struct {
	ticker *time.Ticker
	live   bool
}

func (b *bandwidthRecorder) worker(ctx context.Context) {
	state := &bandwidthRecorderWorkerState{
		ticker: time.NewTicker(time.Second),
		live:   false,
	}

	//state.ticker.Stop()

	for {
		select {
		case <-state.ticker.C:
			b.buckets.Rotate()
		case msg := <-b.control:
			b.handleMessage(&msg, state)
		case <-ctx.Done():
			return
		}
	}
}

func (b *bandwidthRecorder) handleMessage(msg *bandwidthRecorderMessage, state *bandwidthRecorderWorkerState) {
	switch msg.messageType {
	case pacerMessageStart:
		if state.live {
			return // no-op
		}

		state.ticker.Reset(time.Second)
		state.live = true
	case pacerMessagePause:
		if !state.live {
			return // no-op
		}

		state.ticker.Stop()
		state.live = false
	}
}
