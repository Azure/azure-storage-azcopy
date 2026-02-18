package pacer

import (
	"context"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

const (
	/*
		https://learn.microsoft.com/en-us/rest/api/storageservices/setting-timeouts-for-blob-service-operations
		Azure Blob storage defines downloads as 2 minutes per megabyte, and 10 minutes per megabyte for upload.

		https://learn.microsoft.com/en-us/rest/api/storageservices/setting-timeouts-for-file-service-operations
		Azure Files defines it's minimum throughput as 2 minutes per megabytes.

		As such, we'll safely define our understanding at double that-- one megabyte per minute.
	*/
	AzureMinimumBandwidth = common.MegaByte / 60

	BandwidthSaturationTarget  float64 = 0.5
	PretendMinimumRequestCount         = 10

	// PretendBps exists to fill in the lulls (i.e. what if the FE is just enumerating and we have nothing to do?)
	PretendBps = AzureMinimumBandwidth * PretendMinimumRequestCount * int64(float64(1)/BandwidthSaturationTarget)
)

var _ Interface = &impl{}

type impl struct {
	BandwidthRecorder

	appCtx context.Context
	ticker *time.Ticker

	queue        chan requestQueueEntry
	reliveQueue  chan requestQueueEntry
	discardQueue chan Request
	live         map[uuid.UUID]Request
}

func New(recorder BandwidthRecorder, appCtx context.Context) Interface {
	out := &impl{
		BandwidthRecorder: recorder,
		appCtx:            appCtx,
		ticker:            time.NewTicker(time.Second),
		queue:             make(chan requestQueueEntry),
		reliveQueue:       make(chan requestQueueEntry),
		discardQueue:      make(chan Request),
		live:              make(map[uuid.UUID]Request),
	}

	go out.worker()

	return out
}

func (i *impl) InitiateRequest(bodySizeBytes int64, ctx context.Context) <-chan Request {
	out := make(chan Request)
	go func() {
		startCh := make(chan any, 1)
		req := newRequest(i, bodySizeBytes, ctx)

		i.queue <- requestQueueEntry{
			req:     req,
			readyCh: startCh,
		}

		<-startCh
		out <- req
	}()

	return out
}

func (i *impl) InitiateUnpaceable(bodySizeBytes int64, ctx context.Context) <-chan error {
	out := make(chan error)

	go func() {
		req := <-i.InitiateRequest(bodySizeBytes, ctx)

		for req.RemainingReads() > 0 {
			allocated, err := req.requestUse(req.RemainingReads())

			if err != nil {
				req.discard()
				out <- err
			}

			req.confirmUse(allocated, false)
		}

		req.discard()

		out <- nil
	}()

	return out
}

func (i *impl) reinitiateRequest(req Request) <-chan any {
	out := make(chan any)

	i.reliveQueue <- requestQueueEntry{
		req:     req,
		readyCh: out,
	}

	return out
}

func (i *impl) discardRequest(request Request) {
	i.discardQueue <- request
}

func (i *impl) worker() {
	for {
		select {
		case req := <-i.discardQueue:
			delete(i.live, req.ID())
		case <-i.ticker.C:
			i.tick()
		}
	}
}
