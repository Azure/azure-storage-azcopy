package pacer

import (
	"context"
	"errors"
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
	azureMinimumBandwidth = common.MegaByte / 60

	bandwidthSaturationTarget  float64 = 0.35
	pretendMinimumRequestCount         = 10

	allocatorTickrate = time.Second
)

var (
	// pretendBps exists to fill in the lulls (i.e. what if the FE is just enumerating and we have nothing to do?)
	pretendBps = float64(azureMinimumBandwidth*pretendMinimumRequestCount) * (float64(1) / bandwidthSaturationTarget)
)

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
		ticker:            time.NewTicker(allocatorTickrate),
		queue:             make(chan requestQueueEntry, 300),
		reliveQueue:       make(chan requestQueueEntry, 300),
		discardQueue:      make(chan Request, 300),
		live:              make(map[uuid.UUID]Request),
	}

	go out.worker()

	return out
}

func (i *impl) InjectPacer(bodySizeBytes int64, fromTo common.FromTo, ctx context.Context) (context.Context, error) {
	if !fromTo.IsUpload() && !fromTo.IsDownload() {
		return ctx, errors.New("call InjectPacer only on upload and download; For S2S, call InitiateUnpaceable")
	}

	return context.WithValue(ctx, pacerInjectKey, pacerInjectValue{
		pacer:            i,
		wrapMode:         common.Iff(fromTo.IsUpload(), pacerInjectWrapModeRequest, pacerInjectWrapModeResponse), // request on upload, response on download
		expectedBodySize: bodySizeBytes,
	}), nil
}

func (i *impl) initiateRequest(bodySizeBytes int64, ctx context.Context) <-chan Request {
	out := make(chan Request, 1)
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
	out := make(chan error, 1)

	go func() {
		req := <-i.initiateRequest(bodySizeBytes, ctx)

		for req.RemainingReads() > 0 {
			allocated, err := req.requestUse(req.RemainingReads())

			if err != nil {
				req.Discard()
				out <- err
			}

			req.confirmUse(allocated, false)
		}

		req.Discard()

		out <- nil
	}()

	return out
}

func (i *impl) reinitiateRequest(req Request) <-chan any {
	out := make(chan any, 1)

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
			// sometimes we discard mid-tick, that's OK, just ignore it.
			if _, ok := i.live[req.ID()]; ok {
				delete(i.live, req.ID())
			}
		case <-i.ticker.C:
			i.tick()
		}
	}
}
