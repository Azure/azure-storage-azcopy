package pacer

import (
	"context"
	"io"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

const (
	DefaultBandwidthRecorderWindowSeconds = 10
)

type BandwidthRecorder interface {
	// RecordBytes records the number of bytes that have made it through the pipe.
	// We don't care if the request ends in success or failure,
	// because we're trying to observe the available bandwidth.
	RecordBytes(count int)

	// StartObservation must be called after the bandwidth recorder is created. If already started, no-ops.
	StartObservation()
	// PauseObservation should be called whenever transfers aren't actively in progress (i.e. when waiting for another job plan part), followed by StartObservation. If already paused, no-ops.
	PauseObservation()

	// SetObservationPeriod sets the observation period that RecordBytes averages over.
	SetObservationPeriod(seconds uint)
	// ObservationPeriod gets the observation period that RecordBytes averages over.
	ObservationPeriod() (seconds uint)

	// Bandwidth returns the observed (or requested) bandwidth. Requests should be gated based upon this, if it is below a requested hard limit.
	Bandwidth() (bytesPerSecond int64, fullAverage bool)
	// HardLimit indicates that the user has requested a hard limit. Bandwidth should be allocated evenly amongst this if requested.
	HardLimit() (requested bool, bytesPerSecond int64)

	GetTotalTraffic() int64

	// RequestHardLimit indicates that the user is requesting a hard limit.
	RequestHardLimit(bytesPerSecond int64)
	// ClearHardLimit indicates that the user has cancelled the hard limit request.
	ClearHardLimit()
}

// Interface implements the meat and potatoes of the pacer package, recording bandwidth and managing request initiation.
// Think of it like air traffic control. You don't want too many planes in the air, otherwise you're going to have a crash.
// One BandwidthRecorder is attached, intended for measuring the primary transfer direction.
type Interface interface {
	BandwidthRecorder

	// InitiateRequest asks to initiate a request with a specific size. The request should _not_ be on the wire yet.
	// Do not call directly, favor InjectPacer.
	initiateRequest(bodySizeBytes int64, ctx context.Context) <-chan Request

	// InitiateUnpaceable asks to initiate a request which *cannot* be paced and has to act by sheer average (i.e. like S2S).
	// This comes with a very large caveat! Bandwidth is _not_ recorded, and the hard limit is observed "raw".
	// This is incompatible with the pacing of InitiateRequest.
	// This shouldn't wind up being combined, since we don't do S2S transfers at the same time as up/downloads,
	// But word to the wise: HERE BE DRAGONS.
	InitiateUnpaceable(bodySizeBytes int64, ctx context.Context) <-chan error

	// InjectPacer updates the context with the key required to inject the pacer.
	// NewPacerInjectPolicy should be added to the pipeline, following the retry policy, as close to the request execution as possible.
	// Without it, InjectPacer will be ineffectual.
	InjectPacer(bodySizeBytes int64, fromTo common.FromTo, ctx context.Context) (context.Context, error)

	discardRequest(request Request)
	reinitiateRequest(req Request) <-chan any
}

// Request is a lifecycle manager for a single request. It is _not_ idempotent, multiple threads should _not_ be acting upon it at the same time.
// One request, on one thread, that is already on the wire.
type Request interface {
	ID() uuid.UUID

	// RemainingAllocations is how much left this request has to get allocated.
	RemainingAllocations() int
	// RemainingReads is how much left this request has to read. Should always be larger than RemaningAllocations().
	RemainingReads() int

	// WrapRequestBody wraps a request body. This, or WrapResponseBody should only be called once.
	WrapRequestBody(reader io.ReadSeekCloser) io.ReadSeekCloser
	// WrapResponseBody wraps a response body. This, or WrapRequestBody should only be called once.
	WrapResponseBody(reader io.ReadCloser) io.ReadCloser

	// issueBytes hands a request bytes to use. They don't _have_ to be used, and there's intentionally no mechanism to return them, because it complicates the logic.
	issueBytes(size int) (remaining int64)
	// informSeek alters the amount read and allocated (if less has been allocated) and reinitiates the request.
	informSeek(newLoc int64)
	// requestUse should be called with the size of the buffer. It may be larger than Remaining. An amount of bytes [0, Remaining] will be returned. 0 is only returned if Remaining is 0.
	requestUse(size int) (allocated int, err error)
	// confirmUse should be called after reading (or writing) as much as possible from the allocated value of RequestUse. If recordBandwidth is true, it is written to the BandwidthRecorder.
	confirmUse(size int, recordBandwidth bool)
	// Discard indicates that the Request will probably never be used again, and that we should discard the bandwidth allocation.
	Discard()
}
