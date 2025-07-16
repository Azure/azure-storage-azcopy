package ste

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type BodyStatus int8

const (
	PacedBodyInactivityTimeout = time.Minute * 3

	BodyStatusNew         BodyStatus = 0
	BodyStatusAllocated   BodyStatus = 1
	BodyStatusCompleted   BodyStatus = 2
	BodyStatusDeallocated BodyStatus = -1
)

func (r *requestPolicyPacer) GetPacedRequestBody(body io.ReadSeekCloser, contentLength uint64) io.ReadSeekCloser {
	pacedBody := newPolicyPacerBody(r, body, contentLength)

	pacedBody.bodySeekable = true

	r.requestInitChannel <- pacedBody

	if !r.manualTick {
		pacedBody.AwaitFlight()
	}

	return &policyPacerRequestBody{body, pacedBody}
}

func (r *requestPolicyPacer) GetPacedResponseBody(body io.ReadCloser, contentLength uint64) io.ReadCloser {
	pacedBody := newPolicyPacerBody(r, body, contentLength)

	// This makes a strong assumption that nobody will misuse this API!
	// Responses aren't paced like requests,
	// because if they were, it'd unnecessarily extend request lifetime.
	r.respInitChannel <- pacedBody

	if !r.manualTick {
		pacedBody.AwaitFlight()
	}

	return pacedBody
}

var s2sBodySizeWarnOnce = &sync.Once{}
var s2sBodySizeWarnString = `Due to the nature of pacing service to service requests, at a large enough chunk size, the cap-mbps flag can prove ineffective. For more effective throughput limiting, it is recommended to decrease --block-size-mb. The chunk size when this warning was triggered was: %v MiB`

func (r *requestPolicyPacer) GetS2SPacerAllocation(contentLength uint64) {
	// Unfortunately, due to the _nature_ of S2S, it's impossible to properly pace a S2S request.
	// You can, however, pace the rate at which the requests happen. The window grows larger, but,
	// Across that window, the average is still technically "accurate".
	// This does encounter a slight issue though. What if the size is a gigabyte,
	// and we only technically allow through a couple megs per second.
	// this is a hacky workaround, but we should warn if we see something like that.

	if r.maxBytesPerSecond.Load()*10 < contentLength {
		s2sBodySizeWarnOnce.Do(func() {
			logStr := fmt.Sprintf(
				s2sBodySizeWarnString,
				float64(contentLength)/float64(common.MegaByte),
			)

			common.GetLifecycleMgr().Info(logStr)
			common.AzcopyCurrentJobLogger.Log(common.ELogLevel.Warning(), logStr)
		})
	}

	pacedBody := &policyPacerBody{
		parent: r,
		id:     uuid.New(),

		bodySize:     contentLength,
		bodySeekable: false,
		bodyStatus:   &atomic.Pointer[BodyStatus]{},

		allocationChannel: make(chan uint64, 10),
	}

	r.respInitChannel <- pacedBody

	for alloc := uint64(0); alloc < pacedBody.bodySize; alloc += <-pacedBody.allocationChannel {
	}

	return
}

type policyPacerBody struct {
	parent *requestPolicyPacer
	id     uuid.UUID

	body               io.ReadCloser
	bodySeekable       bool
	bodyDeallocateOnce *sync.Once
	respDeallocateOnce *sync.Once

	timeout *time.Timer

	bodySize       uint64
	totalReadBytes common.AtomicNumeric[uint64]
	readHead       common.AtomicNumeric[uint64]
	allocatedBytes common.AtomicNumeric[uint64]

	bodyStatus common.Atomic[*BodyStatus]

	allocationChannel chan uint64
}

func newPolicyPacerBody(parent *requestPolicyPacer, body io.ReadCloser, contentLength uint64) *policyPacerBody {
	_, bodySeekable := body.(io.Seeker)

	pBody := &policyPacerBody{
		parent: parent,

		id: uuid.New(),

		body:               body,
		bodySeekable:       bodySeekable,
		bodyDeallocateOnce: &sync.Once{},
		respDeallocateOnce: &sync.Once{},

		bodyStatus: &atomic.Pointer[BodyStatus]{},

		bodySize:       contentLength,
		totalReadBytes: &atomic.Uint64{},
		readHead:       &atomic.Uint64{},
		allocatedBytes: &atomic.Uint64{},

		allocationChannel: make(chan uint64, 100),
	}

	pBody.bodyStatus.Store(to.Ptr(BodyStatusNew))
	pBody.setupTimeout(contentLength)

	return pBody
}

// AwaitFlight should be called before calling Read for the first time.
func (b *policyPacerBody) AwaitFlight() {
	b.timeout.Stop()
	defer b.timeout.Reset(PacedBodyInactivityTimeout)

	for {
		if *b.bodyStatus.Load() != BodyStatusNew {
			return
		}

		time.Sleep(PacerTickrate)
	}
}

func (b *policyPacerBody) Read(p []byte) (n int, err error) {
	b.timeout.Stop()
	defer b.timeout.Reset(PacedBodyInactivityTimeout)
	for loop, writeHead := 0, 0; writeHead < len(p); loop++ {
		allocu64 := b.captureAllocations(true)
		// Pull only what we need for this read from our real allocation pool
		allocation := int(min(allocu64, uint64(len(p)-writeHead)))

		if status := *b.bodyStatus.Load(); status != BodyStatusAllocated && status != BodyStatusCompleted {
			return writeHead, errors.New("body is not yet allocated or has been deallocated")
		} else if status == BodyStatusCompleted && uint64(max(allocation, 0))+b.readHead.Load() < b.bodySize {
			return writeHead, errors.New("body is complete but was not allocated enough bytes")
		}

		if allocation < 0 {
			allocation = len(p)
		} else if allocation == 0 {
			continue
		}

		window := p[writeHead : writeHead+allocation]
		n, err = b.body.Read(window)

		writeHead += n
		b.parent.ProcessBytes(uint64(n))
		readHead := b.readHead.Add(uint64(n))
		common.AtomicSubtract(b.allocatedBytes, uint64(n))

		if readHead == b.bodySize {
			return writeHead, io.EOF
		}

		if err != nil {
			if n < allocation { // If we didn't use the entire allocation, return what's left.
				b.parent.ReturnBytes(uint64(allocation - n))
			}

			if errors.Is(err, io.EOF) {
				return writeHead, io.EOF
			}

			return writeHead, fmt.Errorf(
				"failed to read %d bytes on loop %d (%d/%d bytes for this read): %w",
				allocation, loop, writeHead, len(p), err)
		}
	}

	return len(p), nil
}

func (b *policyPacerBody) Close() error {
	b.Deallocate() // Timer is stopped by deallocate
	return b.body.Close()
}

func (b *policyPacerBody) Deallocate() {
	b.timeout.Stop()
	b.bodyDeallocateOnce.Do(func() {
		b.parent.pacerExitChannel <- b
	})
}

func (b *policyPacerBody) setupTimeout(contentLength uint64) {
	// If a body sits idle for an especially long time, with no attempted read,
	// this is extremely abnormal. It probably should be disposed of because somebody forgot to close it.
	b.timeout = time.AfterFunc(PacedBodyInactivityTimeout, func() {
		_ = b.Close()
	})
}

// captureAllocations does a non-blocking capture of allocations present on the channel
// unless specified, in which case it blocks until it has any allocation available.
func (b *policyPacerBody) captureAllocations(blocking ...bool) uint64 {
	toBlock := false
	if len(blocking) > 0 {
		toBlock = blocking[0]
	}

	var out = b.allocatedBytes.Load()
	if toBlock && out == 0 {
		func() { // open a closure so it's easy to exit the loop
			t := time.NewTicker(PacerTickrate)
			defer t.Stop()

			for {
				select {
				case alloc := <-b.allocationChannel:
					out = b.allocatedBytes.Add(alloc)
					return
				case <-t.C:
					if status := *b.bodyStatus.Load(); status != BodyStatusAllocated && status != BodyStatusCompleted {
						return // now we should exit near immediately
					}
				}
			}
		}()
	}

	for {
		select {
		case alloc := <-b.allocationChannel:
			out = b.allocatedBytes.Add(uint64(alloc))
		default:
			return out
		}
	}
}

func (b *policyPacerBody) UndoBytes() {
	b.parent.UndoBytes(b.totalReadBytes.Load())
	b.totalReadBytes.Store(0)
}

type policyPacerRequestBody struct {
	seeker io.Seeker
	*policyPacerBody
}

func (b *policyPacerRequestBody) Seek(offset int64, whence int) (int64, error) {
	b.timeout.Reset(PacedBodyInactivityTimeout)

	// For bodies
	if !b.bodySeekable {
		return 0, errors.New("can't seek a non-seekable body")
	}

	n, err := b.seeker.Seek(offset, whence)
	b.readHead.Store(uint64(n))

	b.parent.reqSeekChannel <- struct {
		p        *policyPacerBody
		newAlloc uint64
	}{p: b.policyPacerBody, newAlloc: b.bodySize - uint64(max(n, 0))}

	return n, err
}

func (b *policyPacerRequestBody) DeallocateResponse() {
	b.respDeallocateOnce.Do(func() {
		common.AtomicSubtract(b.parent.allocatedBytesPerSecond, PacerBodyMinimumBytesPerSecond)
	})
}
