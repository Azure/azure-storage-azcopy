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
	BodyStatusDeallocated BodyStatus = -1
)

func (r *requestPolicyPacer) GetPacedRequestBody(body io.ReadSeekCloser) (io.ReadSeekCloser, error) {
	n, err := body.Seek(0, io.SeekEnd) // find the length of the body
	if err != nil {
		return nil, err
	}

	_, err = body.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	pacedBody := newPolicyPacerBody(r, body, uint64(n))

	pacedBody.bodySeekable = true

	r.requestInitChannel <- pacedBody

	if !r.manualTick {
		pacedBody.AwaitFlight()
	}

	r.allocationRequestchannel <- struct {
		pacerID uuid.UUID
		size    uint64
	}{pacerID: pacedBody.id, size: uint64(max(n, 0)) + 1}

	return &policyPacerRequestBody{body, pacedBody}, nil
}

func (r *requestPolicyPacer) GetPacedResponseBody(body io.ReadCloser, contentLength uint64) (io.ReadCloser, error) {
	pacedBody := newPolicyPacerBody(r, body, contentLength)

	// This makes a strong assumption that nobody will misuse this API!
	// Responses aren't paced like requests,
	// because if they were, it'd unnecessarily extend request lifetime.
	r.respInitChannel <- pacedBody

	if !r.manualTick {
		pacedBody.AwaitFlight()
	}

	r.allocationRequestchannel <- struct {
		pacerID uuid.UUID
		size    uint64
	}{pacerID: pacedBody.id, size: contentLength}

	return pacedBody, nil
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
		out = b.allocatedBytes.Add(uint64(<-b.allocationChannel))
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
	return n, err
}

func (b *policyPacerRequestBody) DeallocateResponse() {
	b.respDeallocateOnce.Do(func() {
		common.AtomicSubtract(b.parent.allocatedBytesPerSecond, RequestMinimumBytesPerSecond)
	})
}

type pacerBodyAllocationBucket struct {
	Requested int64
	Allocated int64
}
