package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	"sync/atomic"
	"time"
)

/*
A couple problems must be solved here.

1. Requests _larger_ than the throughput window must be allowed through. This means a large request must be able to start and then gradually be paced.
2. Requests
*/

const (
	// RequestMinimumBytesPerSecond is the bare minimum bytes per second we're allowed to allocate to a request.
	RequestMinimumBytesPerSecond = common.MegaByte / 60 // 1 minute per megabyte

	// PacerTickrate defines how frequently we provide bytes to requests in flight. This must be less than or equal to a second.
	PacerTickrate = time.Second / 10

	// PacerMaxOverflow defines how much the "bucket", per PacerTickrate, can overflow, as a multiple.
	PacerMaxOverflow = 2.5

	// PacerRedlinePercentage defines what portion of the max mbps should be filled before halting the acceptance of further additional requests
	// Effectively, the remainder should get distributed to live requests.
	PacerRedlinePercentage = 0.75
	// RealBandwidthRedlinePercentage defines what portion of our actually hit bandwidth, if it is dissimilar to our ideal cap
	RealBandwidthRedlinePercentage = 0.6
	// RealBandwidthPacerSimilarityError defines how much of a gap we will accept before falling back to RealBandwidthRedlinePercentage if a cap is enabled
	RealBandwidthPacerSimilarityError = 0.1
)

func NewRequestPolicyPacer(bytesPerSecond uint64) RequestPolicyPacer {
	p := &requestPolicyPacer{
		processedBytes:          &atomic.Uint64{},
		maxBytesPerSecond:       &atomic.Uint64{},
		allocatedBytesPerSecond: &atomic.Uint64{},
		maxAvailableBytes:       &atomic.Uint64{},
		availableBytes:          &atomic.Uint64{},
		isLive:                  &atomic.Pointer[bool]{},

		liveBodies:         make(map[uuid.UUID]*policyPacerBody),
		requestInitChannel: make(chan *policyPacerBody, 300),
		respInitChannel:    make(chan *policyPacerBody, 300),
		pacerExitChannel:   make(chan *policyPacerBody, 300),
		allocationRequests: make(map[uuid.UUID]uint64),
		allocationRequestchannel: make(chan struct {
			pacerID uuid.UUID
			size    uint64
		}),
		ticker: time.NewTicker(PacerTickrate),
	}

	p.isLive.Store(to.Ptr(true))

	p.UpdateTargetBytesPerSecond(bytesPerSecond)
	go p.pacerBody()

	return p
}

type requestPolicyPacer struct {
	processedBytes *atomic.Uint64

	maxBytesPerSecond       *atomic.Uint64
	allocatedBytesPerSecond *atomic.Uint64

	bytesPerTick      *atomic.Uint64
	maxAvailableBytes *atomic.Uint64
	availableBytes    *atomic.Uint64

	isLive *atomic.Pointer[bool]

	requestInitChannel       chan *policyPacerBody // Requests are paced
	respInitChannel          chan *policyPacerBody // Responses come as a part of the req/resp package and are processed instantly, with priority.
	pacerExitChannel         chan *policyPacerBody // Requests *and* responses exit here
	allocationRequestchannel chan struct {
		pacerID uuid.UUID
		size    uint64
	}

	shutdownCh chan bool

	liveBodies         map[uuid.UUID]*policyPacerBody
	allocationRequests map[uuid.UUID]uint64

	// testing harness stuff
	ticker     *time.Ticker
	manualTick bool // With manualTick enabled, requests will flight themselves to prevent a chicken and egg problem.
}

func (r *requestPolicyPacer) pacerBody() {
	for {
		select {
		case <-r.shutdownCh:
			r.isLive.Store(to.Ptr(false))
			r.ticker.Stop()
			r.shutdownCh <- true
			close(r.shutdownCh)
			return
		case <-r.ticker.C:
			r.refillBucket()               // Refill the bucket
			r.deallocateFinishedRequests() // Deallocate what we can
			r.allocateNewRequests()        // Add new requests
			r.allocateNewResponses()       // Add new responses
			r.feedRequests()               // Use the bytes in the bucket
		}
	}
}

func (r *requestPolicyPacer) refillBucket() {
	if refillRate := r.bytesPerTick.Load(); refillRate != 0 {
		r.availableBytes.Store(min(r.maxAvailableBytes.Load(), r.availableBytes.Load()+refillRate))
	}
}

func (r *requestPolicyPacer) deallocateFinishedRequests() {
	// Discount closed requests
	for {
		escape := false
		select {
		case req := <-r.pacerExitChannel:
			delete(r.liveBodies, req.id)
			common.AtomicSubtract(r.allocatedBytesPerSecond, RequestMinimumBytesPerSecond)
		default:
			escape = true // break doesn't work in here
		}

		if escape {
			break
		}
	}
}

func (r *requestPolicyPacer) allocateNewRequests() {
	// Allow new requests
	// todo try to keep in flight requests to a certain percentage of our "actual" hit bandwidth.
	maxBytesPerSecond := r.maxBytesPerSecond.Load()
	reqRespAllocation := uint64(RequestMinimumBytesPerSecond * 2)

	if maxBytesPerSecond != 0 { // If we have a throughput limit, trickle in requests up to our redline.
		redlineBytesPerSecond := uint64(float64(maxBytesPerSecond) * PacerRedlinePercentage)

		condition := func() bool {
			return r.allocatedBytesPerSecond.Load()+reqRespAllocation < redlineBytesPerSecond
		}

		// If we can't allocate the bare minimum, we just have to allocate a single request at a time, and give it what we can.
		if maxBytesPerSecond < reqRespAllocation {
			condition = func() bool {
				return len(r.liveBodies) == 0
			}
		}

		for condition() {
			escape := false
			select {
			case newRequest := <-r.requestInitChannel:
				r.liveBodies[newRequest.id] = newRequest         // Register the request
				r.allocatedBytesPerSecond.Add(reqRespAllocation) // Allocate the bytes

				// Inform the body that it is allocated
				newRequest.bodyStatus.Store(to.Ptr(BodyStatusAllocated))
			default: // there's nothing new, so let's use what we have.
				escape = true
			}

			if escape {
				break
			}
		}
	} else {
		// todo: placeholder.
		// Currently, we blindly accept all requests. However, instead, we should be watching for our current throughput and

		for {
			escape := false
			select {
			case newRequest := <-r.requestInitChannel:
				r.liveBodies[newRequest.id] = newRequest         // Register the request
				r.allocatedBytesPerSecond.Add(reqRespAllocation) // Allocate the bytes

				// Inform the body that it is allocated
				newRequest.bodyStatus.Store(to.Ptr(BodyStatusAllocated))
			default: // there's nothing new, so let's use what we have.
				escape = true
			}

			if escape {
				break
			}
		}
	}
}

func (r *requestPolicyPacer) allocateNewResponses() {
	// We already included the response in our allocation for the request, so any responses in the queue can freely join in.
	for {
		escape := false
		select {
		case newResponse := <-r.respInitChannel:
			r.liveBodies[newResponse.id] = newResponse

			// Inform the body that it is allocated
			newResponse.bodyStatus.Store(to.Ptr(BodyStatusAllocated))
		default:
			escape = true
		}

		if escape {
			break
		}
	}
}

func (r *requestPolicyPacer) feedRequests() {
	/*
		We ideally want to repeat this process as little as possible, and want to give
		each request as much bandwidth as we can.

		To do this, we start with seeding what we can, and re-iterate until we've exhausted the pool, or we can't allocate anything more.
	*/

	// First, receive requests.
	func() { // Enter a closure so we can cleanly exit the loop without special tricks
		for {
			select {
			case req := <-r.allocationRequestchannel: // A body should never be requesting more than one read at a time.
				if _, ok := r.allocationRequests[req.pacerID]; req.size <= 0 && ok {
					delete(r.allocationRequests, req.pacerID)
				} else if req.size > 0 {
					r.allocationRequests[req.pacerID] = req.size
				}
			default:
				return
			}
		}
	}()

	// If we are in standard allocation mode, do our rounds.
	if bytesPerTick := r.bytesPerTick.Load(); bytesPerTick != 0 {
		availableBytes := r.availableBytes.Load()
		usedBytes := uint64(0)
		allocations := make(map[uuid.UUID]uint64)

		allocateBytes := func(id uuid.UUID, n uint64) {
			// defensively check in case we try to submit a request larger than what's needed or possible
			if availableBytes < n {
				n = availableBytes
			}
			if req := r.allocationRequests[id]; req < n {
				n = req
			}

			usedBytes += n // Keep track of allocated byte count
			availableBytes -= n

			allocations[id] += n // Prepare the allocation
			r.allocationRequests[id] -= n

			if r.allocationRequests[id] == 0 { // Trim it from the list if it doesn't need anything else.
				delete(r.allocationRequests, id)
			}
		}

		func() { // Cycle and try to use the entire pool
			for availableBytes > 0 && len(r.allocationRequests) > 0 {
				avgAllocation := availableBytes / uint64(len(r.allocationRequests))

				if avgAllocation == 0 {
					avgAllocation = availableBytes
				}

				for k, v := range r.allocationRequests {
					allocSize := min(v, avgAllocation)

					allocateBytes(k, allocSize)

					if availableBytes == 0 { // We have allocated all we can. There's no need to allocate anything else.
						return
					}
				}
			}
		}()

		common.AtomicSubtract(r.availableBytes, usedBytes)

		for k, v := range allocations {
			common.NonBlockingSafeSend(r.liveBodies[k].allocationChannel, v)
		}
	}
}

func (r *requestPolicyPacer) UpdateTargetBytesPerSecond(bytesPerSecond uint64) {
	maxBytesPerSecond := bytesPerSecond
	ticksPerSecond := uint64(time.Second / PacerTickrate) // Find how many ticks per second
	if ticksPerSecond < 1 {
		panic("invalid specification for policy pacer tickrate")
	}
	bytesPerTick := maxBytesPerSecond / ticksPerSecond                   // So we can break our cap-mbps into mb/tick
	maxOverflowBytes := uint64(float64(bytesPerTick) * PacerMaxOverflow) // Then, extend our overage window

	r.maxBytesPerSecond.Store(maxBytesPerSecond)
	r.bytesPerTick.Store(bytesPerTick)
	r.maxAvailableBytes.Store(maxOverflowBytes)
}

func (r *requestPolicyPacer) ReturnBytes(returned uint64) {
	common.AtomicMorph(r.availableBytes, func(startVal uint64) (val uint64, res uint64) {
		maxAvailable := r.maxAvailableBytes.Load()
		val = startVal + returned
		if val > maxAvailable {
			val = maxAvailable
		}

		return
	})
}

func (r *requestPolicyPacer) ProcessBytes(processed uint64) {
	r.processedBytes.Add(processed)
}

func (r *requestPolicyPacer) GetTotalTraffic() uint64 {
	return r.processedBytes.Load()
}

func (r *requestPolicyPacer) Cleanup() {
	r.shutdownCh <- true
	<-r.shutdownCh // Wait for the "true shutdown" response.
}
