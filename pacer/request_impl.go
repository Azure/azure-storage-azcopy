package pacer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

var pacerIncorrectBehaviorWarnOnce = &sync.Once{}

type request struct {
	parent Interface

	ctx context.Context
	id  uuid.UUID

	allocationsFinished *atomic.Bool

	/*
		I (adreed, 2/18/26) have to do a little justifying to myself as to why this
		(adding to requestedBudget when we need more budget) is unnecessary to be worrying about.

		The largest block in a block blob or append blob is 4 gigabytes a block (common.MaxBlockBlobBlockSize)
		Dividing math.MaxInt64 by that yields the reality that it fits into an int64,
		not just a handful of times, but, 11 whole digits times (21990232255)!

		Append blobs having a maximum block size of 40x less (common.MaxAppendBlobBlockSize),
		Page blobs, and Azure Files being able to put only 4MB per chunk. (exactly 1000x less, if you ask our code!)
		ADLS Gen 2 doesn't list a max size in its REST API docs, but we do fall back to similar logic for block blobs, so we can assume that's the max.

		Either way, if a retry adds a requirement for new budget, it very certainly won't do it (10^11)*2.2 times under current retry policies...
		If it does, call me, so I can laugh at my own hubris, and panic over how to solve that problem.
	*/

	requestedBudget *atomic.Int64
	allocatedBudget *atomic.Int64
	usedBudget      *atomic.Int64
	readHead        *atomic.Int64

	bodySize int64
}

func newRequest(parent Interface, bodySize int64, ctx context.Context) Request {
	out := &request{
		parent: parent,

		ctx: ctx,
		id:  uuid.New(),

		allocationsFinished: &atomic.Bool{},

		requestedBudget: &atomic.Int64{},
		allocatedBudget: &atomic.Int64{},
		usedBudget:      &atomic.Int64{},
		readHead:        &atomic.Int64{}, // we can trust our read head is at 0, since the SDK expects it too!

		bodySize: bodySize,
	}

	out.allocationsFinished.Store(false)
	out.requestedBudget.Store(bodySize)
	out.allocatedBudget.Store(0)
	out.usedBudget.Store(0)
	out.readHead.Store(0)

	return out
}

func (r *request) ID() uuid.UUID {
	return r.id
}

func (r *request) RemainingAllocations() int {
	return int(min(r.bodySize-r.allocatedBudget.Load(), math.MaxInt))
}

func (r *request) RemainingReads() int {
	return int(min(r.bodySize-r.readHead.Load(), math.MaxInt))
}

func (r *request) WrapRequestBody(reader io.ReadSeekCloser) io.ReadSeekCloser {
	return &wrappedRSC{
		seeker: reader,
		wrappedRC: wrappedRC{
			parentReq:   r,
			childReader: reader,
		},
	}
}

func (r *request) WrapResponseBody(reader io.ReadCloser) io.ReadCloser {
	return &wrappedRC{
		parentReq:   r,
		childReader: reader,
	}
}

func (r *request) issueBytes(size int) (remaining int64) {
	// allocate as many bytes as both size, and our current request will allow.
	cRequest := r.requestedBudget.Load()
	cAlloc := r.allocatedBudget.Load()

	maxAlloc := cRequest - cAlloc
	sizei64 := min(int64(size), maxAlloc)

	return cRequest - r.allocatedBudget.Add(sizei64)
}

func (r *request) informSeek(newLoc int64) {
	// We'll never, ever remove request, only add here. Why?
	// 1) Subtracting request bytes could cause issueBytes to over-issue if it occurs at the right time.
	// 2) Subtracting request bytes could incorrectly cause our allocator to de-allocate us, which would be extremely bad mid-request.
	// So, we should calculate if we'll need new bytes with our new location

	cRequest := r.requestedBudget.Load()
	cRead := r.usedBudget.Load() // we ask about what we've read, not what we've been allocated, as current allocations are "free" under this model.
	toAllocate := cRequest - cRead
	newRequirement := r.bodySize - newLoc

	// if we are about to be allocated less than we'll now need, we add what's missing.
	if toAllocate < newRequirement {
		fmt.Println("what", toAllocate, newRequirement, r.requestedBudget.Load(), r.usedBudget.Load())
		// per my rant in the request struct, this'll pretty much always be a safe operation.
		r.requestedBudget.Add(newRequirement - toAllocate)
	}

	// make sure we also update our read head for accurate read reporting (even though that's only really used for S2S as of 2/18/26!)
	r.readHead.Store(newLoc)
}

func (r *request) requestUse(size int) (allocated int, err error) {
	// a request to use is not a confirmed use-- readers can always read less than you ask them to, so we request, then confirm.
	// simultaneously, sometimes size might be larger than we expect! there could be less of the reader remaining than we intend.

	// if the allocator doesn't have a hard limit, we go as fast as we want. the allocator is trying to gauge how many requests is safe to send.
	if hardLimitRequested, _ := r.parent.HardLimit(); !hardLimitRequested {
		// if we're doing this, we're going to allocate for ourselves, that way if throughput limiting is suddenly enabled (i.e. via stgexp), the request gets paced normally again.
		postAllocation := r.allocatedBudget.Add(int64(size))
		if r.requestedBudget.Load() <= postAllocation { // expand our budget if it's needed (probably not), and de-allocate ourselves, since we're likely finished.
			r.requestedBudget.Store(postAllocation)
			r.Discard()
		}

		return size, nil
	}

	// first, let's return what's available, if anything.
	available := r.allocatedBudget.Load() - r.usedBudget.Load()

	if available > 0 {
		allocated = min(int(min(available, math.MaxInt)), size)
		return
	}

	// prepare a function to reanimate ourselves, if needed.
	reanimate := func() {
		if r.allocationsFinished.Load() {
			common.GetLifecycleMgr().Info(fmt.Sprintf("reanimating %s with %d bytes needed", r.id.String(), size))

			// ensure we are going to get our portion, in case something weird happened.
			futureReadBudget := r.requestedBudget.Load() - r.usedBudget.Load()
			if futureReadBudget < int64(size) {
				r.requestedBudget.Add(int64(size) - futureReadBudget)
			}

			// fire off the reanimate request and flip the allocation bit
			<-r.parent.reinitiateRequest(r)
			r.allocationsFinished.Store(false)
		}
	}

	// then, we wait for a new allocation. this won't be perfectly on pace with the traverser, but 1 second isn't a ton of time to lose for one request.
	t := time.NewTicker(allocatorTickrate)
	defer t.Stop()
	for available == 0 {
		select {
		case <-t.C:
			// no-op, check if we have an allocation (or if we need to reanimate ourselves)
			available = r.allocatedBudget.Load() - r.usedBudget.Load()

			// try reanimating if we still need bytes but have nothing available
			if available == 0 {
				reanimate()
			}
		case <-r.ctx.Done():
			return 0, errors.New("context canceled while checking for allocation")
		}
	}

	allocated = min(int(min(available, math.MaxInt)), size)

	return
}

func (r *request) confirmUse(size int, recordBandwidth bool) {
	// while it's incorrect behavior to confirm more than we've ever been allocated,
	// we'll let it slide and drop a warning in the logs that something *probably* isn't as we expect,
	// because working is better than not working, even if working incorrectly.

	afterUse := r.usedBudget.Add(int64(size))
	r.readHead.Add(int64(size))
	if r.allocatedBudget.Load() < afterUse { // handle the incorrect scenario
		pacerIncorrectBehaviorWarnOnce.Do(func() {
			common.AzcopyCurrentJobLogger.Log(common.LogWarning, "This won't cause issues with your job, but the request pacer has observed incorrect behavior, confirming more bytes than allocated. Please file a bug on the AzCopy github repo if you see this.")
		})

		r.allocatedBudget.Store(afterUse)
		if r.requestedBudget.Load() < afterUse {
			r.requestedBudget.Store(afterUse)
		}
	}

	if recordBandwidth { // observe the bytes.
		r.parent.RecordBytes(size)
	}
}

func (r *request) Discard() {
	// mark ourselves discarded, and use all allocations.
	r.allocationsFinished.Store(true)

	r.parent.discardRequest(r)
}
