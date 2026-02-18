package pacer

import (
	"context"
	"errors"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type request struct {
	parent Interface

	ctx context.Context
	id  uuid.UUID

	bodySize       int64
	totalRequested atomic.Int64
	allocated      atomic.Int64
	read           atomic.Int64
}

func newRequest(parent Interface, bodySize int64, ctx context.Context) Request {
	out := &request{
		parent: parent,

		ctx: ctx,
		id:  uuid.New(),

		totalRequested: atomic.Int64{},
		allocated:      atomic.Int64{},
		read:           atomic.Int64{},
	}

	out.totalRequested.Store(bodySize)

	return out
}

func (r *request) ID() uuid.UUID {
	return r.id
}

func (r *request) RemainingAllocations() int {
	return int(min(r.totalRequested.Load()-r.allocated.Load(), math.MaxInt))
}

func (r *request) RemainingReads() int {
	return int(min(r.totalRequested.Load()-r.read.Load(), math.MaxInt))
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
	result := r.allocated.Add(int64(size))

	cBodySize := r.totalRequested.Load()
	if result > cBodySize {
		r.allocated.Store(cBodySize)
		return 0
	}

	return cBodySize - result
}

func (r *request) informSeek(newLoc int64) {
	//readPrior := r.read.Load()
	//
	//r.read.Store(newLoc)
	//r.allocated.Store(newLoc)
	//
	//if newLoc < r.totalRequested {
	//	r.parent.reinitiateRequest(r)
	//}

	// if the amount left to read, after the seek, differs from the amonut we've actually read, we should add what remains to the body length.
	remReads := r.RemainingReads()

}

func (r *request) requestUse(size int) (allocated int, err error) {
	// if our body is already fully read, we can never read anything.
	if r.totalRequested <= r.read.Load() {
		return 0, errors.New("request has no remaining reads")
	}

	// if no hard limit is being observed, we have been allocated whatever we please!
	if hardLimitRequested, _ := r.parent.HardLimit(); !hardLimitRequested {
		// But we should still function properly.
		read, alloc := r.read.Load(), r.allocated.Load()
		// If read is less than alloc, allocate what's left of that. Theoretically, it should be 0.
		// If this function were to be called again, alloc - read = size
		r.allocated.Add(int64(size) - (alloc - read))

		return size, nil
	}

	// if we already have something allocated, immediately return it.
	allocated = min(int(r.allocated.Load()-r.read.Load()), size)
	if allocated > 0 {
		return
	}

	// since we have nothing allocated, we gotta wait for it.
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
		case <-r.ctx.Done():
			t.Stop()
			return 0, errors.New("context cancelled while awaiting allocation")
		}

		allocated = min(int(r.allocated.Load()-r.read.Load()), size)
		if allocated > 0 {
			t.Stop()
			break
		}
	}

	return
}

func (r *request) confirmUse(size int, recordBandwidth bool) {
	existingRead := r.read.Load()
	existingAlloc := r.allocated.Load()
	theoreticalRead := existingRead + int64(size)

	if theoreticalRead > existingAlloc {
		panic("sanity check: confirmUse would exceed allocation")
	} else if theoreticalRead > r.totalRequested {
		panic("sanity check: confirmUse would exceed body size")
	}

	r.read.Add(int64(size))

	if recordBandwidth {
		r.parent.RecordBytes(size)
	}
}

func (r *request) discard() {
	r.informSeek(r.totalRequested) // seek to the end so that no more can be read

	r.parent.discardRequest(r)
}
