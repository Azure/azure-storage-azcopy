// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"math/bits"
)

// A pool of byte slices
// Like sync.Pool, but strongly-typed to byte slices
type ByteSlicePooler interface {
	RentSlice(desiredLength int64) []byte
	ReturnSlice(slice []byte)
	Prune()
}

// Pools byte slices of a single size.
// We are not using sync.Pool because it reserves the right
// to ignore is contents and pretend to be empty. That's OK if
// there are enough GCs to cause it to be flushed/emptied.
// But it didn't seem to be getting emptied for us (presumably because
// we didn't have many GCs... because we were pooling resources!)
// And so we would get 150 or so objects just sitting there in the pool,
// and if each of those is for a 100 MB "max size" storage block, that gets bad.
// Discussion at the following URL confirms the problematic nature and that "roll your own"
// can be better for low-contention cases - which is what we believe ours to be:
// https://github.com/golang/go/issues/22950
type simpleSlicePool struct {
	c chan []byte
}

func newSimpleSlicePool(maxCapacity int) *simpleSlicePool {
	return &simpleSlicePool{
		c: make(chan []byte, maxCapacity),
	}
}

func (p *simpleSlicePool) Get() []byte {
	select {
	case existingItem := <-p.c:
		return existingItem
	default:
		return nil
	}
}

func (p *simpleSlicePool) Put(b []byte) {
	select {
	case p.c <- b:
		return
	default:
		// just throw b away and let it get GC'd if p.c is full
	}
}

// A pool of byte slices, optimized so that it actually has a sub-pool for each
// different size (in powers of 2) up to some pre-specified limit.  The use of sub-pools
// minimized wastage, in cases where the desired slice sizes vary greatly.
// (E.g. if only had one pool, holding really big slices, it would be wasteful when
// we only need to put put small amounts of data into them).
type multiSizeSlicePool struct {
	// It is safe for multiple readers to read this, once we have populated it
	// See https://groups.google.com/forum/#!topic/golang-nuts/nL8z96SXcDs
	poolsBySize []*simpleSlicePool
}

// Create new slice pool capable of pooling slices up to maxSliceLength in size
func NewMultiSizeSlicePool(maxSliceLength int64) ByteSlicePooler {
	maxSlotIndex, _ := getSlotInfo(maxSliceLength)
	poolsBySize := make([]*simpleSlicePool, maxSlotIndex+1)
	for i := 0; i <= maxSlotIndex; i++ {
		maxCount := getMaxSliceCountInPool(i)
		poolsBySize[i] = newSimpleSlicePool(maxCount)
	}
	return &multiSizeSlicePool{poolsBySize: poolsBySize}
}

var indexOf32KSlot, _ = getSlotInfo(32 * 1024)

// For a given requested len(slice), this returns the slot index to use, and the max
// cap(slice) of the slices that will be found at that index
func getSlotInfo(exactSliceLength int64) (slotIndex int, maxCapInSlot int) {
	if exactSliceLength <= 0 {
		panic("exact slice length must be greater than zero")
	}
	// raw slot index is fast computation of the base-2 logarithm, rounded down...
	rawSlotIndex := 63 - bits.LeadingZeros64(uint64(exactSliceLength))

	// ...but in most cases we actually want to round up.
	// E.g. we want 255 to go into the same bucket as 256. Why? because we want exact
	// powers of 2 to be the largest thing in each bucket, since usually
	// we will be using powers of 2, and that means we will usually be using
	// all the allocated capacity (i.e. len == cap).  That gives the most efficient use of RAM.
	// The only time we don't want to round up, is if we already had an exact power of
	// 2 to start with.
	isExactPowerOfTwo := bits.OnesCount64(uint64(exactSliceLength)) == 1
	if isExactPowerOfTwo {
		slotIndex = rawSlotIndex
	} else {
		slotIndex = rawSlotIndex + 1
	}

	// Max cap in slot is the biggest number that maps to that slot index
	// (e.g. slot index of exactSliceLength=1 (which=2 to the power of 0)
	// is 0 (because log-base2 of 1 == 0), so (2 to the power of slotIndex)
	//  is the highest number that still fits the slot)
	maxCapInSlot = 1 << uint(slotIndex)

	return
}

func holdsSmallSlices(slotIndex int) bool {
	return slotIndex <= indexOf32KSlot
}

// For a given slot index, this returns the max number of pooled slices which the pool at
// that index should be allowed to hold.
func getMaxSliceCountInPool(slotIndex int) int {
	if holdsSmallSlices(slotIndex) {
		// Choose something fairly high for these because there's no significant RAM
		// cost in doing so, and small files are a tricky case for perf so let's give
		// them all the pooling help we can
		return 500
	} else {
		// Limit the medium and large ones a bit more strictly.
		return 100
	}
}

// RentSlice borrows a slice from the pool (or creates a new one if none of suitable capacity is available)
// Note that the returned slice may contain non-zero data - i.e. old data from the previous time it was used.
// That's safe IFF you are going to do the likes of io.ReadFull to read into it, since you know that all of the
// old bytes will be overwritten in that case.
func (mp *multiSizeSlicePool) RentSlice(desiredSize int64) []byte {
	slotIndex, maxCapInSlot := getSlotInfo(desiredSize)

	// get the pool that most closely corresponds to the desired size
	pool := mp.poolsBySize[slotIndex]

	// try to get a pooled slice
	if typedSlice := pool.Get(); typedSlice != nil {
		// clear out the entire slice up to the capacity
		// a zero-ing-out loop written in the right form in Go, will be automatically turned into a call to memclr,
		// which is an optimized Go runtime routine written in assembler
		// more info here: https://github.com/golang/go/commit/f03c9202c43e0abb130669852082117ca50aa9b1
		typedSlice = typedSlice[0:maxCapInSlot]
		for i := range typedSlice {
			typedSlice[i] = 0
		}

		// here we set len to the exact desired size that was requested
		typedSlice = typedSlice[0:desiredSize]
		return typedSlice
	}

	// make a new slice if nothing pooled
	return make([]byte, desiredSize, maxCapInSlot)
}

// returns the slice to its pool
func (mp *multiSizeSlicePool) ReturnSlice(slice []byte) {
	slotIndex, _ := getSlotInfo(int64(cap(slice))) // be sure to use capacity, not length, here

	// get the pool that most closely corresponds to the desired size
	pool := mp.poolsBySize[slotIndex]

	// put the slice back into the pool
	pool.Put(slice)
}

// Prune inactive stuff in all the big slots if due (don't worry about the little ones, they don't eat much RAM)
// Why do this? Because for the large slot sizes its hard to deal with slots that are full and IDLE.
// I.e. we were using them, but now we're working with other files in the same job that have different chunk sizes,
// so we have a pool slot that's full of slices that are no longer getting used.
// Would it work to just give the slot a very small max capacity? Maybe. E.g. max number in slot = 4 for the 128 MB slot.
// But can we be confident that 4 is enough for the pooling to have the desired benefits?  Not sure.
// Hence the pruning, so that we don't need to set the fixed limits that low.  With pruning, the count will (gradually)
// come down only when the slot is IDLE.
func (mp *multiSizeSlicePool) Prune() {
	for index := 0; index < len(mp.poolsBySize); index++ {
		shouldPrune := !holdsSmallSlices(index)
		if shouldPrune {
			// Get one item from the pool and throw it away.
			// With repeated calls of Prune, this will gradually drain idle pools.
			// But, since Prune is not called very often,
			// it won't have much adverse impact on active pools.
			_ = mp.poolsBySize[index].Get()
		}
	}
}
