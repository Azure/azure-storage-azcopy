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
	"sync"
)

// A pool of byte slices
// Like sync.Pool, but strongly-typed to byte slices
type ByteSlicePooler interface {
	RentSlice(desiredLength uint32) []byte
	ReturnSlice(slice []byte)
}

// A pool of byte slices, optimized so that it actually has a sub-pool for each
// different size (in powers of 2) up to some pre-specified limit.  The use of sub-pools
// minimized wastage, in cases where the desired slice sizes vary greatly.
// (E.g. if only had one pool, holding really big slices, it would be wasteful when
// we only need to put put small amounts of data into them).
type multiSizeSlicePool struct {
	// It is safe for multiple readers to read this, once we have populated it
	// See https://groups.google.com/forum/#!topic/golang-nuts/nL8z96SXcDs
	poolsBySize []*sync.Pool
}

// Create new slice pool capable of pooling slices up to maxSliceLength in size
func NewMultiSizeSlicePool(maxSliceLength uint32) ByteSlicePooler {
	maxSlotIndex, _ :=  getSlotInfo(maxSliceLength)
	poolsBySize := make([]*sync.Pool, maxSlotIndex + 1)
	for i := 0; i <= maxSlotIndex; i++ {
		poolsBySize[i] = new(sync.Pool)
	}
	return &multiSizeSlicePool{poolsBySize: poolsBySize}
}

func getSlotInfo(exactSliceLength uint32) (slotIndex int, maxCapInSlot int) {
	// slot index is fast computation of the base-2 logarithm, rounded down
	slotIndex = 32 - bits.LeadingZeros32(exactSliceLength)
	// max cap in slot is the biggest number that maps to that slot index
	// (e.g. slot index of 1 (which=2 to the power of 0) is 1, so (2 to the power of slotIndex)
	//  is the first number that doesn't fit the slot)
	maxCapInSlot = (1 << uint(slotIndex)) - 1

	// check  TODO: replace this check with a proper unit test
	if 32 - bits.LeadingZeros32(uint32(maxCapInSlot)) != slotIndex {
		panic("cross check of cap and slot index failed")
	}
	return
}

// Borrows a slice from the pool (or creates a new one if none of suitable capacity is available)
func (mp *multiSizeSlicePool) RentSlice(desiredSize uint32) []byte {
	slotIndex, maxCapInSlot := getSlotInfo(desiredSize)

	// get the pool that most closely corresponds to the desired size
	pool := mp.poolsBySize[slotIndex]

	// try to get a pooled slice
	if typedSlice, ok := pool.Get().([]byte); ok {
		// capacity will be equal to maxCapInSlot, but we need to set len to the
		// exact desired size that was requested
		typedSlice = typedSlice[0:desiredSize]
		// TODO: should we also zero out the content of the slice?
		// There seems to be no in-built way to do that, on an existing slice, in Go, other than
		// by having a slice full of zeros and using the built-in copy() function to move the data over.
		// Is it really worth doing that, when the only reason to zero it out is to offer some kind of
		// protection against any bug in which old data is accidentally read
		// TODO: or should we re-work this code to return bytes.Buffer instances instead of simple slices??
		return typedSlice
	}

	// make a new slice if nothing pooled
	return make([]byte, desiredSize, maxCapInSlot)
}

// returns the slice to its pool
func (mp *multiSizeSlicePool) ReturnSlice(slice []byte) {
	slotIndex, _ := getSlotInfo(uint32(cap(slice)))   // be sure to use capacity, not length, here

	// get the pool that most closely corresponds to the desired size
	pool := mp.poolsBySize[slotIndex]

	// put the slice back into the pool
	pool.Put(slice)
}

