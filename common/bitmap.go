// Copyright © Microsoft <wastore@microsoft.com>
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
	"math"
)

const bitsPerElement = 64

type Bitmap []uint64

// NewBitmap returns a bitmap with at least 'size' bits, backed by an array of uint64. Maximum allowed value
// for size is math.MaxUint16. Higher value will result in a empty bitmap. This should suffice for AzCopy's
// usecase, where we need at most azblob.BlockBlobMaxBlocks bits. 
func NewBitMap(size int) (Bitmap) {
	if (size > math.MaxUint16) {
		return Bitmap{}
	}

	numberOfUint64sRequired := (size/bitsPerElement) + 1
	
	return Bitmap(make([]uint64, numberOfUint64sRequired))
}

func (b Bitmap) getSliceIndexAndMask(index int) (blockIndex int, mask uint64) {
	if index >= len(b) * bitsPerElement || index < 0 {
		return 0, 0
	}

	return (index/bitsPerElement), uint64(1 << (index % bitsPerElement))
}

// Test returns true if the bit at given index is set.
func (b Bitmap) Test(index int) bool {
	BlockIndex, mask := b.getSliceIndexAndMask(index)
	return b[BlockIndex] & mask != 0
}

// Set the bit at given index
func (b Bitmap) Set(index int) {
	indexInSlice, mask := b.getSliceIndexAndMask(index)
	b[indexInSlice] |= mask
}

// Clear the bit at given index
func (b Bitmap) Clear(index int) {
	indexInSlice, mask := b.getSliceIndexAndMask(index)
	b[indexInSlice] &= ^mask
}

// Size returns maximum size of bitmap
func (b Bitmap) Size() int {
	return len(b) * bitsPerElement
}
