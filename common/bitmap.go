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

const BitsPerElement = 64


// BitMap is a collection of bit-blocks backed by uint64.
// We support a max of math.MaxUint16 bits which is enough for AzCopy's usecase
type Bitmap []uint64

// size is the minimum num of bits to be available in the bitmap.
func NewBitMap(size int) (Bitmap) {
	if (size > math.MaxUint16) {
		return Bitmap{}
	}

	numberOfUint64sRequired := math.Ceil(float64(size)/float64(BitsPerElement))
	
	return Bitmap(make([]uint64, int(numberOfUint64sRequired)))
}

func (b Bitmap) getSliceIndexAndMask(index int) (blockIndex int, mask uint64) {
	if index >= len(b) * BitsPerElement || index < 0 {
		return 0, 0
	}

	return (index/BitsPerElement), uint64(1 << (index % BitsPerElement))
}

// Test returns true if the bit at given index is set.
func (b Bitmap) Test(index int) bool {
	BlockIndex, mask := b.getSliceIndexAndMask(index)
	return b[BlockIndex] & mask != 0
}

//set the bit at given index
func (b Bitmap) Set(index int) {
	indexInSlice, mask := b.getSliceIndexAndMask(index)
	b[indexInSlice] |= mask
}

//clear the bit at given index
func (b Bitmap) Clear(index int) {
	indexInSlice, mask := b.getSliceIndexAndMask(index)
	b[indexInSlice] &= ^mask
}

//Size returns maximum size of bitmap
func (b Bitmap) Size() int {
	return len(b) * BitsPerElement
}
