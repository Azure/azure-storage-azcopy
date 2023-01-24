package common

import (
	"math"
)

const BitsPerElement = 64


// BitMap is a collecton of bit-blocks backed by uint64.
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

func (b Bitmap) Test(index int) bool {
	BlockIndex, mask := b.getSliceIndexAndMask(index)
	return b[BlockIndex] & mask != 0
}

func (b Bitmap) Set(index int) {
	indexInSlice, mask := b.getSliceIndexAndMask(index)
	b[indexInSlice] |= mask
}

func (b Bitmap) Clear(index int) {
	indexInSlice, mask := b.getSliceIndexAndMask(index)
	b[indexInSlice] &= ^mask
}
