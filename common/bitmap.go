package common

import (
	"errors"
	"math"
)

const BitsPerElement = 64


// BitMap is a collecton of bit-blocks backed by uint64.
// We support a max of math.MaxUint16 bits which is enough for AzCopy's usecase
type Bitmap []uint64

// size is the minimum num of bits to be available in the bitmap.
func NewBitMap(size int) (Bitmap, error) {
	if (size > math.MaxUint16) {
		return Bitmap{}, errors.New("Size exceeds maximum allowed value")
	}

	numberOfUint64sRequired := math.Ceil(float64(size)/float64(BitsPerElement))
	
	return Bitmap(make([]uint64, int(numberOfUint64sRequired))), nil
}

func (b *Bitmap) getSliceIndexAndMask(index int) (blockIndex int, mask uint64, err error) {
	if index >= len(*b) * BitsPerElement || index < 0 {
		return 0, 0, errors.New("Index out of bounds")
	}

	return (index/BitsPerElement), uint64(1 << (index % BitsPerElement)), nil
}

func (b *Bitmap) Bit(index int) (bool, error) {
	BlockIndex, mask, err := b.getSliceIndexAndMask(index)
	if err != nil {
		return false, err
	}

	return ((*b)[BlockIndex] & mask != 0), nil
}

func (b *Bitmap) Set(index int) error {
	indexInSlice, mask, err := b.getSliceIndexAndMask(index)
	if err != nil {
		return err
	}

	(*b)[indexInSlice] |= mask

	return nil
}

func (b *Bitmap) Clear(index int) error {
	indexInSlice, mask, err := b.getSliceIndexAndMask(index)
	if err != nil {
		return err
	}

	(*b)[indexInSlice] &= ^mask

	return nil
}
