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

// TODO : Remove this?
// GetBlocksRoundedUp returns the number of blocks given size, rounded up
func GetBlocksRoundedUp(size uint64, blockSize uint64) uint16 {
	return uint16(size/blockSize) + uint16(Iff((size%blockSize) == 0, 0, 1))
}

func FirstOrZero[T any](list []T) T {
	if len(list) != 0 {
		return list[0]
	}

	var zero T
	return zero
}

func DerefOrZero[T any](in *T) (out T) {
	if in != nil {
		out = *in
	}

	return
}

func Iff[T any](test bool, trueVal, falseVal T) T {
	if test {
		return trueVal
	}
	return falseVal
}

func IffNotNil[T any](wanted *T, instead T) T {
	if wanted == nil {
		return instead
	}

	return *wanted
}

func IffNotEmpty(wanted string) *string {
	if wanted == "" {
		return nil
	}
	return &wanted
}
