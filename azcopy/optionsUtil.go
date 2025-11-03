// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"errors"
	"fmt"
	"math"
)

// BlockSizeInBytes converts a FLOATING POINT number of MiB, to a number of bytes
// A non-nil error is returned if the conversion is not possible to do accurately (e.g. it comes out of a fractional number of bytes)
// The purpose of using floating point is to allow specialist users (e.g. those who want small block sizes to tune their read IOPS)
// to use fractions of a MiB. E.g.
// 0.25 = 256 KiB
// 0.015625 = 16 KiB
func BlockSizeInBytes(rawBlockSizeInMiB float64) (int64, error) {
	if rawBlockSizeInMiB < 0 {
		return 0, errors.New("negative block size not allowed")
	}
	rawSizeInBytes := rawBlockSizeInMiB * 1024 * 1024 // internally we use bytes, but users' convenience the command line uses MiB
	if rawSizeInBytes > math.MaxInt64 {
		return 0, errors.New("block size too big for int64")
	}
	const epsilon = 0.001 // arbitrarily using a tolerance of 1000th of a byte
	_, frac := math.Modf(rawSizeInBytes)
	isWholeNumber := frac < epsilon || frac > 1.0-epsilon // frac is very close to 0 or 1, so rawSizeInBytes is (very close to) an integer
	if !isWholeNumber {
		return 0, fmt.Errorf("while fractional numbers of MiB are allowed as the block size, the fraction must result to a whole number of bytes. %.12f MiB resolves to %.3f bytes", rawBlockSizeInMiB, rawSizeInBytes)
	}
	return int64(math.Round(rawSizeInBytes)), nil
}
