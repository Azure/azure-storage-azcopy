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

// GetBlocksRoundedUp returns the number of blocks given sie, rounded up
func GetBlocksRoundedUp(size uint64, blockSize uint64) uint16 {
	return uint16(size/blockSize) + Iffuint16((size%blockSize) == 0, 0, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// inline if functions
func IffError(test bool, trueVal, falseVal error) error {
	if test {
		return trueVal
	}
	return falseVal
}

func IffString(test bool, trueVal, falseVal string) string {
	if test {
		return trueVal
	}
	return falseVal
}

func IffUint8(test bool, trueVal, falseVal uint8) byte {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint8(test bool, trueVal, falseVal int8) int8 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffuint16(test bool, trueVal, falseVal uint16) uint16 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint16(test bool, trueVal, falseVal int16) int16 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffuint32(test bool, trueVal, falseVal uint32) uint32 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint32(test bool, trueVal, falseVal int32) int32 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffuint64(test bool, trueVal, falseVal uint64) uint64 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint64(test bool, trueVal, falseVal int64) int64 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffloat64(test bool, trueVal, falseVal float64) float64 {
	if test {
		return trueVal
	}
	return falseVal
}

// used to get properties in a safe, but not so verbose manner
func IffStringNotNil(wanted *string, instead string) string {
	if wanted == nil {
		return instead
	}

	return *wanted
}
