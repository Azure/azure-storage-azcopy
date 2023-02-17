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
	"math/rand"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

type bitmapTestSuite struct{}

var _ = chk.Suite(&bitmapTestSuite{})

func (b *bitmapTestSuite) TestBitmapBasic(c *chk.C) {
	numOfBlocks := rand.Int31n(azblob.BlockBlobMaxBlocks)
	fileMap := NewBitMap(int(numOfBlocks))

	// Make some unique random keys to test
	m := make(map[int]int)
	for i := 0; i < 10; i++ {
		m[int(rand.Int31n(numOfBlocks))] = 1
	}
	testBits := make([]int, len(m))

	i := 0
	for k := range m {
		testBits[i] = k
		i++
	}

	//Verify that the bits are unset before use
	for  _, index := range testBits {
		c.Assert(fileMap.Test(index), chk.Equals, false)
	}

	// set the bits and verify
	for _, index := range testBits {
		fileMap.Set(index)
		c.Assert(fileMap.Test(index), chk.Equals, true)
	}

	// clear some 5 bits 
	for i := 0; i < len(testBits); i = i+2 {
		fileMap.Clear(testBits[i])
		c.Assert(fileMap.Test(testBits[i]), chk.Equals, false)
	}

	//verify the others are still set
	for i := 1; i < len(testBits); i = i+2 {
		c.Assert(fileMap.Test(testBits[i]), chk.Equals, true)
	}
}

func (b *bitmapTestSuite) TestBitmapBoundary(c *chk.C) {
	// For a given n, such that (n % bitsPerElement) == 0, len(bitmap of size n) == len(bitmap of size n-1) + 1
	for size := bitsPerElement; size < math.MaxUint16; size += bitsPerElement {
		c.Assert(len(NewBitMap(size)), chk.Equals, len(NewBitMap(size - 1)) + 1)
	}

	// Verify first and last bit of each element
	for firstBitInElement := 0; firstBitInElement < math.MaxUint16; firstBitInElement += bitsPerElement {
		lastBitInElement := firstBitInElement + (bitsPerElement - 1)
		bitmap := NewBitMap(lastBitInElement)

		// Test that bit is false
		c.Assert(bitmap.Test(firstBitInElement), chk.Equals, false)
		c.Assert(bitmap.Test(lastBitInElement), chk.Equals, false)

		// Set the bit, and test that it is true
		bitmap.Set(firstBitInElement)
		bitmap.Set(lastBitInElement)
		c.Assert(bitmap.Test(firstBitInElement), chk.Equals, true)
		c.Assert(bitmap.Test(lastBitInElement), chk.Equals, true)
		
		// Clear the bit, and verify again.
		bitmap.Clear(firstBitInElement)
		bitmap.Clear(lastBitInElement)
		c.Assert(bitmap.Test(firstBitInElement), chk.Equals, false)
		c.Assert(bitmap.Test(lastBitInElement), chk.Equals, false)
	}
}