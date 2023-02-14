// Copyright © 2018 Microsoft <wastore@microsoft.com>
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
	"math/rand"

	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

type bitmapTestSuite struct{}

var _ = chk.Suite(&bitmapTestSuite{})

func (b *bitmapTestSuite) TestBitmap(c *chk.C) {
	numOfBlocks := rand.Int31n(azblob.AppendBlobMaxBlocks)
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