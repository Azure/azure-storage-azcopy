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

package ste

import (
	"fmt"

	"github.com/Azure/azure-storage-blob-go/azblob"

	chk "gopkg.in/check.v1"
)

type pageBlobFromURLSuite struct{}

var _ = chk.Suite(&pageBlobFromURLSuite{})

func (s *pageBlobFromURLSuite) TestRangeWorthTransferring(c *chk.C) {
	// Arrange
	copier := urlToPageBlobCopier{}
	copier.srcPageList = &azblob.PageList{
		PageRange: []azblob.PageRange{
			{Start: 512, End: 1023},
			{Start: 2560, End: 4095},
			{Start: 7168, End: 8191},
		},
	}

	testCases := map[azblob.PageRange]bool{
		{Start: 512, End: 1023}:    true,  // fully included
		{Start: 2048, End: 3071}:   true,  // overlapping
		{Start: 3071, End: 4606}:   true,  // overlapping
		{Start: 0, End: 511}:       false, // before all ranges
		{Start: 1536, End: 2559}:   false, // in between ranges
		{Start: 15360, End: 15871}: false, // all the way out
	}

	// Action & Assert
	for testRange, expectedResult := range testCases {
		fmt.Println(testRange)
		doesContainData := copier.doesRangeContainData(testRange)
		c.Assert(doesContainData, chk.Equals, expectedResult)
	}
}
