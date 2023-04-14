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

package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	chk "gopkg.in/check.v1"
)

type pageBlobFromURLSuite struct{}

var _ = chk.Suite(&pageBlobFromURLSuite{})

func (s *pageBlobFromURLSuite) TestRangeWorthTransferring(c *chk.C) {
	// Arrange
	copier := pageRangeOptimizer{}
	copier.srcPageList = &pageblob.PageList{
		PageRange: []*pageblob.PageRange{
			{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))},
			{Start: to.Ptr(int64(2560)), End: to.Ptr(int64(4095))},
			{Start: to.Ptr(int64(7168)), End: to.Ptr(int64(8191))},
		},
	}

	testCases := map[pageblob.PageRange]bool{
		{Start: to.Ptr(int64(512)), End: to.Ptr(int64(1023))}:    true,  // fully included
		{Start: to.Ptr(int64(2048)), End: to.Ptr(int64(3071))}:   true,  // overlapping
		{Start: to.Ptr(int64(3071)), End: to.Ptr(int64(4606))}:   true,  // overlapping
		{Start: to.Ptr(int64(0)), End: to.Ptr(int64(511))}:       false, // before all ranges
		{Start: to.Ptr(int64(1536)), End: to.Ptr(int64(2559))}:   false, // in between ranges
		{Start: to.Ptr(int64(15360)), End: to.Ptr(int64(15871))}: false, // all the way out
	}

	// Action & Assert
	for testRange, expectedResult := range testCases {
		doesContainData := copier.doesRangeContainData(testRange)
		c.Assert(doesContainData, chk.Equals, expectedResult)
	}
}
