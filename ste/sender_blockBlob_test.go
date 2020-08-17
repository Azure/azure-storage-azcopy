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
	"fmt"

	chk "gopkg.in/check.v1"
)

type blockBlobSuite struct{}

var _ = chk.Suite(&blockBlobSuite{})

func (s *blockBlobSuite) TestGetVerifiedChunkParams(c *chk.C) {
	// Mock required params
	transferInfo := TransferInfo{
		BlockSize:  4195352576, // 4001MiB
		Source:     "tmpSrc",
		SourceSize: 8389656576, // 8001MiB
	}

	//Verify memory limit
	memLimit := int64(2097152000) // 2000Mib
	expectedErr := fmt.Sprintf("Cannot use a block size of 3.91GiB. AzCopy is limited to use only 1.95GiB of memory")
	_, _, err := getVerifiedChunkParams(transferInfo, memLimit)
	c.Assert(err.Error(), chk.Equals, expectedErr)

	// Verify large block Size
	memLimit = int64(8388608000) // 8000MiB
	expectedErr = fmt.Sprintf("block size of 3.91GiB for file tmpSrc of size 7.81GiB exceeds maxmimum allowed block size for a BlockBlob")
	_, _, err = getVerifiedChunkParams(transferInfo, memLimit)
	c.Assert(err.Error(), chk.Equals, expectedErr)

	// High block count
	transferInfo.SourceSize = 2147483648 //16GiB
	transferInfo.BlockSize = 2048        // 2KiB
	expectedErr = fmt.Sprintf("Block size 2048 for source of size 2147483648 is not correct. Number of blocks will exceed the limit")
	_, _, err = getVerifiedChunkParams(transferInfo, memLimit)
	c.Assert(err.Error(), chk.Equals, expectedErr)

}
