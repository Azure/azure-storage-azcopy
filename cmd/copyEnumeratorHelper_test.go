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

package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	chk "gopkg.in/check.v1"
)

type copyEnumeratorHelperTestSuite struct{}

var _ = chk.Suite(&copyEnumeratorHelperTestSuite{})

func newLocalRes(path string) common.ResourceString {
	return common.ResourceString{Value: path}
}

func newRemoteRes(url string) common.ResourceString {
	r, err := SplitResourceString(url, common.ELocation.Blob())
	if err != nil {
		panic("can't parse resource string")
	}
	return r
}

func (s *copyEnumeratorHelperTestSuite) TestAddTransferPathRootsTrimmed(c *chk.C) {
	// setup
	request := common.CopyJobPartOrderRequest{
		SourceRoot:      newLocalRes("a/b/"),
		DestinationRoot: newLocalRes("y/z/"),
	}

	transfer := common.CopyTransfer{
		Source:      "a/b/c.txt",
		Destination: "y/z/c.txt",
	}

	// execute
	err := addTransfer(&request, transfer, &CookedCopyCmdArgs{})

	// assert
	c.Assert(err, chk.IsNil)
	c.Assert(request.Transfers.List[0].Source, chk.Equals, "c.txt")
	c.Assert(request.Transfers.List[0].Destination, chk.Equals, "c.txt")
}
