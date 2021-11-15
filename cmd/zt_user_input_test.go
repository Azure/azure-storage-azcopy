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

package cmd

import (
	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestCPKEncryptionInputTest(c *chk.C) {
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	dirPath := "this/is/a/dummy/path"
	rawDFSEndpointWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)
	raw := getDefaultRawCopyInput(dirPath, rawDFSEndpointWithSAS.String())
	raw.recursive = true
	raw.cpkInfo = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(err.Error(), StringContains, "client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
	})

	mockedRPC.reset()
	raw.cpkInfo = false
	raw.cpkScopeInfo = "dummyscope"
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(err.Error(), StringContains, "client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
	})

	rawContainerURL := scenarioHelper{}.getContainerURL(c, "testcpkcontainer")
	raw2 := getDefaultRawCopyInput(dirPath, rawContainerURL.String())
	raw2.recursive = true
	raw2.cpkInfo = true

	_, err := raw2.cook()
	c.Assert(err, chk.IsNil)
}
