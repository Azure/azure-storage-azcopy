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
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
)

func TestCPKEncryptionInputTest(t *testing.T) {
	a := assert.New(t)
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	dirPath := "this/is/a/dummy/path"
	rawDFSEndpointWithSAS := scenarioHelper{}.getDatalakeServiceClientWithSAS(a)
	raw := getDefaultRawCopyInput(dirPath, rawDFSEndpointWithSAS.DFSURL())
	raw.recursive = true
	raw.cpkInfo = true

	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Contains(err.Error(), "client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
	})

	mockedRPC.reset()
	raw.cpkInfo = false
	raw.cpkScopeInfo = "dummyscope"
	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)
		a.Contains(err.Error(), "client provided keys (CPK) based encryption is only supported with blob endpoints (blob.core.windows.net)")
	})

	rawContainerURL := scenarioHelper{}.getContainerClient(a, "testcpkcontainer")
	raw2 := getDefaultRawCopyInput(dirPath, rawContainerURL.URL())
	raw2.recursive = true
	raw2.cpkInfo = true

	_, err := raw2.cook()
	a.NoError(err)
}
