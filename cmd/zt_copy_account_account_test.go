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
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
)

// test copy
func TestExcludeContainerFlagCopy(t *testing.T) {
	a := assert.New(t)
	srcBSC := scenarioHelper{}.getBlobServiceClientWithSAS(a)
	dstBSC := scenarioHelper{}.getSecondaryBlobServiceClientWithSAS(a)

	// set up 3 containers with blobs on source account
	containerNames := []string{"foo", "bar", "baz"}
	containersToIgnore := []string{containerNames[0], containerNames[1]}
	blobNames := []string{"stuff-1", "stuff-2"}
	var containerClients []*container.Client

	for _, name := range containerNames {
		// create container client
		cc := srcBSC.NewContainerClient(name)
		_, err := cc.Create(ctx, nil)
		a.NoError(err)

		// create blobs
		scenarioHelper{}.generateBlobsFromList(a, cc, blobNames, blockBlobDefaultData)

		// append to array of container clients
		containerClients = append(containerClients, cc)
	}

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	raw := getDefaultCopyRawInput(srcBSC.URL(), dstBSC.URL())
	raw.recursive = true
	raw.excludeContainer = strings.Join(containersToIgnore, ";")

	runCopyAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that each transfer is not of the excluded container names
		for _, transfer := range mockedRPC.transfers {
			a.NotNil(transfer)
			for _, excludeName := range containersToIgnore {
				a.False(strings.Contains(transfer.Source, excludeName))
				a.False(strings.Contains(transfer.Destination, excludeName))
			}
		}
	})

	// deleting test containers from source acc
	for _, container := range containerNames {
		cc := srcBSC.NewContainerClient(container)
		deleteContainer(a, cc)
	}

	// deleting test containers from dst acc
	deleteContainer(a, dstBSC.NewContainerClient(containerNames[2]))

}

func TestExcludeContainerFlagCopyNegative(t *testing.T) {
	a := assert.New(t)
	srcBSC := scenarioHelper{}.getBlobServiceClientWithSAS(a)
	dstBSC := scenarioHelper{}.getSecondaryBlobServiceClientWithSAS(a)

	// set up 2 containers with blobs on source account
	containerNames := []string{"hello", "world"}
	// ignore a container name that doesn't actually exist, AzCopy will continue as normal
	containersToIgnore := []string{"xxx"}
	blobNames := []string{"stuff-1", "stuff-2"}
	var containerClients []*container.Client

	for _, name := range containerNames {
		// create container client
		cc := srcBSC.NewContainerClient(name)
		_, err := cc.Create(ctx, nil)
		a.NoError(err)

		// create blobs
		scenarioHelper{}.generateBlobsFromList(a, cc, blobNames, blockBlobDefaultData)

		// append to array of container clients
		containerClients = append(containerClients, cc)
	}

	// set up interceptor
	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	// construct the raw input to simulate user input
	raw := getDefaultCopyRawInput(srcBSC.URL(), dstBSC.URL())
	raw.recursive = true
	raw.excludeContainer = strings.Join(containersToIgnore, ";")

	runCopyAndVerify(a, raw, func(err error) {
		a.NoError(err)

		// validate that each transfer is not of the excluded container names
		for _, transfer := range mockedRPC.transfers {
			a.NotNil(transfer)
			for _, excludeName := range containersToIgnore {
				a.False(strings.Contains(transfer.Source, excludeName))
				a.False(strings.Contains(transfer.Destination, excludeName))
			}
		}
	})

	// deleting test containers from source acc
	for i, _ := range containerNames {
		deleteContainer(a, srcBSC.NewContainerClient(containerNames[i]))
		deleteContainer(a, dstBSC.NewContainerClient(containerNames[i]))
	}
}
