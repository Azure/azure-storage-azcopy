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
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestOverwritePosixProperties(t *testing.T) {
	a := assert.New(t)
	if runtime.GOOS != "linux" {
		t.Skip("This test will run only on linux")
	}
	bsc := getBlobServiceClient()
	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)

	files := []string{
		"filea",
		"fileb",
		"filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(a, dirPath, files)

	mockedRPC := interceptor{}
	jobsAdmin.ExecuteNewCopyJobPartOrder = func(order common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return mockedRPC.intercept(order)
	}
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(3, len(mockedRPC.transfers))
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(a, "/", "/"+filepath.Base(dirPath)+"/", files[:], mockedRPC)
	})

	time.Sleep(10 * time.Second)

	newTimeStamp := time.Now()
	for _, file := range files {
		os.Chtimes(filepath.Join(dirPath, file), newTimeStamp, newTimeStamp)
	}

	//=====================================
	mockedRPC.reset()
	raw.forceWrite = "posixproperties"

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(3, len(mockedRPC.transfers))
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(a, "/", "/"+filepath.Base(dirPath)+"/", files[:], mockedRPC)
	})

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Metadata: true, Tags: true},
		Prefix:  to.Ptr(filepath.Base(dirPath)),
	})
	listBlob, err := pager.NextPage(context.TODO())

	a.Nil(err)

	for _, blob := range listBlob.Segment.BlobItems {
		a.Equal(strconv.FormatInt(newTimeStamp.UnixNano(), 10), blob.Metadata[common.POSIXCTimeMeta])
		a.Equal(strconv.FormatInt(newTimeStamp.UnixNano(), 10), blob.Metadata[common.POSIXATimeMeta])
	}
}
