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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestOverwritePosixProperties(c *chk.C) {
	if runtime.GOOS != "linux" {
		c.Skip("This test will run only on linux")
	}
	
	bsc := getBSC()
	containerClient, containerName := createNewContainer(c, bsc)
	defer deleteContainer(c, containerClient)

	files := []string{
		"filea",
		"fileb",
		"filec",
	}

	dirPath := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dirPath)
	scenarioHelper{}.generateLocalFilesFromList(c, dirPath, files)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawBlobURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
	raw := getDefaultRawCopyInput(dirPath, rawBlobURLWithSAS.String())
	raw.recursive = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 3)
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(c, "/", "/"+filepath.Base(dirPath)+"/", files[:], mockedRPC)
	})

	time.Sleep(10 * time.Second)

	newTimeStamp := time.Now()
	for _, file := range files {
		os.Chtimes(filepath.Join(dirPath, file), newTimeStamp, newTimeStamp)
	}

	//=====================================
	mockedRPC.reset()
	raw.forceWrite = "posixproperties"

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 3)
		// trim / and /folder/ off
		validateDownloadTransfersAreScheduled(c, "/", "/"+filepath.Base(dirPath)+"/", files[:], mockedRPC)
	})

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Metadata: true, Tags: true},
		Prefix: to.Ptr(filepath.Base(dirPath)),
	})
	listBlob, err := pager.NextPage(context.TODO())

	c.Assert(err, chk.Equals, nil)

	for _, blob := range listBlob.Segment.BlobItems {
		c.Assert(blob.Metadata[common.POSIXCTimeMeta], chk.Equals, strconv.FormatInt(newTimeStamp.UnixNano(), 10))
		c.Assert(blob.Metadata[common.POSIXATimeMeta], chk.Equals, strconv.FormatInt(newTimeStamp.UnixNano(), 10))
	}
}
