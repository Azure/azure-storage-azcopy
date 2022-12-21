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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

type traverserBlobSuite struct{}

var _ = chk.Suite(&traverserBlobSuite{})

func (s *traverserBlobSuite) TestIsSourceDir(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	defer deleteContainer(c, containerURL)

	dirName := "source_dir"
	createNewDirectoryStub(c, containerURL, dirName)
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	isDir, _ := blobTraverser.IsDirectory(true)
	c.Assert(isDir, chk.Equals, true)
}

func (s *traverserBlobSuite) TestIsSourceDirWithNoStub(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	defer deleteContainer(c, containerURL)

	dirName := "source_dir/"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	isDir, _ := blobTraverser.IsDirectory(true)
	c.Assert(isDir, chk.Equals, true)
}

func (s *traverserBlobSuite) TestFailIfFileDoesNotExist(c *chk.C) {
	// copy non-existent file to local
	bsu := getBSU()
	_, cName := createNewContainer(c, bsu)
	// set up container name

	dstDirName := scenarioHelper{}.generateLocalDirectory(c)

	rawBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, cName, "non_existent_blob")

	raw := getDefaultRawCopyInput(rawBlobURL.String(), dstDirName)
	raw.recursive = false

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	_, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// Test and ensure only one file is being downloaded
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 0)
	})
}
