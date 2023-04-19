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
	chk "gopkg.in/check.v1"
)

type traverserBlobSuite struct{}

var _ = chk.Suite(&traverserBlobSuite{})

func (s *traverserBlobSuite) TestIsSourceDirWithStub(c *chk.C) {
	bsc := getBSC()

	// Generate source container and blobs
	cc, containerName := createNewContainer(c, bsc)
	defer deleteContainer(c, cc)
	c.Assert(cc, chk.NotNil)

	dirName := "source_dir"
	createNewDirectoryStub(c, cc, dirName)
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(c, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(c, rawBlobURLWithSAS)
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	isDir, err := blobTraverser.IsDirectory(true)
	c.Assert(isDir, chk.Equals, true)
	c.Assert(err, chk.Equals, nil)
}

func (s *traverserBlobSuite) TestIsSourceDirWithNoStub(c *chk.C) {
	bsc := getBSC()

	// Generate source container and blobs
	cc, containerName := createNewContainer(c, bsc)
	defer deleteContainer(c, cc)
	c.Assert(cc, chk.NotNil)

	dirName := "source_dir/"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(c, containerName, dirName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(c, rawBlobURLWithSAS)
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	isDir, err := blobTraverser.IsDirectory(true)
	c.Assert(isDir, chk.Equals, true)
	c.Assert(err, chk.Equals, nil)
}

func (s *traverserBlobSuite) TestIsSourceFileExists(c *chk.C) {
	bsc := getBSC()

	// Generate source container and blobs
	cc, containerName := createNewContainer(c, bsc)
	defer deleteContainer(c, cc)
	c.Assert(cc, chk.NotNil)

	fileName := "source_file"
	_, fileName = createNewBlockBlob(c, cc, fileName)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(c, containerName, fileName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(c, rawBlobURLWithSAS)
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	isDir, err := blobTraverser.IsDirectory(true)
	c.Assert(isDir, chk.Equals, false)
	c.Assert(err, chk.IsNil)
}

func (s *traverserBlobSuite) TestIsSourceFileDoesNotExist(c *chk.C) {
	bsc := getBSC()

	// Generate source container and blobs
	cc, containerName := createNewContainer(c, bsc)
	defer deleteContainer(c, cc)
	c.Assert(cc, chk.NotNil)

	fileName := "file_does_not_exist"
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(c, containerName, fileName).URL()
	serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(c, rawBlobURLWithSAS)
	blobTraverser := newBlobTraverser(rawBlobURLWithSAS, serviceClientWithSAS, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	isDir, err := blobTraverser.IsDirectory(true)
	c.Assert(isDir, chk.Equals, false)
	c.Assert(err.Error(), chk.Equals, common.FILE_NOT_FOUND)
}
