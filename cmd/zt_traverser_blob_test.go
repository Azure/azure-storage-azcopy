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
	"strings"
)

type traverserBlobSuite struct{}

var _ = chk.Suite(&traverserBlobSuite{})

func (s *traverserBlobSuite) TestDetectRootBlob(c *chk.C) {
	bsu := getBSU()
	cleanBlobAccount(c, bsu)

	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Test base directory as a marker directory
	//baseMarkerDirName := generateName("basedir", 25)
	//baseDirName := baseMarkerDirName + common.AZCOPY_PATH_SEPARATOR_STRING
	objectList := []string{
		"basemarkerdir",
		"basemarkerdirfile",
		"basemarkerdirvirtdir/somefile.out",
		"basemarkerdirmarkerdir/",
		"basemarkerdirmarkerdir/file.in",
	}
	for _, o := range objectList {
		if strings.HasSuffix(o, common.AZCOPY_PATH_SEPARATOR_STRING) {
			createNewDirectoryStub(c, containerURL, strings.TrimSuffix(o, common.AZCOPY_PATH_SEPARATOR_STRING))
		} else {
			createNewBlockBlob(c, containerURL, o, false)
		}
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, objectList[0], "l")
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	propItem, isBlob, isDir, err := blobTraverser.detectRootUsingList()
	c.Assert(err, chk.IsNil)
	c.Assert(propItem, chk.NotNil)
	c.Assert(isBlob, chk.Equals, true)
	c.Assert(isDir, chk.Equals, false)

	// GetProperties
	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, objectList[0], "r")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	prop, isBlob, isDir, err := blobTraverser.detectRootUsingGetProperties()
	c.Assert(err, chk.IsNil)
	c.Assert(prop, chk.NotNil)
	c.Assert(isBlob, chk.Equals, true)
	c.Assert(isDir, chk.Equals, false)
}

func (s *traverserBlobSuite) TestDetectRootMarkerDir(c *chk.C) {
	bsu := getBSU()
	cleanBlobAccount(c, bsu)

	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Test base directory as a marker directory
	//baseMarkerDirName := generateName("basedir", 25)
	//baseDirName := baseMarkerDirName + common.AZCOPY_PATH_SEPARATOR_STRING
	objectList := []string{
		"basemarkerdir/",
		"basemarkerdir/subdir1/test.txt",
		"basemarkerdir/subdir2/",
		"basemarkerdir/subdir2/myfile.pdf",
		"basemarkerdirfile",
		"basemarkerdirvirtdir/somefile.out",
		"basemarkerdirmarkerdir/",
		"basemarkerdirmarkerdir/file.in",
	}
	for _, o := range objectList {
		if strings.HasSuffix(o, common.AZCOPY_PATH_SEPARATOR_STRING) {
			createNewDirectoryStub(c, containerURL, strings.TrimSuffix(o, common.AZCOPY_PATH_SEPARATOR_STRING))
		} else {
			createNewBlockBlob(c, containerURL, o, false)
		}
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, objectList[0], "l")
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	propItem, isBlob, isDir, err := blobTraverser.detectRootUsingList()
	c.Assert(err, chk.IsNil)
	c.Assert(propItem, chk.NotNil)
	c.Assert(isBlob, chk.Equals, false)
	c.Assert(isDir, chk.Equals, true)

	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, "basemarkerdir", "l")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	propItem, isBlob, isDir, err = blobTraverser.detectRootUsingList()
	c.Assert(err, chk.IsNil)
	c.Assert(propItem, chk.NotNil)
	c.Assert(isBlob, chk.Equals, false)
	c.Assert(isDir, chk.Equals, true)

	// GetProperties
	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, objectList[0], "r")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	prop, isBlob, isDir, err := blobTraverser.detectRootUsingGetProperties()
	c.Assert(err, chk.IsNil)
	c.Assert(prop, chk.NotNil)
	c.Assert(isBlob, chk.Equals, false)
	c.Assert(isDir, chk.Equals, true)

	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, "basemarkerdir", "r")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	prop, isBlob, isDir, err = blobTraverser.detectRootUsingGetProperties()
	c.Assert(err, chk.IsNil)
	c.Assert(prop, chk.NotNil)
	c.Assert(isBlob, chk.Equals, false)
	c.Assert(isDir, chk.Equals, true)
}

func (s *traverserBlobSuite) TestDetectRootBlobVirtualDir(c *chk.C) {
	bsu := getBSU()
	cleanBlobAccount(c, bsu)

	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// Test base directory as a marker directory
	//baseMarkerDirName := generateName("basedir", 25)
	//baseDirName := baseMarkerDirName + common.AZCOPY_PATH_SEPARATOR_STRING
	objectList := []string{
		"basemarkerdir/subdir1/test.txt",
		"basemarkerdir/subdir2/",
		"basemarkerdir/subdir2/myfile.pdf",
		"basemarkerdirfile",
		"basemarkerdirvirtdir/somefile.out",
		"basemarkerdirmarkerdir/",
		"basemarkerdirmarkerdir/file.in",
	}
	for _, o := range objectList {
		if strings.HasSuffix(o, common.AZCOPY_PATH_SEPARATOR_STRING) {
			createNewDirectoryStub(c, containerURL, strings.TrimSuffix(o, common.AZCOPY_PATH_SEPARATOR_STRING))
		} else {
			createNewBlockBlob(c, containerURL, o, false)
		}
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, "basemarkerdir/", "l")
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	propItem, isBlob, isDir, err := blobTraverser.detectRootUsingList()
	c.Assert(err, chk.IsNil)
	c.Assert(propItem, chk.IsNil)
	c.Assert(isBlob, chk.Equals, false)
	c.Assert(isDir, chk.Equals, true)

	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, "basemarkerdir", "l")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	propItem, isBlob, isDir, err = blobTraverser.detectRootUsingList()
	c.Assert(err, chk.IsNil)
	c.Assert(propItem, chk.IsNil)
	c.Assert(isBlob, chk.Equals, false)
	c.Assert(isDir, chk.Equals, true)

	// GetProperties
	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, "basemarkerdir/", "r")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	_, isBlob, isDir, err = blobTraverser.detectRootUsingGetProperties()
	c.Assert(err, chk.NotNil)

	rawBlobURLWithSAS = scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, "basemarkerdir", "r")
	blobTraverser = newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false)

	_, isBlob, isDir, err = blobTraverser.detectRootUsingGetProperties()
	c.Assert(err, chk.NotNil)
}
