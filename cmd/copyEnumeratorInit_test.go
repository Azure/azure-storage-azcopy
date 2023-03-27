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

type copyEnumeratorSuite struct{}

var _ = chk.Suite(&copyEnumeratorSuite{})

// ============================================= BLOB TRAVERSER TESTS =======================================
func (ce *copyEnumeratorSuite) TestValidateSourceDirThatExists(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	dirName := "source_dir"
	createNewDirectoryStub(c, containerURL, dirName)
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	// dir but recursive flag not set - fail
	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	c.Assert(err.Error(), chk.Equals, "cannot use directory as source without --recursive or a trailing wildcard (/*)")

	// dir but recursive flag set - pass
	cca.Recursive = true
	err = cca.validateSourceDir(blobTraverser)
	c.Assert(err, chk.IsNil)
	c.Assert(cca.IsSourceDir, chk.Equals, true)
}

func (ce *copyEnumeratorSuite) TestValidateSourceDirDoesNotExist(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	dirName := "source_dir/"
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	// dir but recursive flag not set - fail
	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	c.Assert(err.Error(), chk.Equals, "cannot use directory as source without --recursive or a trailing wildcard (/*)")

	// dir but recursive flag set - pass
	cca.Recursive = true
	err = cca.validateSourceDir(blobTraverser)
	c.Assert(err, chk.IsNil)
	c.Assert(cca.IsSourceDir, chk.Equals, true)
}

func (ce *copyEnumeratorSuite) TestValidateSourceFileExists(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	fileName := "source_file"
	_, fileName = createNewBlockBlob(c, containerURL, fileName)

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, fileName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	c.Assert(err, chk.IsNil)
	c.Assert(cca.IsSourceDir, chk.Equals, false)
}

func (ce *copyEnumeratorSuite) TestValidateSourceFileDoesNotExist(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	fileName := "source_file"

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, fileName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	cca := CookedCopyCmdArgs{StripTopDir: false, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	c.Assert(err.Error(), chk.Equals, common.FILE_NOT_FOUND)
	c.Assert(cca.IsSourceDir, chk.Equals, false)
}

func (ce *copyEnumeratorSuite) TestValidateSourceWithWildCard(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)
	c.Assert(containerURL, chk.NotNil)

	dirName := "source_dir_does_not_exist"
	// set up to create blob traverser
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	// List
	rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, dirName)
	blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, true, true, func(common.EntityType) {}, false, common.CpkOptions{}, false, false, false, common.EPreservePermissionsOption.None())

	// dir but recursive flag not set - fail
	cca := CookedCopyCmdArgs{StripTopDir: true, Recursive: false}
	err := cca.validateSourceDir(blobTraverser)
	c.Assert(err, chk.IsNil)
	c.Assert(cca.IsSourceDir, chk.Equals, false)
}
