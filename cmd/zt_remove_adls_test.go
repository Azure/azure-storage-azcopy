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
	"context"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	chk "gopkg.in/check.v1"
)

func (s *cmdIntegrationSuite) TestRemoveFilesystem(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	// set up the filesystem to be deleted
	bfsServiceURL := GetBFSSU()
	fsURL, fsName := createNewFilesystem(c, bfsServiceURL)

	// set up directory + file as children of the filesystem to delete
	dirURL := fsURL.NewDirectoryURL(generateName("dir", 0))
	_, err := dirURL.Create(ctx)
	c.Assert(err, chk.IsNil)
	fileURL := dirURL.NewFileURL(generateName("file", 0))
	_, err = fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)

	// removing the filesystem
	fsURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName).URL()
	raw := getDefaultRemoveRawInput(fsURLWithSAS.String())
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the directory does not exist anymore
		_, err = fsURL.GetProperties(ctx)
		c.Assert(err, chk.NotNil)
	})
}

func (s *cmdIntegrationSuite) TestRemoveDirectory(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	// set up the file system
	bfsServiceURL := GetBFSSU()
	fsURL, fsName := createNewFilesystem(c, bfsServiceURL)
	defer deleteFilesystem(c, fsURL)

	// set up the directory to be deleted
	dirName := generateName("dir", 0)
	dirURL := fsURL.NewDirectoryURL(dirName)
	_, err := dirURL.Create(ctx)
	c.Assert(err, chk.IsNil)
	fileURL := dirURL.NewFileURL(generateName("file", 0))
	_, err = fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)

	// trying to remove the dir with recursive=false should fail
	dirURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName).NewDirectoryURL(dirName)
	raw := getDefaultRemoveRawInput(dirURLWithSAS.String())
	raw.recursive = false
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
	})

	// removing the dir with recursive=true should succeed
	raw.recursive = true
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the directory does not exist anymore
		_, err = dirURL.GetProperties(ctx)
		c.Assert(err, chk.NotNil)
	})
}

func (s *cmdIntegrationSuite) TestRemoveFile(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	// set up the file system
	bfsServiceURL := GetBFSSU()
	fsURL, fsName := createNewFilesystem(c, bfsServiceURL)
	defer deleteFilesystem(c, fsURL)

	// set up the parent of the file to be deleted
	parentDirName := generateName("dir", 0)
	parentDirURL := fsURL.NewDirectoryURL(parentDirName)
	_, err := parentDirURL.Create(ctx)
	c.Assert(err, chk.IsNil)

	// set up the file to be deleted
	fileName := generateName("file", 0)
	fileURL := parentDirURL.NewFileURL(fileName)
	_, err = fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)

	// delete single file
	fileURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName).NewDirectoryURL(parentDirName).NewFileURL(fileName)
	raw := getDefaultRemoveRawInput(fileURLWithSAS.String())
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the file does not exist anymore
		_, err = fileURL.GetProperties(ctx)
		c.Assert(err, chk.NotNil)
	})
}
