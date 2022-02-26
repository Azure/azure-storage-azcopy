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
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	chk "gopkg.in/check.v1"
)

func createFileSystem(c *chk.C) (azbfs.ServiceURL, azbfs.FileSystemURL, string, string, azbfs.DirectoryURL) { // get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	// set up the filesystem to be deleted
	bfsServiceURL := GetBFSSU()
	fsURL, fsName := createNewFilesystem(c, bfsServiceURL)

	// set up directory + file as children of the filesystem to delete
	dirName := generateName("dir", 0)
	dirURL := fsURL.NewDirectoryURL(dirName)
	_, err := dirURL.Create(ctx, true)
	c.Assert(err, chk.IsNil)

	return serviceURLWithSAS, fsURL, fsName, dirName, dirURL
}

func (s *cmdIntegrationSuite) TestRemoveFilesystem(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	serviceURLWithSAS, fsURL, fsName, _, dirURL := createFileSystem(c)

	fileURL := dirURL.NewFileURL(generateName("file", 0))
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
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

	serviceURLWithSAS, _, fsName, dirName, dirURL := createFileSystem(c)

	fileURL := dirURL.NewFileURL(generateName("file", 0))
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
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

	serviceURLWithSAS, _, fsName, parentDirName, parentDirURL := createFileSystem(c)

	// set up the file to be deleted
	fileName := generateName("file", 0)
	fileURL := parentDirURL.NewFileURL(fileName)
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
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

func (s *cmdIntegrationSuite) TestRemoveListOfALDSFilesAndDirectories(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	serviceURLWithSAS, fsURL, fsName, parentDirName, parentDirURL := createFileSystem(c)

	fileName1 := generateName("file1", 0)
	fileURL1 := parentDirURL.NewFileURL(fileName1)
	_, err := fileURL1.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// set up the second file to be deleted, it sits at the top level
	fileName2 := generateName("file2", 0)
	fileURL2 := fsURL.NewRootDirectoryURL().NewFileURL(fileName2)
	_, err = fileURL2.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// make the input for list-of-files
	listOfFiles := []string{common.GenerateFullPath(parentDirName, fileName1), fileName2}

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")

	// delete file2 and dir
	fileURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName)
	raw := getDefaultRemoveRawInput(fileURLWithSAS.String())
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the file1 does not exist anymore
		_, err = fileURL1.GetProperties(ctx)
		c.Assert(err, chk.NotNil)

		// make sure the file2 does not exist anymore
		_, err = fileURL2.GetProperties(ctx)
		c.Assert(err, chk.NotNil)

		// make sure the filesystem did not get deleted
		_, err = fsURL.GetProperties(ctx)
		c.Assert(err, chk.IsNil)
	})
}

func (s *cmdIntegrationSuite) TestRemoveListOfALDSFilesWithIncludeExclude(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	serviceURLWithSAS, fsURL, fsName, _, _ := createFileSystem(c)

	// set up the second file to be deleted, it sits at the top level
	fileName := generateName("file", 0)
	fileURL := fsURL.NewRootDirectoryURL().NewFileURL(fileName)
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// make the input for list-of-files
	listOfFiles := []string{fileName}

	// attempt to use an include flag
	fileURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName)
	raw := getDefaultRemoveRawInput(fileURLWithSAS.String())
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)
	raw.include = "file*"

	// and it should fail
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(strings.Contains(err.Error(), "include"), chk.Equals, true)
	})

	// attempt to use an exclude flag
	raw.include = ""
	raw.exclude = "file*"

	// and it should fail
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(strings.Contains(err.Error(), "exclude"), chk.Equals, true)
	})
}

func (s *cmdIntegrationSuite) TestRemoveFilesystemWithFromTo(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	serviceURLWithSAS, fsURL, fsName, _, dirURL := createFileSystem(c)

	fileURL := dirURL.NewFileURL(generateName("file", 0))
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// removing the filesystem
	fsURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName).URL()
	raw := getDefaultRemoveRawInput(fsURLWithSAS.String())
	raw.fromTo = "BlobFSTrash"
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the directory does not exist anymore
		_, err = fsURL.GetProperties(ctx)
		c.Assert(err, chk.NotNil)
	})
}

func (s *cmdIntegrationSuite) TestRemoveDirectoryWithFromTo(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	serviceURLWithSAS, _, fsName, dirName, dirURL := createFileSystem(c)

	fileURL := dirURL.NewFileURL(generateName("file", 0))
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// trying to remove the dir with recursive=false should fail
	dirURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName).NewDirectoryURL(dirName)
	raw := getDefaultRemoveRawInput(dirURLWithSAS.String())
	raw.recursive = false
	raw.fromTo = "BlobFSTrash"
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

func (s *cmdIntegrationSuite) TestRemoveFileWithFromTo(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	serviceURLWithSAS, _, fsName, parentDirName, parentDirURL := createFileSystem(c)

	// set up the file to be deleted
	fileName := generateName("file", 0)
	fileURL := parentDirURL.NewFileURL(fileName)
	_, err := fileURL.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// delete single file
	fileURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName).NewDirectoryURL(parentDirName).NewFileURL(fileName)
	raw := getDefaultRemoveRawInput(fileURLWithSAS.String())
	raw.fromTo = "BlobFSTrash"
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the file does not exist anymore
		_, err = fileURL.GetProperties(ctx)
		c.Assert(err, chk.NotNil)
	})
}

func (s *cmdIntegrationSuite) TestRemoveListOfALDSFilesAndDirectoriesWithFromTo(c *chk.C) {
	// invoke the interceptor so lifecycle manager does not shut down the tests
	mockedRPC := interceptor{}
	mockedRPC.init()
	ctx := context.Background()

	// get service SAS for raw input
	serviceURLWithSAS := scenarioHelper{}.getRawAdlsServiceURLWithSAS(c)

	serviceURLWithSAS, fsURL, fsName, parentDirName, parentDirURL := createFileSystem(c)

	fileName1 := generateName("file1", 0)
	fileURL1 := parentDirURL.NewFileURL(fileName1)
	_, err := fileURL1.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// set up the second file to be deleted, it sits at the top level
	fileName2 := generateName("file2", 0)
	fileURL2 := fsURL.NewRootDirectoryURL().NewFileURL(fileName2)
	_, err = fileURL2.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.Assert(err, chk.IsNil)

	// make the input for list-of-files
	listOfFiles := []string{common.GenerateFullPath(parentDirName, fileName1), fileName2}

	// add some random files that don't actually exist
	listOfFiles = append(listOfFiles, "WUTAMIDOING")
	listOfFiles = append(listOfFiles, "DONTKNOW")

	// delete file2 and dir
	fileURLWithSAS := serviceURLWithSAS.NewFileSystemURL(fsName)
	raw := getDefaultRemoveRawInput(fileURLWithSAS.String())
	raw.listOfFilesToCopy = scenarioHelper{}.generateListOfFiles(c, listOfFiles)
	raw.fromTo = "BlobFSTrash"
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// make sure the file1 does not exist anymore
		_, err = fileURL1.GetProperties(ctx)
		c.Assert(err, chk.NotNil)

		// make sure the file2 does not exist anymore
		_, err = fileURL2.GetProperties(ctx)
		c.Assert(err, chk.NotNil)

		// make sure the filesystem did not get deleted
		_, err = fsURL.GetProperties(ctx)
		c.Assert(err, chk.IsNil)
	})
}
