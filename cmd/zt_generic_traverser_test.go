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
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

type genericTraverserSuite struct{}

var _ = chk.Suite(&genericTraverserSuite{})

// Test follow symlink functionality
func (s *genericTraverserSuite) TestWalkWithSymlinks(c *chk.C) {
	fileNames := []string{"March 20th is international happiness day.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(c)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)

	for _, v := range fileNames {
		f, err := os.Create(filepath.Join(tmpDir, v))
		f2, err2 := os.Create(filepath.Join(symlinkTmpDir, v))
		c.Assert(err, chk.IsNil)
		c.Assert(err2, chk.IsNil)
		c.Assert(f.Close(), chk.IsNil)
		c.Assert(f2.Close(), chk.IsNil)
	}

	c.Assert(os.Symlink(symlinkTmpDir, filepath.Join(tmpDir, "so long and thanks for all the fish")), chk.IsNil)

	fileCount := 0
	c.Assert(WalkWithSymlinks(tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	}), chk.IsNil)

	// 3 files live in base, 3 files live in symlink
	c.Assert(fileCount, chk.Equals, 6)
}

// Test cancel symlink loop functionality
func (s *genericTraverserSuite) TestWalkWithSymlinksBreakLoop(c *chk.C) {
	fileNames := []string{"stonks.txt", "jaws but its a baby shark.mp3", "my crow soft.txt"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	c.Assert(os.Symlink(tmpDir, filepath.Join(tmpDir, "spinloop")), chk.IsNil)

	for _, v := range fileNames {
		f, err := os.Create(filepath.Join(tmpDir, v))
		c.Assert(err, chk.IsNil)
		c.Assert(f.Close(), chk.IsNil)
	}

	// Only 3 files should ever be found.
	// This is because the symlink links back to the root dir
	fileCount := 0
	c.Assert(WalkWithSymlinks(tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	}), chk.IsNil)

	c.Assert(fileCount, chk.Equals, 3)
}

// Test ability to dedupe within the same directory
func (s *genericTraverserSuite) TestWalkWithSymlinksDedupe(c *chk.C) {
	fileNames := []string{"stonks.txt", "jaws but its a baby shark.mp3", "my crow soft.txt"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	symlinkTmpDir := filepath.Join(tmpDir, "subdir")
	c.Assert(os.Mkdir(symlinkTmpDir, os.ModeDir), chk.IsNil)
	c.Assert(os.Symlink(symlinkTmpDir, filepath.Join(tmpDir, "symlinkdir")), chk.IsNil)

	for _, v := range fileNames {
		f, err := os.Create(filepath.Join(tmpDir, v))
		f2, err2 := os.Create(filepath.Join(symlinkTmpDir, v))
		c.Assert(err, chk.IsNil)
		c.Assert(err2, chk.IsNil)
		c.Assert(f.Close(), chk.IsNil)
		c.Assert(f2.Close(), chk.IsNil)
	}

	// Only 6 files should ever be found.
	// 3 in the root dir, 3 in subdir, then symlinkdir should be ignored because it's been seen.
	fileCount := 0
	c.Assert(WalkWithSymlinks(tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	}), chk.IsNil)

	c.Assert(fileCount, chk.Equals, 6)
}

// Test ability to only get the output of one symlink when two point to the same place
func (s *genericTraverserSuite) TestWalkWithSymlinksMultitarget(c *chk.C) {
	fileNames := []string{"my cat keeps sending me to bed.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(c)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)

	for _, v := range fileNames {
		f, err := os.Create(filepath.Join(tmpDir, v))
		f2, err2 := os.Create(filepath.Join(symlinkTmpDir, v))
		c.Assert(err, chk.IsNil)
		c.Assert(err2, chk.IsNil)
		c.Assert(f.Close(), chk.IsNil)
		c.Assert(f2.Close(), chk.IsNil)
	}

	c.Assert(os.Symlink(symlinkTmpDir, filepath.Join(tmpDir, "so long and thanks for all the fish")), chk.IsNil)
	c.Assert(os.Symlink(symlinkTmpDir, filepath.Join(tmpDir, "extradir")), chk.IsNil)
	c.Assert(os.Symlink(filepath.Join(tmpDir, "extradir"), filepath.Join(tmpDir, "linktolink")), chk.IsNil)

	fileCount := 0
	c.Assert(WalkWithSymlinks(tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	}), chk.IsNil)

	// 3 files live in base, 3 files live in first symlink, second & third symlink is ignored.
	c.Assert(fileCount, chk.Equals, 6)
}

func (s *genericTraverserSuite) TestWalkWithSymlinksToParentAndChild(c *chk.C) {
	fileNames := []string{"file1.txt", "file2.txt", "file3.txt"}

	root1 := scenarioHelper{}.generateLocalDirectory(c)
	root2 := scenarioHelper{}.generateLocalDirectory(c)
	child := filepath.Join(root2, "childdir")

	c.Assert(os.Mkdir(child, os.ModeDir), chk.IsNil)
	c.Assert(os.Symlink(root2, filepath.Join(root1, "toroot")), chk.IsNil)
	c.Assert(os.Symlink(child, filepath.Join(root1, "tochild")), chk.IsNil)

	for _, v := range fileNames {
		f, err := os.Create(filepath.Join(root2, v))
		f2, err2 := os.Create(filepath.Join(child, v))
		c.Assert(err, chk.IsNil)
		c.Assert(err2, chk.IsNil)
		c.Assert(f.Close(), chk.IsNil)
		c.Assert(f2.Close(), chk.IsNil)
	}

	fileCount := 0
	c.Assert(WalkWithSymlinks(root1, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	}), chk.IsNil)

	// 6 files total live under toroot. tochild should be ignored (or if tochild was traversed first, child will be ignored on toroot).
	c.Assert(fileCount, chk.Equals, 6)
}

// validate traversing a single Blob, a single Azure File, and a single local file
// compare that the traversers get consistent results
func (s *genericTraverserSuite) TestTraverserWithSingleObject(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	fsu := getFSU()
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)

	bfsu := GetBFSSU()
	filesystemURL, _ := createNewFilesystem(c, bfsu)
	defer deleteFilesystem(c, filesystemURL)

	// test two scenarios, either blob is at the root virtual dir, or inside sub virtual dirs
	for _, storedObjectName := range []string{"sub1/sub2/singleblobisbest", "nosubsingleblob", "满汉全席.txt"} {
		// set up the container with a single blob
		blobList := []string{storedObjectName}
		scenarioHelper{}.generateBlobsFromList(c, containerURL, blobList, blockBlobDefaultData)

		// set up the directory as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(c)
		dstFileName := storedObjectName
		scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, blobList)

		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, dstFileName), false, false, func() {})

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err := localTraverser.traverse(localDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(localDummyProcessor.record), chk.Equals, 1)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, false, func() {})

		// invoke the blob traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(blobDummyProcessor.record), chk.Equals, 1)

		// assert the important info are correct
		c.Assert(localDummyProcessor.record[0].name, chk.Equals, blobDummyProcessor.record[0].name)
		c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, blobDummyProcessor.record[0].relativePath)

		// Azure File cannot handle names with '/' in them
		if !strings.Contains(storedObjectName, "/") {
			// set up the Azure Share with a single file
			fileList := []string{storedObjectName}
			scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)

			// construct an Azure file traverser
			filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
			rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, fileList[0])
			azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, false, func() {})

			// invoke the file traversal with a dummy processor
			fileDummyProcessor := dummyProcessor{}
			err = azureFileTraverser.traverse(fileDummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
			c.Assert(len(fileDummyProcessor.record), chk.Equals, 1)

			c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, fileDummyProcessor.record[0].relativePath)
			c.Assert(localDummyProcessor.record[0].name, chk.Equals, fileDummyProcessor.record[0].name)
		}

		// set up the filesystem with a single file
		bfsList := []string{storedObjectName}
		scenarioHelper{}.generateBFSPathsFromList(c, filesystemURL, bfsList)

		// construct a BlobFS traverser
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFileURL := filesystemURL.NewRootDirectoryURL().NewFileURL(bfsList[0]).URL()
		bfsTraverser := newBlobFSTraverser(&rawFileURL, bfsPipeline, ctx, false, func() {})

		// Construct and run a dummy processor for bfs
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(bfsDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(bfsDummyProcessor.record), chk.Equals, 1)

		c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, bfsDummyProcessor.record[0].relativePath)
		c.Assert(localDummyProcessor.record[0].name, chk.Equals, bfsDummyProcessor.record[0].name)
	}
}

// validate traversing a container, a share, and a local directory containing the same objects
// compare that traversers get consistent results
func (s *genericTraverserSuite) TestTraverserContainerAndLocalDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	fsu := getFSU()
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)

	bfsu := GetBFSSU()
	filesystemURL, _ := createNewFilesystem(c, bfsu)
	defer deleteFilesystem(c, filesystemURL)

	// set up the container with numerous blobs
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	c.Assert(containerURL, chk.NotNil)

	// set up an Azure File Share with the same files
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)

	// set up a filesystem with the same files
	scenarioHelper{}.generateBFSPathsFromList(c, filesystemURL, fileList)

	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(dstDirName, isRecursiveOn, false, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		blobTraverser := newBlobTraverser(&rawContainerURLWithSAS, p, ctx, isRecursiveOn, func() {})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct an Azure File traverser
		filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
		rawFileURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
		azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, isRecursiveOn, func() {})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.traverse(fileDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct a directory URL and pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func() {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(bfsDummyProcessor.process, nil)

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, storedObject := range append(append(blobDummyProcessor.record, fileDummyProcessor.record...), bfsDummyProcessor.record...) {
			correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)

			if !isRecursiveOn {
				c.Assert(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}

// validate traversing a virtual and a local directory containing the same objects
// compare that blob and local traversers get consistent results
func (s *genericTraverserSuite) TestTraverserWithVirtualAndLocalDirectory(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	fsu := getFSU()
	shareURL, shareName := createNewAzureShare(c, fsu)
	defer deleteShare(c, shareURL)

	bfsu := GetBFSSU()
	filesystemURL, _ := createNewFilesystem(c, bfsu)
	defer deleteFilesystem(c, filesystemURL)

	// set up the container with numerous blobs
	virDirName := "virdir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, virDirName+"/")
	c.Assert(containerURL, chk.NotNil)

	// set up an Azure File Share with the same files
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)

	// set up the filesystem with the same files
	scenarioHelper{}.generateBFSPathsFromList(c, filesystemURL, fileList)

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, virDirName), isRecursiveOn, false, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawVirDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, virDirName)
		blobTraverser := newBlobTraverser(&rawVirDirURLWithSAS, p, ctx, isRecursiveOn, func() {})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct an Azure File traverser
		filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, virDirName)
		azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, isRecursiveOn, func() {})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.traverse(fileDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct a filesystem URL & pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().NewDirectoryURL(virDirName).URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func() {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(bfsDummyProcessor.process, nil)

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, storedObject := range append(append(blobDummyProcessor.record, fileDummyProcessor.record...), bfsDummyProcessor.record...) {
			correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
			c.Assert(correspondingLocalFile.isMoreRecentThan(storedObject), chk.Equals, true)

			if !isRecursiveOn {
				c.Assert(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}
