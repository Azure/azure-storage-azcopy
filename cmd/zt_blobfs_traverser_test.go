// Copyright © 2019 Microsoft <wastore@microsoft.com>
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
	"path/filepath"
	"strings"

	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

type bfsTraverserSuite struct{}

var _ = chk.Suite(&bfsTraverserSuite{})

// validate traversing a single Blob, a single Azure File, and a single local file
// compare that the traversers get consistent results
func (s *bfsTraverserSuite) TestBfsTraverserWithSingleObject(c *chk.C) {
	bfsu := getBFSU()
	filesystemURL, _ := createNewFilesystem(c, bfsu)
	defer deleteFilesystem(c, filesystemURL)

	// test two scenarios, either blob is at the root virtual dir, or inside sub virtual dirs
	for _, storedObjectName := range []string{"sub1/sub2/singleblobisbest", "nosubsingleblob", "满汉全席.txt"} {
		// set up the container with a single blob
		blobList := []string{storedObjectName}

		// set up the directory as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(c)
		dstFileName := storedObjectName
		scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, blobList)

		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, dstFileName), false, func() {})

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err := localTraverser.traverse(localDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(localDummyProcessor.record), chk.Equals, 1)

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
func (s *bfsTraverserSuite) TestBfsTraverserContainerAndLocalDirectory(c *chk.C) {
	bfsu := getBFSU()
	filesystemURL, _ := createNewFilesystem(c, bfsu)
	defer deleteFilesystem(c, filesystemURL)

	// set up a filesystem with the same files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlobFS(c, filesystemURL, "")

	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(dstDirName, isRecursiveOn, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a directory URL and pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func() {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(bfsDummyProcessor.process, nil)

		c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, storedObject := range bfsDummyProcessor.record {
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
func (s *bfsTraverserSuite) TestBfsTraverserWithVirtualAndLocalDirectory(c *chk.C) {
	bfsu := getBFSU()
	filesystemURL, _ := createNewFilesystem(c, bfsu)
	defer deleteFilesystem(c, filesystemURL)

	// set up the container with numerous blobs
	virDirName := "virdir"
	// set up the filesystem with the same files
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlobFS(c, filesystemURL, virDirName+"/")

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, virDirName), isRecursiveOn, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a filesystem URL & pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().NewDirectoryURL(virDirName + "/").URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func() {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(bfsDummyProcessor.process, nil)

		c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		for _, storedObject := range bfsDummyProcessor.record {
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
