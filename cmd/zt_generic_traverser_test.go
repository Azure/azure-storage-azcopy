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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/minio/minio-go"
	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

type genericTraverserSuite struct{}

var _ = chk.Suite(&genericTraverserSuite{})

// On Windows, if you don't hold adequate permissions to create a symlink, tests regarding symlinks will fail.
// This is arguably annoying to dig through, therefore, we cleanly skip the test.
func trySymlink(src, dst string, c *chk.C) {
	if err := os.Symlink(src, dst); err != nil {
		if strings.Contains(err.Error(), "A required privilege is not held by the client") {
			c.Skip("client lacks required privilege to create symlinks; symlinks will not be tested")
		}
		c.Error(err)
	}
}

// GetProperties tests.
// GetProperties does not exist on Blob, as the properties come in the list call.
// While BlobFS could get properties in the future, it's currently disabled as BFS source S2S isn't set up right now, and likely won't be.
func (s *genericTraverserSuite) TestFilesGetProperties(c *chk.C) {
	fsu := getFSU()
	share, shareName := createNewAzureShare(c, fsu)
	fileName := generateAzureFileName()

	headers := azfile.FileHTTPHeaders{
		ContentType:        "text/random",
		ContentEncoding:    "testEncoding",
		ContentLanguage:    "en-US",
		ContentDisposition: "testDisposition",
		CacheControl:       "testCacheControl",
	}

	scenarioHelper{}.generateAzureFilesFromList(c, share, []string{fileName})
	_, err := share.NewRootDirectoryURL().NewFileURL(fileName).SetHTTPHeaders(ctx, headers)
	c.Assert(err, chk.IsNil)
	shareURL := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)

	pipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	// first test reading from the share itself
	traverser := newFileTraverser(&shareURL, pipeline, ctx, false, true, func() {})

	// embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object storedObject) error {
		// test all attributes
		c.Assert(object.contentType, chk.Equals, headers.ContentType)
		c.Assert(object.contentEncoding, chk.Equals, headers.ContentEncoding)
		c.Assert(object.contentLanguage, chk.Equals, headers.ContentLanguage)
		c.Assert(object.contentDisposition, chk.Equals, headers.ContentDisposition)
		c.Assert(object.cacheControl, chk.Equals, headers.CacheControl)
		seenContentType = true
		return nil
	}

	err = traverser.traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)

	// then test reading from the filename exactly, because that's a different codepath.
	seenContentType = false
	fileURL := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, fileName)
	traverser = newFileTraverser(&fileURL, pipeline, ctx, false, true, func() {})

	err = traverser.traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)
}

func (s *genericTraverserSuite) TestS3GetProperties(c *chk.C) {
	skipIfS3Disabled(c)
	client, err := createS3ClientWithMinio(createS3ResOptions{})

	if err != nil {
		// TODO: Alter all tests that use S3 credentials to just skip instead of failing
		//       This is useful for local testing, when we don't want to have to sift through errors related to S3 clients not being created
		//       Just so that we can test locally without interrupting CI.
		c.Skip("S3-based tests will not be ran as no credentials were supplied.")
		return // make syntax highlighting happy
	}

	headers := minio.PutObjectOptions{
		ContentType:        "text/random",
		ContentEncoding:    "testEncoding",
		ContentLanguage:    "en-US",
		ContentDisposition: "testDisposition",
		CacheControl:       "testCacheControl",
	}

	bucketName := generateBucketName()
	objectName := generateObjectName()
	err = client.MakeBucket(bucketName, "")
	defer deleteBucket(c, client, bucketName, false)
	c.Assert(err, chk.IsNil)

	_, err = client.PutObjectWithContext(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), int64(len(objectDefaultData)), headers)
	c.Assert(err, chk.IsNil)

	// First test against the bucket
	s3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName)

	traverser, err := newS3Traverser(&s3BucketURL, ctx, false, true, func() {})
	c.Assert(err, chk.IsNil)

	// Embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object storedObject) error {
		// test all attributes
		c.Assert(object.contentType, chk.Equals, headers.ContentType)
		c.Assert(object.contentEncoding, chk.Equals, headers.ContentEncoding)
		c.Assert(object.contentLanguage, chk.Equals, headers.ContentLanguage)
		c.Assert(object.contentDisposition, chk.Equals, headers.ContentDisposition)
		c.Assert(object.cacheControl, chk.Equals, headers.CacheControl)
		seenContentType = true
		return nil
	}

	err = traverser.traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)

	// Then, test against the object itself because that's a different codepath.
	seenContentType = false
	s3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, objectName)
	traverser, err = newS3Traverser(&s3ObjectURL, ctx, false, true, func() {})
	c.Assert(err, chk.IsNil)

	err = traverser.traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)
}

// Test follow symlink functionality
func (s *genericTraverserSuite) TestWalkWithSymlinks(c *chk.C) {
	fileNames := []string{"March 20th is international happiness day.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(symlinkTmpDir)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)

	scenarioHelper{}.generateLocalFilesFromList(c, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(c, symlinkTmpDir, fileNames)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "so long and thanks for all the fish"), c)

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
	defer os.RemoveAll(tmpDir)

	scenarioHelper{}.generateLocalFilesFromList(c, tmpDir, fileNames)
	trySymlink(tmpDir, filepath.Join(tmpDir, "spinloop"), c)

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
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir, err := ioutil.TempDir(tmpDir, "subdir")
	c.Assert(err, chk.IsNil)

	scenarioHelper{}.generateLocalFilesFromList(c, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(c, symlinkTmpDir, fileNames)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "symlinkdir"), c)

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
	fileNames := []string{"March 20th is international happiness day.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(symlinkTmpDir)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)

	scenarioHelper{}.generateLocalFilesFromList(c, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(c, symlinkTmpDir, fileNames)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "so long and thanks for all the fish"), c)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "extradir"), c)
	trySymlink(filepath.Join(tmpDir, "extradir"), filepath.Join(tmpDir, "linktolink"), c)

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
	defer os.RemoveAll(root1)
	root2 := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(root2)

	child, err := ioutil.TempDir(root2, "childdir")
	c.Assert(err, chk.IsNil)

	scenarioHelper{}.generateLocalFilesFromList(c, root2, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(c, child, fileNames)
	trySymlink(root2, filepath.Join(root1, "toroot"), c)
	trySymlink(child, filepath.Join(root1, "tochild"), c)

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

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	s3Enabled := err == nil && !isS3Disabled()
	var bucketName string

	if s3Enabled {
		bucketName = createNewBucket(c, s3Client, createS3ResOptions{})
		defer deleteBucket(c, s3Client, bucketName, true)
	}

	// test two scenarios, either blob is at the root virtual dir, or inside sub virtual dirs
	for _, storedObjectName := range []string{"sub1/sub2/singleblobisbest", "nosubsingleblob", "满汉全席.txt"} {
		// set up the container with a single blob
		blobList := []string{storedObjectName}
		scenarioHelper{}.generateBlobsFromList(c, containerURL, blobList, blockBlobDefaultData)

		// set up the directory as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(c)
		defer os.RemoveAll(dstDirName)
		dstFileName := storedObjectName
		scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, blobList)

		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, dstFileName), false, false, func() {})

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err := localTraverser.traverse(noPreProccessor, localDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(localDummyProcessor.record), chk.Equals, 1)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, false, func() {})

		// invoke the blob traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(noPreProccessor, blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(blobDummyProcessor.record), chk.Equals, 1)

		// assert the important info are correct
		c.Assert(localDummyProcessor.record[0].name, chk.Equals, blobDummyProcessor.record[0].name)
		c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, blobDummyProcessor.record[0].relativePath)

		// Azure File cannot handle names with '/' in them
		// TODO: Construct a directory URL and then build a file URL atop it in order to solve this portion of the test.
		//  We shouldn't be excluding things the traverser is actually capable of doing.
		//  Fix within scenarioHelper.generateAzureFilesFromList, since that's what causes the fail.
		if !strings.Contains(storedObjectName, "/") {
			// set up the Azure Share with a single file
			fileList := []string{storedObjectName}
			scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)

			// construct an Azure file traverser
			filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
			rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, fileList[0])
			azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, false, false, func() {})

			// invoke the file traversal with a dummy processor
			fileDummyProcessor := dummyProcessor{}
			err = azureFileTraverser.traverse(noPreProccessor, fileDummyProcessor.process, nil)
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
		err = bfsTraverser.traverse(noPreProccessor, bfsDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(bfsDummyProcessor.record), chk.Equals, 1)

		c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, bfsDummyProcessor.record[0].relativePath)
		c.Assert(localDummyProcessor.record[0].name, chk.Equals, bfsDummyProcessor.record[0].name)

		if s3Enabled {
			// set up the bucket with a single file
			s3List := []string{storedObjectName}
			scenarioHelper{}.generateObjects(c, s3Client, bucketName, s3List)

			// construct a s3 traverser
			s3DummyProcessor := dummyProcessor{}
			url := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, storedObjectName)
			S3Traverser, err := newS3Traverser(&url, ctx, false, false, func() {})
			c.Assert(err, chk.IsNil)

			err = S3Traverser.traverse(noPreProccessor, s3DummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
			c.Assert(len(s3DummyProcessor.record), chk.Equals, 1)

			c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, s3DummyProcessor.record[0].relativePath)
			c.Assert(localDummyProcessor.record[0].name, chk.Equals, s3DummyProcessor.record[0].name)
		}
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

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	s3Enabled := err == nil && !isS3Disabled() // are creds supplied, and is S3 enabled
	var bucketName string
	if s3Enabled {
		bucketName = createNewBucket(c, s3Client, createS3ResOptions{})
		defer deleteBucket(c, s3Client, bucketName, true)
	}

	// set up the container with numerous blobs
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, "")
	c.Assert(containerURL, chk.NotNil)

	// set up an Azure File Share with the same files
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)

	// set up a filesystem with the same files
	scenarioHelper{}.generateBFSPathsFromList(c, filesystemURL, fileList)

	if s3Enabled {
		// set up a bucket with the same files
		scenarioHelper{}.generateObjects(c, s3Client, bucketName, fileList)
	}

	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(dstDirName, isRecursiveOn, false, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(noPreProccessor, localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		blobTraverser := newBlobTraverser(&rawContainerURLWithSAS, p, ctx, isRecursiveOn, func() {})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(noPreProccessor, blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct an Azure File traverser
		filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
		rawFileURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
		azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, isRecursiveOn, false, func() {})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.traverse(noPreProccessor, fileDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct a directory URL and pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func() {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(noPreProccessor, bfsDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		s3DummyProcessor := dummyProcessor{}
		if s3Enabled {
			// construct and run a S3 traverser
			rawS3URL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName)
			S3Traverser, err := newS3Traverser(&rawS3URL, ctx, isRecursiveOn, false, func() {})
			c.Assert(err, chk.IsNil)
			err = S3Traverser.traverse(noPreProccessor, s3DummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
		}

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))

		if s3Enabled {
			c.Assert(len(s3DummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		}

		// if s3dummyprocessor is empty, it's A-OK because no records will be tested
		for _, storedObject := range append(append(append(blobDummyProcessor.record, fileDummyProcessor.record...), bfsDummyProcessor.record...), s3DummyProcessor.record...) {
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

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	s3Enabled := err == nil && !isS3Disabled()
	var bucketName string
	if s3Enabled {
		bucketName = createNewBucket(c, s3Client, createS3ResOptions{})
		defer deleteBucket(c, s3Client, bucketName, true)
	}

	// set up the container with numerous blobs
	virDirName := "virdir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, virDirName+"/")
	c.Assert(containerURL, chk.NotNil)

	// set up an Azure File Share with the same files
	scenarioHelper{}.generateAzureFilesFromList(c, shareURL, fileList)

	// set up the filesystem with the same files
	scenarioHelper{}.generateBFSPathsFromList(c, filesystemURL, fileList)

	if s3Enabled {
		// Set up the bucket with the same files
		scenarioHelper{}.generateObjects(c, s3Client, bucketName, fileList)
	}

	time.Sleep(time.Second * 2) // Ensure the objects' LMTs are in the past

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(filepath.Join(dstDirName, virDirName), isRecursiveOn, false, func() {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.traverse(noPreProccessor, localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawVirDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, virDirName)
		blobTraverser := newBlobTraverser(&rawVirDirURLWithSAS, p, ctx, isRecursiveOn, func() {})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.traverse(noPreProccessor, blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct an Azure File traverser
		filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, virDirName)
		azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, isRecursiveOn, false, func() {})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.traverse(noPreProccessor, fileDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct a filesystem URL & pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().NewDirectoryURL(virDirName).URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func() {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.traverse(noPreProccessor, bfsDummyProcessor.process, nil)

		s3DummyProcessor := dummyProcessor{}
		if s3Enabled {
			// construct and run a S3 traverser
			// directory object keys always end with / in S3
			rawS3URL := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, virDirName+"/")
			S3Traverser, err := newS3Traverser(&rawS3URL, ctx, isRecursiveOn, false, func() {})
			c.Assert(err, chk.IsNil)
			err = S3Traverser.traverse(noPreProccessor, s3DummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)

			// check that the results are the same length
			c.Assert(len(s3DummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		}

		// make sure the results are the same
		c.Assert(len(blobDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(fileDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		c.Assert(len(bfsDummyProcessor.record), chk.Equals, len(localIndexer.indexMap))
		// if s3 testing is disabled the s3 dummy processors' records will be empty. This is OK for appending. Nothing will happen.
		for _, storedObject := range append(append(append(blobDummyProcessor.record, fileDummyProcessor.record...), bfsDummyProcessor.record...), s3DummyProcessor.record...) {
			correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

			c.Assert(present, chk.Equals, true)
			c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)
			// Say, here's a good question, why do we have this last check?
			// None of the other tests have it.
			c.Assert(correspondingLocalFile.isMoreRecentThan(storedObject), chk.Equals, true)

			if !isRecursiveOn {
				c.Assert(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
			}
		}
	}
}
