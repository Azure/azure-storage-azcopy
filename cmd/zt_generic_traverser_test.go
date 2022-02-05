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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	gcpUtils "cloud.google.com/go/storage"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/minio/minio-go"
	chk "gopkg.in/check.v1"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/azbfs"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/ste"
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
	traverser := newFileTraverser(&shareURL, pipeline, ctx, false, true, func(common.EntityType) {})

	// embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object StoredObject) error {
		if object.entityType == common.EEntityType.File() {
			// test all attributes (but only for files, since folders don't have them)
			c.Assert(object.contentType, chk.Equals, headers.ContentType)
			c.Assert(object.contentEncoding, chk.Equals, headers.ContentEncoding)
			c.Assert(object.contentLanguage, chk.Equals, headers.ContentLanguage)
			c.Assert(object.contentDisposition, chk.Equals, headers.ContentDisposition)
			c.Assert(object.cacheControl, chk.Equals, headers.CacheControl)
			seenContentType = true
		}
		return nil
	}

	err = traverser.Traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)

	// then test reading from the filename exactly, because that's a different codepath.
	seenContentType = false
	fileURL := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, fileName)
	traverser = newFileTraverser(&fileURL, pipeline, ctx, false, true, func(common.EntityType) {})

	err = traverser.Traverse(noPreProccessor, processor, nil)
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

	credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
	traverser, err := newS3Traverser(credentialInfo.CredentialType, &s3BucketURL, ctx, false, true, func(common.EntityType) {})
	c.Assert(err, chk.IsNil)

	// Embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object StoredObject) error {
		// test all attributes
		c.Assert(object.contentType, chk.Equals, headers.ContentType)
		c.Assert(object.contentEncoding, chk.Equals, headers.ContentEncoding)
		c.Assert(object.contentLanguage, chk.Equals, headers.ContentLanguage)
		c.Assert(object.contentDisposition, chk.Equals, headers.ContentDisposition)
		c.Assert(object.cacheControl, chk.Equals, headers.CacheControl)
		seenContentType = true
		return nil
	}

	err = traverser.Traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)

	// Then, test against the object itself because that's a different codepath.
	seenContentType = false
	s3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, objectName)
	credentialInfo = common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
	traverser, err = newS3Traverser(credentialInfo.CredentialType, &s3ObjectURL, ctx, false, true, func(common.EntityType) {})
	c.Assert(err, chk.IsNil)

	err = traverser.Traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)
}

func (s *genericTraverserSuite) TestGCPGetProperties(c *chk.C) {
	skipIfGCPDisabled(c)
	client, err := createGCPClientWithGCSSDK()

	if err != nil {
		c.Skip("GCP-based tests will not be run as no credentials were supplied.")
		return
	}

	headers := gcpUtils.ObjectAttrsToUpdate{
		ContentType:        "text/html",
		ContentEncoding:    "gzip",
		ContentLanguage:    "en",
		ContentDisposition: "inline",
		CacheControl:       "no-cache",
	}

	bucketName := generateBucketName()
	objectName := generateObjectName()
	bkt := client.Bucket(bucketName)
	err = bkt.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
	defer deleteGCPBucket(c, client, bucketName, false)
	c.Assert(err, chk.IsNil)

	reader := strings.NewReader(objectDefaultData)
	obj := bkt.Object(objectName)
	wc := obj.NewWriter(ctx)
	n, err := io.Copy(wc, reader)
	c.Assert(err, chk.IsNil)
	c.Assert(n, chk.Equals, int64(len(objectDefaultData)))
	err = wc.Close()
	c.Assert(err, chk.IsNil)
	_, err = obj.Update(ctx, headers)
	c.Assert(err, chk.IsNil)

	// First test against the bucket
	gcpBucketURL := scenarioHelper{}.getRawGCPBucketURL(c, bucketName)

	traverser, err := newGCPTraverser(&gcpBucketURL, ctx, false, true, func(common.EntityType) {})
	c.Assert(err, chk.IsNil)

	// Embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object StoredObject) error {
		// test all attributes
		c.Assert(object.contentType, chk.Equals, headers.ContentType)
		c.Assert(object.contentEncoding, chk.Equals, headers.ContentEncoding)
		c.Assert(object.contentLanguage, chk.Equals, headers.ContentLanguage)
		c.Assert(object.contentDisposition, chk.Equals, headers.ContentDisposition)
		c.Assert(object.cacheControl, chk.Equals, headers.CacheControl)
		seenContentType = true
		return nil
	}

	err = traverser.Traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)

	// Then, test against the object itself because that's a different codepath.
	seenContentType = false
	gcpObjectURL := scenarioHelper{}.getRawGCPObjectURL(c, bucketName, objectName)
	traverser, err = newGCPTraverser(&gcpObjectURL, ctx, false, true, func(common.EntityType) {})
	c.Assert(err, chk.IsNil)

	err = traverser.Traverse(noPreProccessor, processor, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(seenContentType, chk.Equals, true)
}

// Test follow symlink functionality
func (s *genericTraverserSuite) TestWalkWithSymlinks_ToFolder(c *chk.C) {
	fileNames := []string{"March 20th is international happiness day.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(symlinkTmpDir)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)

	scenarioHelper{}.generateLocalFilesFromList(c, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(c, symlinkTmpDir, fileNames)
	dirLinkName := "so long and thanks for all the fish"
	time.Sleep(2 * time.Second) // to be sure to get different LMT for link, compared to root, so we can make assertions later about whose fileInfo we get
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, dirLinkName), c)

	fileCount := 0
	sawLinkTargetDir := false
	c.Assert(WalkWithSymlinks(nil, tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			if fi.Name() == dirLinkName {
				sawLinkTargetDir = true
				s, _ := os.Stat(symlinkTmpDir)
				c.Assert(fi.ModTime().UTC(), chk.Equals, s.ModTime().UTC())
			}
			return nil
		}

		fileCount++
		return nil
	},
		true), chk.IsNil)

	// 3 files live in base, 3 files live in symlink
	c.Assert(fileCount, chk.Equals, 6)
	c.Assert(sawLinkTargetDir, chk.Equals, true)
}

// Next test is temporarily disabled, to avoid changing functionality near 10.4 release date
/*
// symlinks are not just to folders. They may be to individual files
func (s *genericTraverserSuite) TestWalkWithSymlinks_ToFile(c *chk.C) {
	mainDirFilenames := []string{"iAmANormalFile.txt"}
	symlinkTargetFilenames := []string{"iAmASymlinkTargetFile.txt"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(symlinkTmpDir)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)

	scenarioHelper{}.generateLocalFilesFromList(c, tmpDir, mainDirFilenames)
	scenarioHelper{}.generateLocalFilesFromList(c, symlinkTmpDir, symlinkTargetFilenames)
	trySymlink(filepath.Join(symlinkTmpDir, symlinkTargetFilenames[0]), filepath.Join(tmpDir, "iPointToTheSymlink"), c)
	trySymlink(filepath.Join(symlinkTmpDir, symlinkTargetFilenames[0]), filepath.Join(tmpDir, "iPointToTheSameSymlink"), c)

	fileCount := 0
	c.Assert(WalkWithSymlinks(tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		if fi.Name() != "iAmANormalFile.txt" {
			c.Assert(strings.HasPrefix(path, tmpDir), chk.Equals, true)                  // the file appears to have the location of the symlink source (not the dest)
			c.Assert(strings.HasPrefix(filepath.Base(path), "iPoint"), chk.Equals, true) // the file appears to have the name of the symlink source (not the dest)
			c.Assert(strings.HasPrefix(fi.Name(), "iPoint"), chk.Equals, true)           // and it still appears to have that name when we look it the fileInfo
		}
		return nil
	},
		true), chk.IsNil)

	// 1 file is in base, 2 are pointed to by a symlink (the fact that both point to the same file is does NOT prevent us
	// processing them both. For efficiency of dedupe algorithm, we only dedupe directories, not files).
	c.Assert(fileCount, chk.Equals, 3)
}
*/

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
	c.Assert(WalkWithSymlinks(nil, tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		true), chk.IsNil)

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
	c.Assert(WalkWithSymlinks(nil, tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		true), chk.IsNil)

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
	c.Assert(WalkWithSymlinks(nil, tmpDir, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		true), chk.IsNil)

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
	c.Assert(WalkWithSymlinks(nil, root1, func(path string, fi os.FileInfo, err error) error {
		c.Assert(err, chk.IsNil)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		true), chk.IsNil)

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
	gcpClient, err := createGCPClientWithGCSSDK()
	gcpEnabled := err == nil && gcpTestsDisabled()
	var bucketName string
	var bucketNameGCP string
	if s3Enabled {
		bucketName = createNewBucket(c, s3Client, createS3ResOptions{})
		defer deleteBucket(c, s3Client, bucketName, true)
	}
	if gcpEnabled {
		bucketNameGCP = createNewGCPBucket(c, gcpClient)
		defer deleteGCPBucket(c, gcpClient, bucketNameGCP, true)
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
		localTraverser := newLocalTraverser(nil, filepath.Join(dstDirName, dstFileName), false, false, func(common.EntityType) {})

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err := localTraverser.Traverse(noPreProccessor, localDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)
		c.Assert(len(localDummyProcessor.record), chk.Equals, 1)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawBlobURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, blobList[0])
		blobTraverser := newBlobTraverser(&rawBlobURLWithSAS, p, ctx, false, false,
			func(common.EntityType) {}, false, common.CpkOptions{})

		// invoke the blob traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
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
			azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, false, false, func(common.EntityType) {})

			// invoke the file traversal with a dummy processor
			fileDummyProcessor := dummyProcessor{}
			err = azureFileTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
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
		bfsTraverser := newBlobFSTraverser(&rawFileURL, bfsPipeline, ctx, false, func(common.EntityType) {})

		// Construct and run a dummy processor for bfs
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.Traverse(noPreProccessor, bfsDummyProcessor.process, nil)
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
			credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
			S3Traverser, err := newS3Traverser(credentialInfo.CredentialType, &url, ctx, false, false, func(common.EntityType) {})
			c.Assert(err, chk.IsNil)

			err = S3Traverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
			c.Assert(len(s3DummyProcessor.record), chk.Equals, 1)

			c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, s3DummyProcessor.record[0].relativePath)
			c.Assert(localDummyProcessor.record[0].name, chk.Equals, s3DummyProcessor.record[0].name)
		}
		if gcpEnabled {
			gcpList := []string{storedObjectName}
			scenarioHelper{}.generateGCPObjects(c, gcpClient, bucketNameGCP, gcpList)

			gcpDummyProcessor := dummyProcessor{}
			gcpURL := scenarioHelper{}.getRawGCPObjectURL(c, bucketNameGCP, storedObjectName)
			GCPTraverser, err := newGCPTraverser(&gcpURL, ctx, false, false, func(entityType common.EntityType) {})
			c.Assert(err, chk.IsNil)

			err = GCPTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
			c.Assert(len(gcpDummyProcessor.record), chk.Equals, 1)

			c.Assert(localDummyProcessor.record[0].relativePath, chk.Equals, gcpDummyProcessor.record[0].relativePath)
			c.Assert(localDummyProcessor.record[0].name, chk.Equals, gcpDummyProcessor.record[0].name)
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
	gcpClient, err := createGCPClientWithGCSSDK()
	gcpEnabled := err == nil && !gcpTestsDisabled()
	var bucketName string
	var bucketNameGCP string
	if s3Enabled {
		bucketName = createNewBucket(c, s3Client, createS3ResOptions{})
		defer deleteBucket(c, s3Client, bucketName, true)
	}
	if gcpEnabled {
		bucketNameGCP = createNewGCPBucket(c, gcpClient)
		defer deleteGCPBucket(c, gcpClient, bucketNameGCP, true)
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
	if gcpEnabled {
		scenarioHelper{}.generateGCPObjects(c, gcpClient, bucketNameGCP, fileList)
	}

	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(nil, dstDirName, isRecursiveOn, false, func(common.EntityType) {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, containerName)
		blobTraverser := newBlobTraverser(&rawContainerURLWithSAS, p, ctx, isRecursiveOn, false,
			func(common.EntityType) {}, false, common.CpkOptions{})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct an Azure File traverser
		filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
		rawFileURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(c, shareName)
		azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, isRecursiveOn, false, func(common.EntityType) {})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct a directory URL and pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func(common.EntityType) {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.Traverse(noPreProccessor, bfsDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		s3DummyProcessor := dummyProcessor{}
		gcpDummyProcessor := dummyProcessor{}
		if s3Enabled {
			// construct and run a S3 traverser
			rawS3URL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName)
			credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
			S3Traverser, err := newS3Traverser(credentialInfo.CredentialType, &rawS3URL, ctx, isRecursiveOn, false, func(common.EntityType) {})
			c.Assert(err, chk.IsNil)
			err = S3Traverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
		}
		if gcpEnabled {
			rawGCPURL := scenarioHelper{}.getRawGCPBucketURL(c, bucketNameGCP)
			GCPTraverser, err := newGCPTraverser(&rawGCPURL, ctx, isRecursiveOn, false, func(entityType common.EntityType) {})
			c.Assert(err, chk.IsNil)
			err = GCPTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)
		}

		// make sure the results are as expected
		localTotalCount := len(localIndexer.indexMap)
		localFileOnlyCount := 0
		for _, x := range localIndexer.indexMap {
			if x.entityType == common.EEntityType.File() {
				localFileOnlyCount++
			}
		}

		c.Assert(len(blobDummyProcessor.record), chk.Equals, localFileOnlyCount)
		if isRecursiveOn {
			c.Assert(len(fileDummyProcessor.record), chk.Equals, localTotalCount)
			c.Assert(len(bfsDummyProcessor.record), chk.Equals, localTotalCount)
		} else {
			// in real usage, folders get stripped out in ToNewCopyTransfer when non-recursive,
			// but that doesn't run here in this test,
			// so we have to count files only on the processor
			c.Assert(fileDummyProcessor.countFilesOnly(), chk.Equals, localTotalCount)
			c.Assert(bfsDummyProcessor.countFilesOnly(), chk.Equals, localTotalCount)
		}

		if s3Enabled {
			c.Assert(len(s3DummyProcessor.record), chk.Equals, localFileOnlyCount)
		}
		if gcpEnabled {
			c.Assert(len(gcpDummyProcessor.record), chk.Equals, localFileOnlyCount)
		}

		// if s3dummyprocessor is empty, it's A-OK because no records will be tested
		for _, storedObject := range append(append(append(append(blobDummyProcessor.record, fileDummyProcessor.record...), bfsDummyProcessor.record...), s3DummyProcessor.record...), gcpDummyProcessor.record...) {
			if isRecursiveOn || storedObject.entityType == common.EEntityType.File() { // folder enumeration knowingly NOT consistent when non-recursive (since the folders get stripped out by ToNewCopyTransfer when non-recursive anyway)
				correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

				c.Assert(present, chk.Equals, true)
				c.Assert(correspondingLocalFile.name, chk.Equals, storedObject.name)

				if !isRecursiveOn {
					c.Assert(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING), chk.Equals, false)
				}
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
	gcpClient, err := createGCPClientWithGCSSDK()
	gcpEnabled := err == nil && !gcpTestsDisabled()
	var bucketName, bucketNameGCP string
	if s3Enabled {
		bucketName = createNewBucket(c, s3Client, createS3ResOptions{})
		defer deleteBucket(c, s3Client, bucketName, true)
	}
	if gcpEnabled {
		bucketNameGCP = createNewGCPBucket(c, gcpClient)
		defer deleteGCPBucket(c, gcpClient, bucketNameGCP, true)
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
	if gcpEnabled {
		scenarioHelper{}.generateGCPObjects(c, gcpClient, bucketNameGCP, fileList)
	}

	time.Sleep(time.Second * 2) // Ensure the objects' LMTs are in the past

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(c)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(c, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser := newLocalTraverser(nil, filepath.Join(dstDirName, virDirName), isRecursiveOn, false, func(common.EntityType) {})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
		c.Assert(err, chk.IsNil)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawVirDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, virDirName)
		blobTraverser := newBlobTraverser(&rawVirDirURLWithSAS, p, ctx, isRecursiveOn, false,
			func(common.EntityType) {}, false, common.CpkOptions{})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct an Azure File traverser
		filePipeline := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(c, shareName, virDirName)
		azureFileTraverser := newFileTraverser(&rawFileURLWithSAS, filePipeline, ctx, isRecursiveOn, false, func(common.EntityType) {})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// construct a filesystem URL & pipeline
		accountName, accountKey := getAccountAndKey()
		bfsPipeline := azbfs.NewPipeline(azbfs.NewSharedKeyCredential(accountName, accountKey), azbfs.PipelineOptions{})
		rawFilesystemURL := filesystemURL.NewRootDirectoryURL().NewDirectoryURL(virDirName).URL()

		// construct and run a FS traverser
		bfsTraverser := newBlobFSTraverser(&rawFilesystemURL, bfsPipeline, ctx, isRecursiveOn, func(common.EntityType) {})
		bfsDummyProcessor := dummyProcessor{}
		err = bfsTraverser.Traverse(noPreProccessor, bfsDummyProcessor.process, nil)

		localTotalCount := len(localIndexer.indexMap)
		localFileOnlyCount := 0
		for _, x := range localIndexer.indexMap {
			if x.entityType == common.EEntityType.File() {
				localFileOnlyCount++
			}
		}

		s3DummyProcessor := dummyProcessor{}
		gcpDummyProcessor := dummyProcessor{}
		if s3Enabled {
			// construct and run a S3 traverser
			// directory object keys always end with / in S3
			rawS3URL := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, virDirName+"/")
			credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
			S3Traverser, err := newS3Traverser(credentialInfo.CredentialType, &rawS3URL, ctx, isRecursiveOn, false, func(common.EntityType) {})
			c.Assert(err, chk.IsNil)
			err = S3Traverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)

			// check that the results are the same length
			c.Assert(len(s3DummyProcessor.record), chk.Equals, localFileOnlyCount)
		}
		if gcpEnabled {
			rawGCPURL := scenarioHelper{}.getRawGCPObjectURL(c, bucketNameGCP, virDirName+"/")
			GCPTraverser, err := newGCPTraverser(&rawGCPURL, ctx, isRecursiveOn, false, func(common.EntityType) {})
			c.Assert(err, chk.IsNil)
			err = GCPTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
			c.Assert(err, chk.IsNil)

			c.Assert(len(gcpDummyProcessor.record), chk.Equals, localFileOnlyCount)
		}

		// make sure the results are as expected
		c.Assert(len(blobDummyProcessor.record), chk.Equals, localFileOnlyCount)
		if isRecursiveOn {
			c.Assert(len(fileDummyProcessor.record), chk.Equals, localTotalCount)
			c.Assert(len(bfsDummyProcessor.record), chk.Equals, localTotalCount)
		} else {
			// only files matter when not recursive (since ToNewCopyTransfer strips out everything else when non-recursive)
			c.Assert(fileDummyProcessor.countFilesOnly(), chk.Equals, localTotalCount)
			c.Assert(bfsDummyProcessor.countFilesOnly(), chk.Equals, localTotalCount)
		}
		// if s3 testing is disabled the s3 dummy processors' records will be empty. This is OK for appending. Nothing will happen.
		for _, storedObject := range append(append(append(append(blobDummyProcessor.record, fileDummyProcessor.record...), bfsDummyProcessor.record...), s3DummyProcessor.record...), gcpDummyProcessor.record...) {
			if isRecursiveOn || storedObject.entityType == common.EEntityType.File() { // folder enumeration knowingly NOT consistent when non-recursive (since the folders get stripped out by ToNewCopyTransfer when non-recursive anyway)

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
}

// validate traversing a virtual directory containing the same objects
// compare that the serial and parallel blob traversers get consistent results
func (s *genericTraverserSuite) TestSerialAndParallelBlobTraverser(c *chk.C) {
	bsu := getBSU()
	containerURL, containerName := createNewContainer(c, bsu)
	defer deleteContainer(c, containerURL)

	// set up the container with numerous blobs
	virDirName := "virdir"
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(c, containerURL, virDirName+"/")
	c.Assert(containerURL, chk.NotNil)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a parallel blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		rawVirDirURLWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, containerName, virDirName)
		parallelBlobTraverser := newBlobTraverser(&rawVirDirURLWithSAS, p, ctx, isRecursiveOn, false,
			func(common.EntityType) {}, false, common.CpkOptions{})

		// construct a serial blob traverser
		serialBlobTraverser := newBlobTraverser(&rawVirDirURLWithSAS, p, ctx, isRecursiveOn, false,
			func(common.EntityType) {}, false, common.CpkOptions{})
		serialBlobTraverser.parallelListing = false

		// invoke the parallel traversal with a dummy processor
		parallelDummyProcessor := dummyProcessor{}
		err := parallelBlobTraverser.Traverse(noPreProccessor, parallelDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// invoke the serial traversal with a dummy processor
		serialDummyProcessor := dummyProcessor{}
		err = parallelBlobTraverser.Traverse(noPreProccessor, serialDummyProcessor.process, nil)
		c.Assert(err, chk.IsNil)

		// make sure the results are as expected
		c.Assert(len(parallelDummyProcessor.record), chk.Equals, len(serialDummyProcessor.record))

		// compare the entries one by one
		lookupMap := make(map[string]StoredObject)
		for _, entry := range parallelDummyProcessor.record {
			lookupMap[entry.relativePath] = entry
		}

		for _, storedObject := range serialDummyProcessor.record {
			correspondingFile, present := lookupMap[storedObject.relativePath]
			c.Assert(present, chk.Equals, true)
			c.Assert(storedObject.lastModifiedTime, chk.DeepEquals, correspondingFile.lastModifiedTime)
			c.Assert(storedObject.md5, chk.DeepEquals, correspondingFile.md5)
		}
	}
}
