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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/stretchr/testify/assert"

	gcpUtils "cloud.google.com/go/storage"

	"github.com/minio/minio-go"
	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type genericTraverserSuite struct{}

var _ = chk.Suite(&genericTraverserSuite{})

// On Windows, if you don't hold adequate permissions to create a symlink, tests regarding symlinks will fail.
// This is arguably annoying to dig through, therefore, we cleanly skip the test.
func trySymlink(src, dst string, t *testing.T) {
	if err := os.Symlink(src, dst); err != nil {
		if strings.Contains(err.Error(), "A required privilege is not held by the client") {
			t.Skip("client lacks required privilege to create symlinks; symlinks will not be tested")
		}
		t.Error(err)
	}
}

func TestLocalWildcardOverlap(t *testing.T) {
	a := assert.New(t)
	if runtime.GOOS == "windows" {
		t.Skip("invalid filename used")
		return
	}

	/*
		Wildcard support is not actually a part of the local traverser, believe it or not.
		It's instead implemented in InitResourceTraverser as a short-circuit to a list traverser
		utilizing the filepath.Glob function, which then initializes local traversers to achieve the same effect.
	*/
	tmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer func(path string) { _ = os.RemoveAll(path) }(tmpDir)

	scenarioHelper{}.generateLocalFilesFromList(a, tmpDir, []string{
		"test.txt",
		"tes*t.txt",
		"foobarbaz/test.txt",
	})

	resource, err := SplitResourceString(filepath.Join(tmpDir, "tes*t.txt"), common.ELocation.Local())
	a.Nil(err)

	traverser, err := InitResourceTraverser(resource, common.ELocation.Local(), ctx, InitResourceTraverserOptions{
		SymlinkHandling:   common.ESymlinkHandlingType.Follow(),
		TrailingDotOption: common.ETrailingDotOption.Enable(),
		Recursive:         true,
		StripTopDir:       true,
		HardlinkHandling:  common.EPreserveHardlinksOption.Follow(),
	})
	a.Nil(err)

	seenFiles := make(map[string]bool)

	err = traverser.Traverse(nil, func(storedObject StoredObject) error {
		seenFiles[storedObject.relativePath] = true
		return nil
	}, []ObjectFilter{})
	a.Nil(err)

	a.Equal(map[string]bool{
		"test.txt":  true,
		"tes*t.txt": true,
	}, seenFiles)
}

// GetProperties tests.
// GetProperties does not exist on Blob, as the properties come in the list call.
// While BlobFS could get properties in the future, it's currently disabled as BFS source S2S isn't set up right now, and likely won't be.
func TestFilesGetProperties(t *testing.T) {
	a := assert.New(t)
	fsc := getFileServiceClient()
	sc, shareName := createNewShare(a, fsc)
	fileName := generateAzureFileName()

	headers := file.HTTPHeaders{
		ContentType:        to.Ptr("text/random"),
		ContentEncoding:    to.Ptr("testEncoding"),
		ContentLanguage:    to.Ptr("en-US"),
		ContentDisposition: to.Ptr("testDisposition"),
		CacheControl:       to.Ptr("testCacheControl"),
	}

	scenarioHelper{}.generateShareFilesFromList(a, sc, fsc, []string{fileName})
	_, err := sc.NewRootDirectoryClient().NewFileClient(fileName).SetHTTPHeaders(ctx, &file.SetHTTPHeadersOptions{HTTPHeaders: &headers})
	a.Nil(err)
	shareURL := scenarioHelper{}.getRawShareURLWithSAS(a, shareName).String()

	serviceClientWithSAS := scenarioHelper{}.getFileServiceClientWithSASFromURL(a, shareURL)
	// first test reading from the share itself
	traverser := newFileTraverser(shareURL, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
		GetPropertiesInFrontend: true,
		TrailingDotOption:       common.ETrailingDotOption.Enable(),
		HardlinkHandling:        common.EPreserveHardlinksOption.Follow(),
	})

	// embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object StoredObject) error {
		if object.entityType == common.EEntityType.File() {
			// test all attributes (but only for files, since folders don't have them)
			a.Equal(*headers.ContentType, object.contentType)
			a.Equal(*headers.ContentEncoding, object.contentEncoding)
			a.Equal(*headers.ContentLanguage, object.contentLanguage)
			a.Equal(*headers.ContentDisposition, object.contentDisposition)
			a.Equal(*headers.CacheControl, object.cacheControl)
			seenContentType = true
		}
		return nil
	}

	err = traverser.Traverse(noPreProccessor, processor, nil)
	a.Nil(err)
	a.True(seenContentType)

	// then test reading from the filename exactly, because that's a different codepath.
	seenContentType = false
	fileURL := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, fileName).String()
	serviceClientWithSAS = scenarioHelper{}.getFileServiceClientWithSASFromURL(a, shareURL)
	traverser = newFileTraverser(fileURL, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
		GetPropertiesInFrontend: true,
		TrailingDotOption:       common.ETrailingDotOption.Enable(),
		HardlinkHandling:        common.EPreserveHardlinksOption.Follow(),
	})

	err = traverser.Traverse(noPreProccessor, processor, nil)
	a.Nil(err)
	a.True(seenContentType)
}

func TestS3GetProperties(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	client, err := createS3ClientWithMinio(createS3ResOptions{})

	if err != nil {
		// TODO: Alter all tests that use S3 credentials to just skip instead of failing
		//       This is useful for local testing, when we don't want to have to sift through errors related to S3 clients not being created
		//       Just so that we can test locally without interrupting CI.
		t.Skip("S3-based tests will not be ran as no credentials were supplied.")
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
	defer deleteBucket(client, bucketName, false)
	a.Nil(err)

	_, err = client.PutObjectWithContext(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), int64(len(objectDefaultData)), headers)
	a.Nil(err)

	// First test against the bucket
	s3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName)

	credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
	traverser, err := newS3Traverser(&s3BucketURL, ctx, InitResourceTraverserOptions{
		Credential:              &credentialInfo,
		GetPropertiesInFrontend: true,
	})
	a.Nil(err)

	// Embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object StoredObject) error {
		// test all attributes
		a.Equal(headers.ContentType, object.contentType)
		a.Equal(headers.ContentEncoding, object.contentEncoding)
		a.Equal(headers.ContentLanguage, object.contentLanguage)
		a.Equal(headers.ContentDisposition, object.contentDisposition)
		a.Equal(headers.CacheControl, object.cacheControl)
		seenContentType = true
		return nil
	}

	err = traverser.Traverse(noPreProccessor, processor, nil)
	a.Nil(err)
	a.True(seenContentType)

	// Then, test against the object itself because that's a different codepath.
	seenContentType = false
	s3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(a, "", bucketName, objectName)
	credentialInfo = common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}

	traverser, err = newS3Traverser(&s3ObjectURL, ctx, InitResourceTraverserOptions{
		Credential:              &credentialInfo,
		GetPropertiesInFrontend: true,
	})
	a.Nil(err)

	err = traverser.Traverse(noPreProccessor, processor, nil)
	a.Nil(err)
	a.True(seenContentType)
}

func TestGCPGetProperties(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)
	client, err := createGCPClientWithGCSSDK()

	if err != nil {
		t.Skip("GCP-based tests will not be run as no credentials were supplied.")
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
	defer deleteGCPBucket(client, bucketName, false)
	a.Nil(err)

	reader := strings.NewReader(objectDefaultData)
	obj := bkt.Object(objectName)
	wc := obj.NewWriter(ctx)
	n, err := io.Copy(wc, reader)
	a.Nil(err)
	a.Equal(int64(len(objectDefaultData)), n)
	err = wc.Close()
	a.Nil(err)
	_, err = obj.Update(ctx, headers)
	a.Nil(err)

	// First test against the bucket
	gcpBucketURL := scenarioHelper{}.getRawGCPBucketURL(a, bucketName)
	traverser, err := newGCPTraverser(&gcpBucketURL, ctx, InitResourceTraverserOptions{
		GetPropertiesInFrontend: true,
	})
	a.Nil(err)

	// Embed the check into the processor for ease of use
	seenContentType := false
	processor := func(object StoredObject) error {
		// test all attributes
		a.Equal(headers.ContentType, object.contentType)
		a.Equal(headers.ContentEncoding, object.contentEncoding)
		a.Equal(headers.ContentLanguage, object.contentLanguage)
		a.Equal(headers.ContentDisposition, object.contentDisposition)
		a.Equal(headers.CacheControl, object.cacheControl)
		seenContentType = true
		return nil
	}

	err = traverser.Traverse(noPreProccessor, processor, nil)
	a.Nil(err)
	a.True(seenContentType)

	// Then, test against the object itself because that's a different codepath.
	seenContentType = false
	gcpObjectURL := scenarioHelper{}.getRawGCPObjectURL(a, bucketName, objectName)

	traverser, err = newGCPTraverser(&gcpObjectURL, ctx, InitResourceTraverserOptions{
		GetPropertiesInFrontend: true,
	})
	a.Nil(err)

	err = traverser.Traverse(noPreProccessor, processor, nil)
	a.Nil(err)
	a.True(seenContentType)
}

// Test follow symlink functionality
func TestWalkWithSymlinks_ToFolder(t *testing.T) {
	a := assert.New(t)
	fileNames := []string{"March 20th is international happiness day.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(symlinkTmpDir)
	a.NotEqual(symlinkTmpDir, tmpDir)

	scenarioHelper{}.generateLocalFilesFromList(a, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(a, symlinkTmpDir, fileNames)
	dirLinkName := "so long and thanks for all the fish"
	time.Sleep(2 * time.Second) // to be sure to get different LMT for link, compared to root, so we can make assertions later about whose fileInfo we get
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, dirLinkName), t)

	fileCount := 0
	sawLinkTargetDir := false
	a.Nil(WalkWithSymlinks(context.TODO(), tmpDir, func(path string, fi os.FileInfo, err error) error {
		a.Nil(err)

		if fi.IsDir() {
			if fi.Name() == dirLinkName {
				sawLinkTargetDir = true
				s, _ := os.Stat(symlinkTmpDir)
				a.Equal(s.ModTime().UTC(), fi.ModTime().UTC())
			}
			return nil
		}

		fileCount++
		return nil
	},
		common.ESymlinkHandlingType.Follow(), nil, common.EPreserveHardlinksOption.Follow()))

	// 3 files live in base, 3 files live in symlink
	a.Equal(6, fileCount)
	a.True(sawLinkTargetDir)
}

// Next test is temporarily disabled, to avoid changing functionality near 10.4 release date
/*
// symlinks are not just to folders. They may be to individual files
func TestWalkWithSymlinks_ToFile(t *testing.T) {
	a := assert.New(t)
	mainDirFilenames := []string{"iAmANormalFile.txt"}
	symlinkTargetFilenames := []string{"iAmASymlinkTargetFile.txt"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(symlinkTmpDir)
	c.Assert(tmpDir, chk.Not(chk.Equals), symlinkTmpDir)
	scenarioHelper{}.generateLocalFilesFromList(a, tmpDir, mainDirFilenames)
	scenarioHelper{}.generateLocalFilesFromList(a, symlinkTmpDir, symlinkTargetFilenames)
	trySymlink(filepath.Join(symlinkTmpDir, symlinkTargetFilenames[0]), filepath.Join(tmpDir, "iPointToTheSymlink"), c)
	trySymlink(filepath.Join(symlinkTmpDir, symlinkTargetFilenames[0]), filepath.Join(tmpDir, "iPointToTheSameSymlink"), c)
	fileCount := 0
	c.Assert(WalkWithSymlinks(tmpDir, func(path string, fi os.FileInfo, err error) error {
		a.Nil(err)
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
func TestWalkWithSymlinksBreakLoop(t *testing.T) {
	a := assert.New(t)
	fileNames := []string{"stonks.txt", "jaws but its a baby shark.mp3", "my crow soft.txt"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(tmpDir)

	scenarioHelper{}.generateLocalFilesFromList(a, tmpDir, fileNames)
	trySymlink(tmpDir, filepath.Join(tmpDir, "spinloop"), t)

	// Only 3 files should ever be found.
	// This is because the symlink links back to the root dir
	fileCount := 0
	a.Nil(WalkWithSymlinks(context.TODO(), tmpDir, func(path string, fi os.FileInfo, err error) error {
		a.Nil(err)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		common.ESymlinkHandlingType.Follow(), nil, common.EPreserveHardlinksOption.Follow()))

	a.Equal(3, fileCount)
}

// Test ability to dedupe within the same directory
func TestWalkWithSymlinksDedupe(t *testing.T) {
	a := assert.New(t)
	fileNames := []string{"stonks.txt", "jaws but its a baby shark.mp3", "my crow soft.txt"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir, err := os.MkdirTemp(tmpDir, "subdir")
	a.Nil(err)

	scenarioHelper{}.generateLocalFilesFromList(a, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(a, symlinkTmpDir, fileNames)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "symlinkdir"), t)

	// Only 6 files should ever be found.
	// 3 in the root dir, 3 in subdir, then symlinkdir should be ignored because it's been seen.
	fileCount := 0
	a.Nil(WalkWithSymlinks(context.TODO(), tmpDir, func(path string, fi os.FileInfo, err error) error {
		a.Nil(err)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		common.ESymlinkHandlingType.Follow(), nil, common.EPreserveHardlinksOption.Follow()))

	a.Equal(6, fileCount)
}

// Test ability to only get the output of one symlink when two point to the same place
func TestWalkWithSymlinksMultitarget(t *testing.T) {
	a := assert.New(t)
	fileNames := []string{"March 20th is international happiness day.txt", "wonderwall but it goes on and on and on.mp3", "bonzi buddy.exe"}
	tmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(tmpDir)
	symlinkTmpDir := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(symlinkTmpDir)
	a.NotEqual(symlinkTmpDir, tmpDir)

	scenarioHelper{}.generateLocalFilesFromList(a, tmpDir, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(a, symlinkTmpDir, fileNames)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "so long and thanks for all the fish"), t)
	trySymlink(symlinkTmpDir, filepath.Join(tmpDir, "extradir"), t)
	trySymlink(filepath.Join(tmpDir, "extradir"), filepath.Join(tmpDir, "linktolink"), t)

	fileCount := 0
	a.Nil(WalkWithSymlinks(context.TODO(), tmpDir, func(path string, fi os.FileInfo, err error) error {
		a.Nil(err)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		common.ESymlinkHandlingType.Follow(), nil, common.EPreserveHardlinksOption.Follow()))

	// 3 files live in base, 3 files live in first symlink, second & third symlink is ignored.
	a.Equal(6, fileCount)
}

func TestWalkWithSymlinksToParentAndChild(t *testing.T) {
	a := assert.New(t)
	fileNames := []string{"file1.txt", "file2.txt", "file3.txt"}

	root1 := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(root1)
	root2 := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(root2)

	child, err := os.MkdirTemp(root2, "childdir")
	a.Nil(err)

	scenarioHelper{}.generateLocalFilesFromList(a, root2, fileNames)
	scenarioHelper{}.generateLocalFilesFromList(a, child, fileNames)
	trySymlink(root2, filepath.Join(root1, "toroot"), t)
	trySymlink(child, filepath.Join(root1, "tochild"), t)

	fileCount := 0
	a.Nil(WalkWithSymlinks(context.TODO(), root1, func(path string, fi os.FileInfo, err error) error {
		a.Nil(err)

		if fi.IsDir() {
			return nil
		}

		fileCount++
		return nil
	},
		common.ESymlinkHandlingType.Follow(), nil, common.EPreserveHardlinksOption.Follow()))

	// 6 files total live under toroot. tochild should be ignored (or if tochild was traversed first, child will be ignored on toroot).
	a.Equal(6, fileCount)
}

// validate traversing a single Blob, a single Azure File, and a single local file
// compare that the traversers get consistent results
func TestTraverserWithSingleObject(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	fsc := getFileServiceClient()
	sc, shareName := createNewShare(a, fsc)
	defer deleteShare(a, sc)

	bfsu := getDatalakeServiceClient()
	filesystemURL, _ := createNewFilesystem(a, bfsu)
	defer deleteFilesystem(a, filesystemURL)

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	s3Enabled := err == nil && !isS3Disabled()
	gcpClient, err := createGCPClientWithGCSSDK()
	gcpEnabled := err == nil && !gcpTestsDisabled()
	var bucketName string
	var bucketNameGCP string
	if s3Enabled {
		bucketName = createNewBucket(a, s3Client, createS3ResOptions{})
		defer deleteBucket(s3Client, bucketName, true)
	}
	if gcpEnabled {
		bucketNameGCP = createNewGCPBucket(a, gcpClient)
		defer deleteGCPBucket(gcpClient, bucketNameGCP, true)
	}

	// test two scenarios, either blob is at the root virtual dir, or inside sub virtual dirs
	for _, storedObjectName := range []string{"sub1/sub2/singleblobisbest", "nosubsingleblob", "满汉全席.txt"} {
		// set up the container with a single blob
		blobList := []string{storedObjectName}
		scenarioHelper{}.generateBlobsFromList(a, cc, blobList, blockBlobDefaultData)

		// set up the directory as a single file
		dstDirName := scenarioHelper{}.generateLocalDirectory(a)
		defer os.RemoveAll(dstDirName)
		dstFileName := storedObjectName
		scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, blobList)

		// construct a local traverser
		localTraverser, _ := newLocalTraverser(filepath.Join(dstDirName, dstFileName), ctx, InitResourceTraverserOptions{
			SymlinkHandling:  common.ESymlinkHandlingType.Follow(),
			HardlinkHandling: common.EPreserveHardlinksOption.Follow(),
		})

		// invoke the local traversal with a dummy processor
		localDummyProcessor := dummyProcessor{}
		err := localTraverser.Traverse(noPreProccessor, localDummyProcessor.process, nil)
		a.Nil(err)
		a.Equal(1, len(localDummyProcessor.record))

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		rawBlobURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, blobList[0]).URL()
		blobServiceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawBlobURLWithSAS)
		blobTraverser := newBlobTraverser(rawBlobURLWithSAS, blobServiceClientWithSAS, ctx, InitResourceTraverserOptions{})

		// invoke the blob traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
		a.Nil(err)
		a.Equal(1, len(blobDummyProcessor.record))

		// assert the important info are correct
		a.Equal(localDummyProcessor.record[0].name, blobDummyProcessor.record[0].name)
		a.Equal(localDummyProcessor.record[0].relativePath, blobDummyProcessor.record[0].relativePath)

		// Azure File cannot handle names with '/' in them
		// TODO: Construct a directory URL and then build a file URL atop it in order to solve this portion of the test.
		//  We shouldn't be excluding things the traverser is actually capable of doing.
		//  Fix within scenarioHelper.generateAzureFilesFromList, since that's what causes the fail.
		if !strings.Contains(storedObjectName, "/") {
			// set up the Azure Share with a single file
			fileList := []string{storedObjectName}
			scenarioHelper{}.generateShareFilesFromList(a, sc, fsc, fileList)

			// construct an Azure file traverser
			rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, fileList[0]).String()
			fileServiceClientWithSAS := scenarioHelper{}.getFileServiceClientWithSASFromURL(a, rawFileURLWithSAS)
			azureFileTraverser := newFileTraverser(rawFileURLWithSAS, fileServiceClientWithSAS, ctx, InitResourceTraverserOptions{})

			// invoke the file traversal with a dummy processor
			fileDummyProcessor := dummyProcessor{}
			err = azureFileTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
			a.Nil(err)
			a.Equal(1, len(fileDummyProcessor.record))

			a.Equal(localDummyProcessor.record[0].relativePath, fileDummyProcessor.record[0].relativePath)
			a.Equal(localDummyProcessor.record[0].name, fileDummyProcessor.record[0].name)
		}

		if s3Enabled {
			// set up the bucket with a single file
			s3List := []string{storedObjectName}
			scenarioHelper{}.generateObjects(a, s3Client, bucketName, s3List)

			// construct a s3 traverser
			s3DummyProcessor := dummyProcessor{}
			url := scenarioHelper{}.getRawS3ObjectURL(a, "", bucketName, storedObjectName)
			credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
			S3Traverser, err := newS3Traverser(&url, ctx, InitResourceTraverserOptions{
				Credential: &credentialInfo,
			})
			a.Nil(err)

			err = S3Traverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
			a.Nil(err)
			a.Equal(1, len(s3DummyProcessor.record))

			a.Equal(localDummyProcessor.record[0].relativePath, s3DummyProcessor.record[0].relativePath)
			a.Equal(localDummyProcessor.record[0].name, s3DummyProcessor.record[0].name)
		}
		if gcpEnabled {
			gcpList := []string{storedObjectName}
			scenarioHelper{}.generateGCPObjects(a, gcpClient, bucketNameGCP, gcpList)

			gcpDummyProcessor := dummyProcessor{}
			gcpURL := scenarioHelper{}.getRawGCPObjectURL(a, bucketNameGCP, storedObjectName)
			GCPTraverser, err := newGCPTraverser(&gcpURL, ctx, InitResourceTraverserOptions{})
			a.Nil(err)

			err = GCPTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
			a.Nil(err)
			a.Equal(1, len(gcpDummyProcessor.record))

			a.Equal(localDummyProcessor.record[0].relativePath, gcpDummyProcessor.record[0].relativePath)
			a.Equal(localDummyProcessor.record[0].name, gcpDummyProcessor.record[0].name)
		}
	}
}

// validate traversing a container, a share, and a local directory containing the same objects
// compare that traversers get consistent results
func TestTraverserContainerAndLocalDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	fsc := getFileServiceClient()
	sc, shareName := createNewShare(a, fsc)
	defer deleteShare(a, sc)

	bfsu := getDatalakeServiceClient()
	filesystemURL, _ := createNewFilesystem(a, bfsu)
	defer deleteFilesystem(a, filesystemURL)

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	s3Enabled := err == nil && !isS3Disabled() // are creds supplied, and is S3 enabled
	gcpClient, err := createGCPClientWithGCSSDK()
	gcpEnabled := err == nil && !gcpTestsDisabled()
	var bucketName string
	var bucketNameGCP string
	if s3Enabled {
		bucketName = createNewBucket(a, s3Client, createS3ResOptions{})
		defer deleteBucket(s3Client, bucketName, true)
	}
	if gcpEnabled {
		bucketNameGCP = createNewGCPBucket(a, gcpClient)
		defer deleteGCPBucket(gcpClient, bucketNameGCP, true)
	}

	// set up the container with numerous blobs
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, "")
	a.NotNil(cc)

	// set up an Azure File Share with the same files
	scenarioHelper{}.generateShareFilesFromList(a, sc, fsc, fileList)

	// set up a filesystem with the same files
	scenarioHelper{}.generateBFSPathsFromList(a, filesystemURL, fileList)

	if s3Enabled {
		// set up a bucket with the same files
		scenarioHelper{}.generateObjects(a, s3Client, bucketName, fileList)
	}
	if gcpEnabled {
		scenarioHelper{}.generateGCPObjects(a, gcpClient, bucketNameGCP, fileList)
	}

	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser, _ := newLocalTraverser(dstDirName, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
		a.Nil(err)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		rawContainerURLWithSAS := scenarioHelper{}.getContainerClientWithSAS(a, containerName).URL()
		blobServiceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawContainerURLWithSAS)
		blobTraverser := newBlobTraverser(rawContainerURLWithSAS, blobServiceClientWithSAS, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
		a.Nil(err)

		// construct an Azure File traverser
		rawShareURLWithSAS := scenarioHelper{}.getRawShareURLWithSAS(a, shareName).String()
		fileServiceClientWithSAS := scenarioHelper{}.getFileServiceClientWithSASFromURL(a, rawShareURLWithSAS)
		azureFileTraverser := newFileTraverser(rawShareURLWithSAS, fileServiceClientWithSAS, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
		a.Nil(err)

		s3DummyProcessor := dummyProcessor{}
		gcpDummyProcessor := dummyProcessor{}
		if s3Enabled {
			// construct and run a S3 traverser
			rawS3URL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName)
			credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
			S3Traverser, err := newS3Traverser(&rawS3URL, ctx, InitResourceTraverserOptions{
				Credential: &credentialInfo,
				Recursive:  isRecursiveOn,
			})
			a.Nil(err)
			err = S3Traverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
			a.Nil(err)
		}
		if gcpEnabled {
			rawGCPURL := scenarioHelper{}.getRawGCPBucketURL(a, bucketNameGCP)
			GCPTraverser, err := newGCPTraverser(&rawGCPURL, ctx, InitResourceTraverserOptions{
				Recursive: isRecursiveOn,
			})
			a.Nil(err)
			err = GCPTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
			a.Nil(err)
		}

		// make sure the results are as expected
		localTotalCount := len(localIndexer.indexMap)
		localFileOnlyCount := 0
		for _, x := range localIndexer.indexMap {
			if x.entityType == common.EEntityType.File() {
				localFileOnlyCount++
			}
		}

		a.Equal(localFileOnlyCount, len(blobDummyProcessor.record))
		if isRecursiveOn {
			a.Equal(localTotalCount, len(fileDummyProcessor.record))
		} else {
			// in real usage, folders get stripped out in ToNewCopyTransfer when non-recursive,
			// but that doesn't run here in this test,
			// so we have to count files only on the processor
			a.Equal(localTotalCount, fileDummyProcessor.countFilesOnly())
		}

		if s3Enabled {
			a.Equal(localFileOnlyCount, len(s3DummyProcessor.record))
		}
		if gcpEnabled {
			a.Equal(localFileOnlyCount, len(gcpDummyProcessor.record))
		}

		// if s3dummyprocessor is empty, it's A-OK because no records will be tested
		for _, storedObject := range append(append(append(blobDummyProcessor.record, fileDummyProcessor.record...), s3DummyProcessor.record...), gcpDummyProcessor.record...) {
			if isRecursiveOn || storedObject.entityType == common.EEntityType.File() { // folder enumeration knowingly NOT consistent when non-recursive (since the folders get stripped out by ToNewCopyTransfer when non-recursive anyway)
				correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

				a.True(present)
				a.Equal(storedObject.name, correspondingLocalFile.name)

				if !isRecursiveOn {
					a.False(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING))
				}
			}
		}
	}
}

// validate traversing a virtual and a local directory containing the same objects
// compare that blob and local traversers get consistent results
func TestTraverserWithVirtualAndLocalDirectory(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	fsc := getFileServiceClient()
	sc, shareName := createNewShare(a, fsc)
	defer deleteShare(a, sc)

	bfsu := getDatalakeServiceClient()
	filesystemURL, _ := createNewFilesystem(a, bfsu)
	defer deleteFilesystem(a, filesystemURL)

	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	s3Enabled := err == nil && !isS3Disabled()
	gcpClient, err := createGCPClientWithGCSSDK()
	gcpEnabled := err == nil && !gcpTestsDisabled()
	var bucketName, bucketNameGCP string
	if s3Enabled {
		bucketName = createNewBucket(a, s3Client, createS3ResOptions{})
		defer deleteBucket(s3Client, bucketName, true)
	}
	if gcpEnabled {
		bucketNameGCP = createNewGCPBucket(a, gcpClient)
		defer deleteGCPBucket(gcpClient, bucketNameGCP, true)
	}

	// set up the container with numerous blobs
	virDirName := "virdir"
	fileList := scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, virDirName+"/")
	a.NotNil(cc)

	// set up an Azure File Share with the same files
	scenarioHelper{}.generateShareFilesFromList(a, sc, fsc, fileList)

	// set up the filesystem with the same files
	scenarioHelper{}.generateBFSPathsFromList(a, filesystemURL, fileList)

	if s3Enabled {
		// Set up the bucket with the same files
		scenarioHelper{}.generateObjects(a, s3Client, bucketName, fileList)
	}
	if gcpEnabled {
		scenarioHelper{}.generateGCPObjects(a, gcpClient, bucketNameGCP, fileList)
	}

	time.Sleep(time.Second * 2) // Ensure the objects' LMTs are in the past

	// set up the destination with a folder that have the exact same files
	dstDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(dstDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, dstDirName, fileList)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a local traverser
		localTraverser, _ := newLocalTraverser(filepath.Join(dstDirName, virDirName), ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// invoke the local traversal with an indexer
		// so that the results are indexed for easy validation
		localIndexer := newObjectIndexer()
		err := localTraverser.Traverse(noPreProccessor, localIndexer.store, nil)
		a.Nil(err)

		// construct a blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		rawVirDirURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, virDirName).URL()
		serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawVirDirURLWithSAS)
		blobTraverser := newBlobTraverser(rawVirDirURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// invoke the local traversal with a dummy processor
		blobDummyProcessor := dummyProcessor{}
		err = blobTraverser.Traverse(noPreProccessor, blobDummyProcessor.process, nil)
		a.Nil(err)

		// construct an Azure File traverser
		rawFileURLWithSAS := scenarioHelper{}.getRawFileURLWithSAS(a, shareName, virDirName).String()
		fileServiceClientWithSAS := scenarioHelper{}.getFileServiceClientWithSASFromURL(a, rawFileURLWithSAS)
		azureFileTraverser := newFileTraverser(rawFileURLWithSAS, fileServiceClientWithSAS, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// invoke the file traversal with a dummy processor
		fileDummyProcessor := dummyProcessor{}
		err = azureFileTraverser.Traverse(noPreProccessor, fileDummyProcessor.process, nil)
		a.Nil(err)

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
			rawS3URL := scenarioHelper{}.getRawS3ObjectURL(a, "", bucketName, virDirName+"/")
			credentialInfo := common.CredentialInfo{CredentialType: common.ECredentialType.S3AccessKey()}
			S3Traverser, err := newS3Traverser(&rawS3URL, ctx, InitResourceTraverserOptions{
				Credential: &credentialInfo,
				Recursive:  isRecursiveOn,
			})
			a.Nil(err)
			err = S3Traverser.Traverse(noPreProccessor, s3DummyProcessor.process, nil)
			a.Nil(err)

			// check that the results are the same length
			a.Equal(localFileOnlyCount, len(s3DummyProcessor.record))
		}
		if gcpEnabled {
			rawGCPURL := scenarioHelper{}.getRawGCPObjectURL(a, bucketNameGCP, virDirName+"/")
			GCPTraverser, err := newGCPTraverser(&rawGCPURL, ctx, InitResourceTraverserOptions{
				Recursive: isRecursiveOn,
			})
			a.Nil(err)
			err = GCPTraverser.Traverse(noPreProccessor, gcpDummyProcessor.process, nil)
			a.Nil(err)

			a.Equal(localFileOnlyCount, len(gcpDummyProcessor.record))
		}

		// make sure the results are as expected
		a.Equal(localFileOnlyCount, len(blobDummyProcessor.record))
		if isRecursiveOn {
			a.Equal(localTotalCount, len(fileDummyProcessor.record))
		} else {
			// only files matter when not recursive (since ToNewCopyTransfer strips out everything else when non-recursive)
			a.Equal(localTotalCount, fileDummyProcessor.countFilesOnly())
		}
		// if s3 testing is disabled the s3 dummy processors' records will be empty. This is OK for appending. Nothing will happen.
		for _, storedObject := range append(append(append(blobDummyProcessor.record, fileDummyProcessor.record...), s3DummyProcessor.record...), gcpDummyProcessor.record...) {
			if isRecursiveOn || storedObject.entityType == common.EEntityType.File() { // folder enumeration knowingly NOT consistent when non-recursive (since the folders get stripped out by ToNewCopyTransfer when non-recursive anyway)

				correspondingLocalFile, present := localIndexer.indexMap[storedObject.relativePath]

				a.True(present)
				a.Equal(storedObject.name, correspondingLocalFile.name)
				// Say, here's a good question, why do we have this last check?
				// None of the other tests have it.
				a.True(correspondingLocalFile.isMoreRecentThan(storedObject, false))

				if !isRecursiveOn {
					a.False(strings.Contains(storedObject.relativePath, common.AZCOPY_PATH_SEPARATOR_STRING))
				}
			}
		}
	}
}

// validate traversing a virtual directory containing the same objects
// compare that the serial and parallel blob traversers get consistent results
func TestSerialAndParallelBlobTraverser(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// set up the container with numerous blobs
	virDirName := "virdir"
	scenarioHelper{}.generateCommonRemoteScenarioForBlob(a, cc, virDirName+"/")
	a.NotNil(cc)

	// test two scenarios, either recursive or not
	for _, isRecursiveOn := range []bool{true, false} {
		// construct a parallel blob traverser
		ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
		rawVirDirURLWithSAS := scenarioHelper{}.getBlobClientWithSAS(a, containerName, virDirName).URL()
		serviceClientWithSAS := scenarioHelper{}.getBlobServiceClientWithSASFromURL(a, rawVirDirURLWithSAS)
		parallelBlobTraverser := newBlobTraverser(rawVirDirURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})

		// construct a serial blob traverser
		serialBlobTraverser := newBlobTraverser(rawVirDirURLWithSAS, serviceClientWithSAS, ctx, InitResourceTraverserOptions{
			Recursive: isRecursiveOn,
		})
		serialBlobTraverser.parallelListing = false

		// invoke the parallel traversal with a dummy processor
		parallelDummyProcessor := dummyProcessor{}
		err := parallelBlobTraverser.Traverse(noPreProccessor, parallelDummyProcessor.process, nil)
		a.Nil(err)

		// invoke the serial traversal with a dummy processor
		serialDummyProcessor := dummyProcessor{}
		err = parallelBlobTraverser.Traverse(noPreProccessor, serialDummyProcessor.process, nil)
		a.Nil(err)

		// make sure the results are as expected
		a.Equal(len(serialDummyProcessor.record), len(parallelDummyProcessor.record))

		// compare the entries one by one
		lookupMap := make(map[string]StoredObject)
		for _, entry := range parallelDummyProcessor.record {
			lookupMap[entry.relativePath] = entry
		}

		for _, storedObject := range serialDummyProcessor.record {
			correspondingFile, present := lookupMap[storedObject.relativePath]
			a.True(present)
			a.Equal(correspondingFile.lastModifiedTime, storedObject.lastModifiedTime)
			a.Equal(correspondingFile.md5, storedObject.md5)
		}
	}
}
