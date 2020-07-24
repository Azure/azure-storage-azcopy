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
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	minio "github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	chk "gopkg.in/check.v1"
)

const defaultFileSize = 1024

type scenarioHelper struct{}

var specialNames = []string{
	"打麻将.txt",
	"wow such space so much space",
	"打%%#%@#%麻将.txt",
	//"saywut.pdf?yo=bla&WUWUWU=foo&sig=yyy", // TODO this breaks on windows, figure out a way to add it only for tests on Unix
	"coração",
	"আপনার নাম কি",
	"%4509%4254$85140&",
	"Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft",
	"お名前は何ですか",
	"Adın ne",
	"як вас звати",
}

// note: this is to emulate the list-of-files flag
func (scenarioHelper) generateListOfFiles(c *chk.C, fileList []string) (path string) {
	parentDirName, err := ioutil.TempDir("", "AzCopyLocalTest")
	c.Assert(err, chk.IsNil)

	// create the file
	path = common.GenerateFullPath(parentDirName, generateName("listy", 0))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	c.Assert(err, chk.IsNil)

	// pipe content into it
	content := strings.Join(fileList, "\n")
	err = ioutil.WriteFile(path, []byte(content), common.DEFAULT_FILE_PERM)
	c.Assert(err, chk.IsNil)
	return
}

func (scenarioHelper) generateLocalDirectory(c *chk.C) (dstDirName string) {
	dstDirName, err := ioutil.TempDir("", "AzCopyLocalTest")
	c.Assert(err, chk.IsNil)
	return
}

// create a test file
func (scenarioHelper) generateLocalFile(filePath string, fileSize int) ([]byte, error) {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// create all parent directories
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	// write to file and return the data
	err = ioutil.WriteFile(filePath, bigBuff, common.DEFAULT_FILE_PERM)
	return bigBuff, err
}

func (s scenarioHelper) generateLocalFilesFromList(c *chk.C, dirPath string, fileList []string) {
	for _, fileName := range fileList {
		_, err := s.generateLocalFile(filepath.Join(dirPath, fileName), defaultFileSize)
		c.Assert(err, chk.IsNil)
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) generateCommonRemoteScenarioForLocal(c *chk.C, dirPath string, prefix string) (fileList []string) {
	fileList = make([]string, 50)
	for i := 0; i < 10; i++ {
		batch := []string{
			generateName(prefix+"top", 0),
			generateName(prefix+"sub1/", 0),
			generateName(prefix+"sub2/", 0),
			generateName(prefix+"sub1/sub3/sub5/", 0),
			generateName(prefix+specialNames[i], 0),
		}

		for j, name := range batch {
			fileList[5*i+j] = name
			_, err := s.generateLocalFile(filepath.Join(dirPath, name), defaultFileSize)
			c.Assert(err, chk.IsNil)
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

// make 50 blobs with random names
// 10 of them at the top level
// 10 of them in sub dir "sub1"
// 10 of them in sub dir "sub2"
// 10 of them in deeper sub dir "sub1/sub3/sub5"
// 10 of them with special characters
func (scenarioHelper) generateCommonRemoteScenarioForBlob(c *chk.C, containerURL azblob.ContainerURL, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(c, containerURL, prefix+"top")
		_, blobName2 := createNewBlockBlob(c, containerURL, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(c, containerURL, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(c, containerURL, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(c, containerURL, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

// same as blob, but for every virtual directory, a blob with the same name is created, and it has metadata 'hdi_isfolder = true'
func (scenarioHelper) generateCommonRemoteScenarioForWASB(c *chk.C, containerURL azblob.ContainerURL, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(c, containerURL, prefix+"top")
		_, blobName2 := createNewBlockBlob(c, containerURL, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(c, containerURL, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(c, containerURL, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(c, containerURL, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	if prefix != "" {
		rootDir := strings.TrimSuffix(prefix, "/")
		createNewDirectoryStub(c, containerURL, rootDir)
		blobList = append(blobList, rootDir)
	}

	createNewDirectoryStub(c, containerURL, prefix+"sub1")
	createNewDirectoryStub(c, containerURL, prefix+"sub1/sub3")
	createNewDirectoryStub(c, containerURL, prefix+"sub1/sub3/sub5")
	createNewDirectoryStub(c, containerURL, prefix+"sub2")

	for _, dirPath := range []string{prefix + "sub1", prefix + "sub1/sub3", prefix + "sub1/sub3/sub5", prefix + "sub2"} {
		blobList = append(blobList, dirPath)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForBlobFS(c *chk.C, filesystemURL azbfs.FileSystemURL, prefix string) (pathList []string) {
	pathList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, pathName1 := createNewBfsFile(c, filesystemURL, prefix+"top")
		_, pathName2 := createNewBfsFile(c, filesystemURL, prefix+"sub1/")
		_, pathName3 := createNewBfsFile(c, filesystemURL, prefix+"sub2/")
		_, pathName4 := createNewBfsFile(c, filesystemURL, prefix+"sub1/sub3/sub5")
		_, pathName5 := createNewBfsFile(c, filesystemURL, prefix+specialNames[i])

		pathList[5*i] = pathName1
		pathList[5*i+1] = pathName2
		pathList[5*i+2] = pathName3
		pathList[5*i+3] = pathName4
		pathList[5*i+4] = pathName5
	}

	// sleep a bit so that the paths' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForAzureFile(c *chk.C, shareURL azfile.ShareURL, prefix string) (fileList []string) {
	fileList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, fileName1 := createNewAzureFile(c, shareURL, prefix+"top")
		_, fileName2 := createNewAzureFile(c, shareURL, prefix+"sub1/")
		_, fileName3 := createNewAzureFile(c, shareURL, prefix+"sub2/")
		_, fileName4 := createNewAzureFile(c, shareURL, prefix+"sub1/sub3/sub5/")
		_, fileName5 := createNewAzureFile(c, shareURL, prefix+specialNames[i])

		fileList[5*i] = fileName1
		fileList[5*i+1] = fileName2
		fileList[5*i+2] = fileName3
		fileList[5*i+3] = fileName4
		fileList[5*i+4] = fileName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (s scenarioHelper) generateBlobContainersAndBlobsFromLists(c *chk.C, serviceURL azblob.ServiceURL, containerList []string, blobList []string, data string) {
	for _, containerName := range containerList {
		curl := serviceURL.NewContainerURL(containerName)
		_, err := curl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		c.Assert(err, chk.IsNil)

		s.generateBlobsFromList(c, curl, blobList, data)
	}
}

func (s scenarioHelper) generateFileSharesAndFilesFromLists(c *chk.C, serviceURL azfile.ServiceURL, shareList []string, fileList []string, data string) {
	for _, shareName := range shareList {
		surl := serviceURL.NewShareURL(shareName)
		_, err := surl.Create(ctx, azfile.Metadata{}, 0)
		c.Assert(err, chk.IsNil)

		s.generateAzureFilesFromList(c, surl, fileList)
	}
}

func (s scenarioHelper) generateFilesystemsAndFilesFromLists(c *chk.C, serviceURL azbfs.ServiceURL, fsList []string, fileList []string, data string) {
	for _, filesystemName := range fsList {
		fsURL := serviceURL.NewFileSystemURL(filesystemName)
		_, err := fsURL.Create(ctx)
		c.Assert(err, chk.IsNil)

		s.generateBFSPathsFromList(c, fsURL, fileList)
	}
}

func (s scenarioHelper) generateS3BucketsAndObjectsFromLists(c *chk.C, s3Client *minio.Client, bucketList []string, objectList []string, data string) {
	for _, bucketName := range bucketList {
		err := s3Client.MakeBucket(bucketName, "")
		c.Assert(err, chk.IsNil)

		s.generateObjects(c, s3Client, bucketName, objectList)
	}
}

// create the demanded blobs
func (scenarioHelper) generateBlobsFromList(c *chk.C, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		blob := containerURL.NewBlockBlobURL(blobName)
		cResp, err := blob.Upload(ctx, strings.NewReader(data), azblob.BlobHTTPHeaders{},
			nil, azblob.BlobAccessConditions{})
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generatePageBlobsFromList(c *chk.C, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		//Create the blob (PUT blob)
		blob := containerURL.NewPageBlobURL(blobName)
		cResp, err := blob.Create(ctx,
			int64(len(data)),
			0,
			azblob.BlobHTTPHeaders{
				ContentType: "text/random",
			},
			azblob.Metadata{},
			azblob.BlobAccessConditions{},
		)
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)

		//Create the page (PUT page)
		uResp, err := blob.UploadPages(ctx,
			0,
			strings.NewReader(data),
			azblob.PageBlobAccessConditions{},
			nil,
		)
		c.Assert(err, chk.IsNil)
		c.Assert(uResp.StatusCode(), chk.Equals, 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateAppendBlobsFromList(c *chk.C, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		//Create the blob (PUT blob)
		blob := containerURL.NewAppendBlobURL(blobName)
		cResp, err := blob.Create(ctx,
			azblob.BlobHTTPHeaders{
				ContentType: "text/random",
			},
			azblob.Metadata{},
			azblob.BlobAccessConditions{},
		)
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)

		//Append a block (PUT block)
		uResp, err := blob.AppendBlock(ctx,
			strings.NewReader(data),
			azblob.AppendBlobAccessConditions{},
			nil)
		c.Assert(err, chk.IsNil)
		c.Assert(uResp.StatusCode(), chk.Equals, 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateBlockBlobWithAccessTier(c *chk.C, containerURL azblob.ContainerURL, blobName string, accessTier azblob.AccessTierType) {
	blob := containerURL.NewBlockBlobURL(blobName)
	cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	_, err = blob.SetTier(ctx, accessTier, azblob.LeaseAccessConditions{})
	c.Assert(err, chk.IsNil)
}

// create the demanded objects
func (scenarioHelper) generateObjects(c *chk.C, client *minio.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		n, err := client.PutObjectWithContext(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
		c.Assert(err, chk.IsNil)
		c.Assert(n, chk.Equals, size)
	}
}

// create the demanded files
func (scenarioHelper) generateFlatFiles(c *chk.C, shareURL azfile.ShareURL, fileList []string) {
	for _, fileName := range fileList {
		file := shareURL.NewRootDirectoryURL().NewFileURL(fileName)
		err := azfile.UploadBufferToAzureFile(ctx, []byte(fileDefaultData), file, azfile.UploadToAzureFileOptions{})
		c.Assert(err, chk.IsNil)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

// make 50 objects with random names
// 10 of them at the top level
// 10 of them in sub dir "sub1"
// 10 of them in sub dir "sub2"
// 10 of them in deeper sub dir "sub1/sub3/sub5"
// 10 of them with special characters
func (scenarioHelper) generateCommonRemoteScenarioForS3(c *chk.C, client *minio.Client, bucketName string, prefix string, returnObjectListWithBucketName bool) (objectList []string) {
	objectList = make([]string, 50)

	for i := 0; i < 10; i++ {
		objectName1 := createNewObject(c, client, bucketName, prefix+"top")
		objectName2 := createNewObject(c, client, bucketName, prefix+"sub1/")
		objectName3 := createNewObject(c, client, bucketName, prefix+"sub2/")
		objectName4 := createNewObject(c, client, bucketName, prefix+"sub1/sub3/sub5/")
		objectName5 := createNewObject(c, client, bucketName, prefix+specialNames[i])

		// Note: common.AZCOPY_PATH_SEPARATOR_STRING is added before bucket or objectName, as in the change minimize JobPartPlan file size,
		// transfer.Source & transfer.Destination(after trimed the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
		// when user provided source & destination are without / suffix, which is the case for scenarioHelper generated URL.

		bucketPath := ""
		if returnObjectListWithBucketName {
			bucketPath = common.AZCOPY_PATH_SEPARATOR_STRING + bucketName
		}

		objectList[5*i] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName1
		objectList[5*i+1] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName2
		objectList[5*i+2] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName3
		objectList[5*i+3] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName4
		objectList[5*i+4] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

// create the demanded azure files
func (scenarioHelper) generateAzureFilesFromList(c *chk.C, shareURL azfile.ShareURL, fileList []string) {
	for _, filePath := range fileList {
		file := shareURL.NewRootDirectoryURL().NewFileURL(filePath)

		// create parents first
		generateParentsForAzureFile(c, file)

		// create the file itself
		cResp, err := file.Create(ctx, defaultAzureFileSizeInBytes, azfile.FileHTTPHeaders{}, azfile.Metadata{})
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateBFSPathsFromList(c *chk.C, filesystemURL azbfs.FileSystemURL, fileList []string) {
	for _, path := range fileList {
		file := filesystemURL.NewRootDirectoryURL().NewFileURL(path)

		// Create the file
		cResp, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{})
		c.Assert(err, chk.IsNil)
		c.Assert(cResp.StatusCode(), chk.Equals, 201)

		aResp, err := file.AppendData(ctx, 0, strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes))))
		c.Assert(err, chk.IsNil)
		c.Assert(aResp.StatusCode(), chk.Equals, 202)

		fResp, err := file.FlushData(ctx, defaultBlobFSFileSizeInBytes, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
		c.Assert(err, chk.IsNil)
		c.Assert(fResp.StatusCode(), chk.Equals, 200)
	}
}

// Golang does not have sets, so we have to use a map to fulfill the same functionality
func (scenarioHelper) convertListToMap(list []string) map[string]int {
	lookupMap := make(map[string]int)
	for _, entryName := range list {
		lookupMap[entryName] = 0
	}

	return lookupMap
}

func (scenarioHelper) convertMapKeysToList(m map[string]int) []string {
	list := make([]string, len(m))
	i := 0
	for key := range m {
		list[i] = key
		i++
	}
	return list
}

// useful for files->files transfers, where folders are included in the transfers.
// includeRoot should be set to true for cases where we expect the root directory to be copied across
// (i.e. where we expect the behaviour that can be, but has not been in this case, turned off by appending /* to the source)
func (s scenarioHelper) addFoldersToList(fileList []string, includeRoot bool) []string {
	m := s.convertListToMap(fileList)
	// for each file, add all its parent dirs
	for name := range m {
		for {
			name = path.Dir(name)
			if name == "." {
				if includeRoot {
					m[""] = 0 // don't use "."
				}
				break
			} else {
				m[name] = 0
			}
		}
	}
	return s.convertMapKeysToList(m)
}

func (scenarioHelper) shaveOffPrefix(list []string, prefix string) []string {
	cleanList := make([]string, len(list))
	for i, item := range list {
		cleanList[i] = strings.TrimPrefix(item, prefix)
	}
	return cleanList
}

func (scenarioHelper) addPrefix(list []string, prefix string) []string {
	modifiedList := make([]string, len(list))
	for i, item := range list {
		modifiedList[i] = prefix + item
	}
	return modifiedList
}

func (scenarioHelper) getRawContainerURLWithSAS(c *chk.C, containerName string) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil)
	containerURLWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	return containerURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobURLWithSAS(c *chk.C, containerName string, blobName string) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil)
	containerURLWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlockBlobURL(blobName)
	return blobURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobServiceURLWithSAS(c *chk.C) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil)

	return getBlobServiceURLWithSAS(c, *credential).URL()
}

func (scenarioHelper) getRawFileServiceURLWithSAS(c *chk.C) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil)

	return getFileServiceURLWithSAS(c, *credential).URL()
}

func (scenarioHelper) getRawAdlsServiceURLWithSAS(c *chk.C) azbfs.ServiceURL {
	accountName, accountKey := getAccountAndKey()
	credential := azbfs.NewSharedKeyCredential(accountName, accountKey)

	return getAdlsServiceURLWithSAS(c, *credential)
}

func (scenarioHelper) getBlobServiceURL(c *chk.C) azblob.ServiceURL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net", credential.AccountName())

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil)

	return azblob.NewServiceURL(*fullURL, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
}

func (s scenarioHelper) getContainerURL(c *chk.C, containerName string) azblob.ContainerURL {
	serviceURL := s.getBlobServiceURL(c)
	containerURL := serviceURL.NewContainerURL(containerName)

	return containerURL
}

func (scenarioHelper) getRawS3AccountURL(c *chk.C, region string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com", common.IffString(region == "", "", "-"+region))

	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil)

	return *fullURL
}

// TODO: Possibly add virtual-hosted-style and dual stack support. Currently use path style for testing.
func (scenarioHelper) getRawS3BucketURL(c *chk.C, region string, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s", common.IffString(region == "", "", "-"+region), bucketName)

	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil)

	return *fullURL
}

func (scenarioHelper) getRawS3ObjectURL(c *chk.C, region string, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s/%s", common.IffString(region == "", "", "-"+region), bucketName, objectName)

	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil)

	return *fullURL
}

func (scenarioHelper) getRawFileURLWithSAS(c *chk.C, shareName string, fileName string) url.URL {
	credential, err := getGenericCredentialForFile("")
	c.Assert(err, chk.IsNil)
	shareURLWithSAS := getShareURLWithSAS(c, *credential, shareName)
	fileURLWithSAS := shareURLWithSAS.NewRootDirectoryURL().NewFileURL(fileName)
	return fileURLWithSAS.URL()
}

func (scenarioHelper) getRawShareURLWithSAS(c *chk.C, shareName string) url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil)
	shareURLWithSAS := getShareURLWithSAS(c, *credential, shareName)
	return shareURLWithSAS.URL()
}

func (scenarioHelper) blobExists(blobURL azblob.BlobURL) bool {
	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{})
	if err == nil {
		return true
	}
	return false
}

func (scenarioHelper) containerExists(containerURL azblob.ContainerURL) bool {
	_, err := containerURL.GetProperties(context.Background(), azblob.LeaseAccessConditions{})
	if err == nil {
		return true
	}
	return false
}

func runSyncAndVerify(c *chk.C, raw rawSyncCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func runCopyAndVerify(c *chk.C, raw rawCopyCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	if err != nil {
		verifier(err)
		return
	}

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func validateUploadTransfersAreScheduled(c *chk.C, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(c, false, true, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateDownloadTransfersAreScheduled(c *chk.C, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(c, true, false, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateS2SSyncTransfersAreScheduled(c *chk.C, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(c, true, true, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateCopyTransfersAreScheduled(c *chk.C, isSrcEncoded bool, isDstEncoded bool, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	// validate that the right number of transfers were scheduled
	c.Assert(len(mockedRPC.transfers), chk.Equals, len(expectedTransfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := strings.TrimPrefix(transfer.Source, sourcePrefix)
		dstRelativeFilePath := strings.TrimPrefix(transfer.Destination, destinationPrefix)

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)

			if runtime.GOOS == "windows" {
				// Decode unsafe dst characters on windows
				pathParts := strings.Split(dstRelativeFilePath, "/")
				invalidChars := `<>\/:"|?*` + string(0x00)

				for _, c := range strings.Split(invalidChars, "") {
					for k, p := range pathParts {
						pathParts[k] = strings.ReplaceAll(p, url.PathEscape(c), c)
					}
				}

				dstRelativeFilePath = strings.Join(pathParts, "/")
			}
		}

		if isDstEncoded {
			dstRelativeFilePath, _ = url.PathUnescape(dstRelativeFilePath)
		}

		// the relative paths should be equal
		c.Assert(srcRelativeFilePath, chk.Equals, dstRelativeFilePath)

		// look up the path from the expected transfers, make sure it exists
		_, transferExist := lookupMap[srcRelativeFilePath]
		c.Assert(transferExist, chk.Equals, true)
	}
}

func validateRemoveTransfersAreScheduled(c *chk.C, isSrcEncoded bool, expectedTransfers []string, mockedRPC interceptor) {

	// validate that the right number of transfers were scheduled
	c.Assert(len(mockedRPC.transfers), chk.Equals, len(expectedTransfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := transfer.Source

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)
		}

		// look up the source from the expected transfers, make sure it exists
		_, srcExist := lookupMap[srcRelativeFilePath]
		c.Assert(srcExist, chk.Equals, true)

		delete(lookupMap, srcRelativeFilePath)
	}
	//if len(lookupMap) > 0 {
	//	panic("set breakpoint here to debug")
	//}
}

func getDefaultSyncRawInput(src, dst string) rawSyncCmdArgs {
	deleteDestination := common.EDeleteDestination.True()

	return rawSyncCmdArgs{
		src:                 src,
		dst:                 dst,
		recursive:           true,
		logVerbosity:        defaultLogVerbosityForSync,
		deleteDestination:   deleteDestination.String(),
		md5ValidationOption: common.DefaultHashValidationOption.String(),
	}
}

func getDefaultCopyRawInput(src string, dst string) rawCopyCmdArgs {
	return rawCopyCmdArgs{
		src:                            src,
		dst:                            dst,
		logVerbosity:                   defaultLogVerbosityForSync,
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
	}
}

func getDefaultRemoveRawInput(src string) rawCopyCmdArgs {
	fromTo := common.EFromTo.BlobTrash()
	srcURL, _ := url.Parse(src)

	if strings.Contains(srcURL.Host, "file") {
		fromTo = common.EFromTo.FileTrash()
	} else if strings.Contains(srcURL.Host, "dfs") {
		fromTo = common.EFromTo.BlobFSTrash()
	}

	return rawCopyCmdArgs{
		src:                            src,
		fromTo:                         fromTo.String(),
		logVerbosity:                   defaultLogVerbosityForSync,
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		includeDirectoryStubs:          true,
	}
}
