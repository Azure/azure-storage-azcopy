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
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	datalakeservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/minio/minio-go/v7"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const defaultFileSize = 1024

type scenarioHelper struct{}

var specialNames = []string{
	"打麻将.txt",
	"wow such space so much space",
	"打%%#%@#%麻将.txt",
	// "saywut.pdf?yo=bla&WUWUWU=foo&sig=yyy", // TODO this breaks on windows, figure out a way to add it only for tests on Unix
	"coração",
	"আপনার নাম কি",
	"%4509%4254$85140&",
	"Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft",
	"お名前は何ですか",
	"Adın ne",
	"як вас звати",
}

// note: this is to emulate the list-of-files flag
func (scenarioHelper) generateListOfFiles(a *assert.Assertions, fileList []string) (path string) {
	parentDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	a.Nil(err)

	// create the file
	path = common.GenerateFullPath(parentDirName, generateName("listy", 0))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	a.Nil(err)

	// pipe content into it
	content := strings.Join(fileList, "\n")
	err = os.WriteFile(path, []byte(content), common.DEFAULT_FILE_PERM)
	a.Nil(err)
	return
}

func (scenarioHelper) generateLocalDirectory(a *assert.Assertions) (dstDirName string) {
	dstDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	a.Nil(err)
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
	err = os.WriteFile(filePath, bigBuff, common.DEFAULT_FILE_PERM)
	return bigBuff, err
}

func (s scenarioHelper) generateLocalFilesFromList(a *assert.Assertions, dirPath string, fileList []string) {
	for _, fileName := range fileList {
		_, err := s.generateLocalFile(filepath.Join(dirPath, fileName), defaultFileSize)
		a.Nil(err)
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) generateCommonRemoteScenarioForLocal(a *assert.Assertions, dirPath string, prefix string) (fileList []string) {
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
			a.Nil(err)
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForSoftDelete(a *assert.Assertions, containerClient *container.Client, prefix string) (string, []*blockblob.Client, []string) {
	blobList := make([]*blockblob.Client, 3)
	blobNames := make([]string, 3)
	var listOfTransfers []string

	blobClient1, blobName1 := createNewBlockBlob(a, containerClient, prefix+"top")
	blobClient2, blobName2 := createNewBlockBlob(a, containerClient, prefix+"sub1/")
	blobClient3, blobName3 := createNewBlockBlob(a, containerClient, prefix+"sub1/sub3/sub5/")

	blobList[0] = blobClient1
	blobNames[0] = blobName1
	blobList[1] = blobClient2
	blobNames[1] = blobName2
	blobList[2] = blobClient3
	blobNames[2] = blobName3

	for i := 0; i < len(blobList); i++ {
		for j := 0; j < 3; j++ { // create 3 soft-deleted snapshots for each blob
			// Create snapshot for blob
			snapResp, err := blobList[i].CreateSnapshot(ctx, nil)
			a.NotNil(snapResp)
			a.Nil(err)

			time.Sleep(time.Millisecond * 30)

			// Soft delete snapshot
			snapshotBlob, err := blobList[i].WithSnapshot(*snapResp.Snapshot)
			a.Nil(err)
			_, err = snapshotBlob.Delete(ctx, nil)
			a.Nil(err)

			listOfTransfers = append(listOfTransfers, blobNames[i])
		}
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return blobName1, blobList, listOfTransfers
}

func (scenarioHelper) generateCommonRemoteScenarioForBlob(a *assert.Assertions, containerClient *container.Client, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(a, containerClient, prefix+"top")
		_, blobName2 := createNewBlockBlob(a, containerClient, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(a, containerClient, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(a, containerClient, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(a, containerClient, prefix+specialNames[i])

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
func (scenarioHelper) generateCommonRemoteScenarioForWASB(a *assert.Assertions, containerClient *container.Client, prefix string) (blobList []string) {
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(a, containerClient, prefix+"top")
		_, blobName2 := createNewBlockBlob(a, containerClient, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(a, containerClient, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(a, containerClient, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(a, containerClient, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	if prefix != "" {
		rootDir := strings.TrimSuffix(prefix, "/")
		createNewDirectoryStub(a, containerClient, rootDir)
		blobList = append(blobList, rootDir)
	}

	createNewDirectoryStub(a, containerClient, prefix+"sub1")
	createNewDirectoryStub(a, containerClient, prefix+"sub1/sub3")
	createNewDirectoryStub(a, containerClient, prefix+"sub1/sub3/sub5")
	createNewDirectoryStub(a, containerClient, prefix+"sub2")

	blobList = append(blobList, []string{prefix + "sub1", prefix + "sub1/sub3", prefix + "sub1/sub3/sub5", prefix + "sub2"}...)
	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForBlobFS(a *assert.Assertions, filesystemClient *filesystem.Client, prefix string) (pathList []string) {
	pathList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, pathName1 := createNewBfsFile(a, filesystemClient, prefix+"top")
		_, pathName2 := createNewBfsFile(a, filesystemClient, prefix+"sub1/")
		_, pathName3 := createNewBfsFile(a, filesystemClient, prefix+"sub2/")
		_, pathName4 := createNewBfsFile(a, filesystemClient, prefix+"sub1/sub3/sub5")
		_, pathName5 := createNewBfsFile(a, filesystemClient, prefix+specialNames[i])

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

func (scenarioHelper) generateCommonRemoteScenarioForAzureFile(a *assert.Assertions, shareClient *share.Client, serviceClient *fileservice.Client, prefix string) (fileList []string) {
	fileList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, fileName1 := createNewShareFile(a, shareClient, serviceClient, prefix+"top")
		_, fileName2 := createNewShareFile(a, shareClient, serviceClient, prefix+"sub1/")
		_, fileName3 := createNewShareFile(a, shareClient, serviceClient, prefix+"sub2/")
		_, fileName4 := createNewShareFile(a, shareClient, serviceClient, prefix+"sub1/sub3/sub5/")
		_, fileName5 := createNewShareFile(a, shareClient, serviceClient, prefix+specialNames[i])

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

func (s scenarioHelper) generateBlobContainersAndBlobsFromLists(a *assert.Assertions, serviceClient *blobservice.Client, containerList []string, blobList []string, data string) {
	for _, containerName := range containerList {
		containerClient := serviceClient.NewContainerClient(containerName)
		_, err := containerClient.Create(ctx, nil)
		a.Nil(err)

		s.generateBlobsFromList(a, containerClient, blobList, data)
	}
}

func (s scenarioHelper) generateFileSharesAndFilesFromLists(a *assert.Assertions, serviceClient *fileservice.Client, shareList []string, fileList []string, data string) {
	for _, shareName := range shareList {
		shareClient := serviceClient.NewShareClient(shareName)
		_, err := shareClient.Create(ctx, nil)
		a.Nil(err)

		s.generateShareFilesFromList(a, shareClient, serviceClient, fileList)
	}
}

func (s scenarioHelper) generateFilesystemsAndFilesFromLists(a *assert.Assertions, serviceClient *datalakeservice.Client, fsList []string, fileList []string, data string) {
	for _, filesystemName := range fsList {
		fsClient := serviceClient.NewFileSystemClient(filesystemName)
		_, err := fsClient.Create(ctx, nil)
		a.Nil(err)

		s.generateBFSPathsFromList(a, fsClient, fileList)
	}
}

func (s scenarioHelper) generateS3BucketsAndObjectsFromLists(a *assert.Assertions, s3Client *minio.Client, bucketList []string, objectList []string, data string) {
	for _, bucketName := range bucketList {
		err := s3Client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: ""})
		a.Nil(err)

		s.generateObjects(a, s3Client, bucketName, objectList)
	}
}

func (s scenarioHelper) generateGCPBucketsAndObjectsFromLists(a *assert.Assertions, client *gcpUtils.Client, bucketList []string, objectList []string) {
	for _, bucketName := range bucketList {
		bkt := client.Bucket(bucketName)
		err := bkt.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
		a.Nil(err)
		s.generateGCPObjects(a, client, bucketName, objectList)
	}
}

// create the demanded blobs
func (scenarioHelper) generateBlobsFromList(a *assert.Assertions, containerClient *container.Client, blobList []string, data string) {
	for _, blobName := range blobList {
		blobClient := containerClient.NewBlockBlobClient(blobName)
		_, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateVersionsForBlobsFromList(a *assert.Assertions, containerClient *container.Client, blobList []string, randomData []string) {
	for i, blobName := range blobList {
		blobClient := containerClient.NewBlockBlobClient(blobName)
		uploadResp, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(randomData[i])), nil)
		a.NoError(err)
		a.NotNil(uploadResp.VersionID)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generatePageBlobsFromList(a *assert.Assertions, containerClient *container.Client, blobList []string, data string) {
	for _, blobName := range blobList {
		// Create the blob (PUT blob)
		blobClient := containerClient.NewPageBlobClient(blobName)
		_, err := blobClient.Create(ctx,
			int64(len(data)),
			&pageblob.CreateOptions{
				SequenceNumber: to.Ptr(int64(0)),
				HTTPHeaders:    &blob.HTTPHeaders{BlobContentType: to.Ptr("text/random")},
			})
		a.Nil(err)

		// Create the page (PUT page)
		_, err = blobClient.UploadPages(ctx, streaming.NopCloser(strings.NewReader(data)),
			blob.HTTPRange{Offset: 0, Count: int64(len(data))}, nil)
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateAppendBlobsFromList(a *assert.Assertions, containerClient *container.Client, blobList []string, data string) {
	for _, blobName := range blobList {
		// Create the blob (PUT blob)
		blobClient := containerClient.NewAppendBlobClient(blobName)
		_, err := blobClient.Create(ctx,
			&appendblob.CreateOptions{
				HTTPHeaders: &blob.HTTPHeaders{BlobContentType: to.Ptr("text/random")},
			})
		a.Nil(err)

		// Append a block (PUT block)
		_, err = blobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
		a.Nil(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateBlockBlobWithAccessTier(a *assert.Assertions, containerClient *container.Client, blobName string, accessTier *blob.AccessTier) {
	blobClient := containerClient.NewBlockBlobClient(blobName)
	_, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(blockBlobDefaultData)), &blockblob.UploadOptions{Tier: accessTier})
	a.Nil(err)
}

// create the demanded objects
func (scenarioHelper) generateObjects(a *assert.Assertions, client *minio.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		n, err := client.PutObject(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
		a.Nil(err)
		a.Equal(size, n)
	}
}

func (scenarioHelper) generateGCPObjects(a *assert.Assertions, client *gcpUtils.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		wc := client.Bucket(bucketName).Object(objectName).NewWriter(context.Background())
		reader := strings.NewReader(objectDefaultData)
		written, err := io.Copy(wc, reader)
		a.Nil(err)
		a.Equal(size, written)
		err = wc.Close()
		a.Nil(err)
	}
}

// create the demanded files
func (scenarioHelper) generateFlatFiles(a *assert.Assertions, shareClient *share.Client, fileList []string) {
	for _, fileName := range fileList {
		fileClient := shareClient.NewRootDirectoryClient().NewFileClient(fileName)
		_, err := fileClient.Create(ctx, int64(len(fileDefaultData)), nil)
		a.Nil(err)
		err = fileClient.UploadBuffer(ctx, []byte(fileDefaultData), nil)
		a.Nil(err)
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
func (scenarioHelper) generateCommonRemoteScenarioForS3(a *assert.Assertions, client *minio.Client, bucketName string, prefix string, returnObjectListWithBucketName bool) (objectList []string) {
	objectList = make([]string, 50)

	for i := 0; i < 10; i++ {
		objectName1 := createNewObject(a, client, bucketName, prefix+"top")
		objectName2 := createNewObject(a, client, bucketName, prefix+"sub1/")
		objectName3 := createNewObject(a, client, bucketName, prefix+"sub2/")
		objectName4 := createNewObject(a, client, bucketName, prefix+"sub1/sub3/sub5/")
		objectName5 := createNewObject(a, client, bucketName, prefix+specialNames[i])

		// Note: common.AZCOPY_PATH_SEPARATOR_STRING is added before bucket or objectName, as in the change minimize JobPartPlan file size,
		// transfer.Source & transfer.Destination(after trimming the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
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

func (scenarioHelper) generateCommonRemoteScenarioForGCP(a *assert.Assertions, client *gcpUtils.Client, bucketName string, prefix string, returnObjectListWithBucketName bool) []string {
	objectList := make([]string, 50)
	for i := 0; i < 10; i++ {
		objectName1 := createNewGCPObject(a, client, bucketName, prefix+"top")
		objectName2 := createNewGCPObject(a, client, bucketName, prefix+"sub1/")
		objectName3 := createNewGCPObject(a, client, bucketName, prefix+"sub2/")
		objectName4 := createNewGCPObject(a, client, bucketName, prefix+"sub1/sub3/sub5/")
		objectName5 := createNewGCPObject(a, client, bucketName, prefix+specialNames[i])

		// Note: common.AZCOPY_PATH_SEPARATOR_STRING is added before bucket or objectName, as in the change minimize JobPartPlan file size,
		// transfer.Source & transfer.Destination(after trimming the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
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
	return objectList
}

func (scenarioHelper) generateShareFilesFromList(a *assert.Assertions, shareClient *share.Client, _ *fileservice.Client, fileList []string) {
	for _, filePath := range fileList {
		fileClient := shareClient.NewRootDirectoryClient().NewFileClient(filePath)

		// create parents first
		generateParentsForShareFile(a, fileClient, shareClient)

		// create the file itself
		_, err := fileClient.Create(ctx, defaultAzureFileSizeInBytes, nil)
		a.Nil(err)
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Second * 3)
}

func (scenarioHelper) generateBFSPathsFromList(a *assert.Assertions, fsClient *filesystem.Client, fileList []string) {
	for _, p := range fileList {
		// TODO : RootDirectoryClient in datalake SDK
		fc, err := fsClient.NewDirectoryClient("").NewFileClient(p)
		a.Nil(err)

		// Create the file
		_, err = fc.Create(ctx, nil)
		a.Nil(err)

		_, err = fc.AppendData(ctx, 0, streaming.NopCloser(strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes)))), nil)
		a.Nil(err)

		_, err = fc.FlushData(ctx, defaultBlobFSFileSizeInBytes, &datalakefile.FlushDataOptions{Close: to.Ptr(true)})
		a.Nil(err)
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

func (scenarioHelper) getRawContainerURLWithSAS(a *assert.Assertions, containerName string) *url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	cc := getContainerClientWithSAS(a, credential, containerName)

	u := cc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getSecondaryRawContainerURLWithSAS(a *assert.Assertions, containerName string) *url.URL {
	accountName, accountKey := getSecondaryAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	cc := getContainerClientWithSAS(a, credential, containerName)

	u := cc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getContainerClientWithSAS(a *assert.Assertions, containerName string) *container.Client {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	containerURLWithSAS := getContainerClientWithSAS(a, credential, containerName)
	return containerURLWithSAS
}

func (scenarioHelper) getRawBlobURLWithSAS(a *assert.Assertions, containerName string, blobName string) *url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	cc := getContainerClientWithSAS(a, credential, containerName)
	bc := cc.NewBlockBlobClient(blobName)

	u := bc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getSecondaryRawBlobURLWithSAS(a *assert.Assertions, containerName string, blobName string) *url.URL {
	accountName, accountKey := getSecondaryAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	cc := getContainerClientWithSAS(a, credential, containerName)
	bc := cc.NewBlockBlobClient(blobName)

	u := bc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getBlobClientWithSAS(a *assert.Assertions, containerName string, blobName string) *blob.Client {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	containerURLWithSAS := getContainerClientWithSAS(a, credential, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlobClient(blobName)
	return blobURLWithSAS
}

func (scenarioHelper) getRawBlobServiceURLWithSAS(a *assert.Assertions) *url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	u := getBlobServiceClientWithSAS(a, credential).URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getBlobServiceClientWithSAS(a *assert.Assertions) *blobservice.Client {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	return getBlobServiceClientWithSAS(a, credential)
}

func (scenarioHelper) getSecondaryBlobServiceClientWithSAS(a *assert.Assertions) *blobservice.Client {
	accountName, accountKey := getSecondaryAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	return getBlobServiceClientWithSAS(a, credential)
}

func (scenarioHelper) getBlobServiceClientWithSASFromURL(a *assert.Assertions, rawURL string) *blobservice.Client {
	blobURLParts, err := blob.ParseURL(rawURL)
	a.Nil(err)
	blobURLParts.ContainerName = ""
	blobURLParts.BlobName = ""
	blobURLParts.VersionID = ""
	blobURLParts.Snapshot = ""

	client, err := blobservice.NewClientWithNoCredential(blobURLParts.String(), nil)
	a.Nil(err)

	return client
}

func (scenarioHelper) getFileServiceClientWithSASFromURL(a *assert.Assertions, rawURL string) *fileservice.Client {
	fileURLParts, err := sharefile.ParseURL(rawURL)
	a.Nil(err)
	fileURLParts.ShareName = ""
	fileURLParts.ShareSnapshot = ""
	fileURLParts.DirectoryOrFilePath = ""

	client, err := fileservice.NewClientWithNoCredential(fileURLParts.String(), nil)
	a.Nil(err)

	return client
}

func (scenarioHelper) getFileServiceClientWithSAS(a *assert.Assertions) *fileservice.Client {
	accountName, accountKey := getAccountAndKey()
	credential, err := sharefile.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	return getFileServiceClientWithSAS(a, credential)
}

func (scenarioHelper) getDatalakeServiceClientWithSAS(a *assert.Assertions) *datalakeservice.Client {
	accountName, accountKey := getAccountAndKey()
	credential, err := azdatalake.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	return getDatalakeServiceClientWithSAS(a, credential)
}

func (scenarioHelper) getBlobServiceClient(a *assert.Assertions) *blobservice.Client {
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net", credential.AccountName())

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	a.Nil(err)

	return client
}

func (s scenarioHelper) getContainerClient(a *assert.Assertions, containerName string) *container.Client {
	serviceURL := s.getBlobServiceClient(a)
	containerURL := serviceURL.NewContainerClient(containerName)

	return containerURL
}

func (scenarioHelper) getRawS3AccountURL(a *assert.Assertions, region string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com", common.Iff(region == "", "", "-"+region))

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return *fullURL
}

func (scenarioHelper) getRawGCPAccountURL(a *assert.Assertions) url.URL {
	rawURL := "https://storage.cloud.google.com/"
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)
	return *fullURL
}

// TODO: Possibly add virtual-hosted-style and dual stack support. Currently use path style for testing.
func (scenarioHelper) getRawS3BucketURL(a *assert.Assertions, region string, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s", common.Iff(region == "", "", "-"+region), bucketName)

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return *fullURL
}

func (scenarioHelper) getRawGCPBucketURL(a *assert.Assertions, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://storage.cloud.google.com/%s", bucketName)
	fmt.Println(rawURL)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)
	return *fullURL
}

func (scenarioHelper) getRawS3ObjectURL(a *assert.Assertions, region string, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s/%s", common.Iff(region == "", "", "-"+region), bucketName, objectName)

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return *fullURL
}

func (scenarioHelper) getRawGCPObjectURL(a *assert.Assertions, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://storage.cloud.google.com/%s/%s", bucketName, objectName)
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)
	return *fullURL
}

func (scenarioHelper) getRawFileURLWithSAS(a *assert.Assertions, shareName string, fileName string) *url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := sharefile.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	sc := getShareClientWithSAS(a, credential, shareName)
	fc := sc.NewRootDirectoryClient().NewFileClient(fileName)

	u := fc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getRawShareURLWithSAS(a *assert.Assertions, shareName string) *url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := sharefile.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	sc := getShareClientWithSAS(a, credential, shareName)

	u := sc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) getRawFileServiceURLWithSAS(a *assert.Assertions) *url.URL {
	accountName, accountKey := getAccountAndKey()
	credential, err := sharefile.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	sc := getFileServiceClientWithSAS(a, credential)

	u := sc.URL()
	parsedURL, err := url.Parse(u)
	return parsedURL
}

func (scenarioHelper) blobExists(blobClient *blob.Client) bool {
	_, err := blobClient.GetProperties(context.Background(), nil)
	if err == nil {
		return true
	}
	return false
}

func (scenarioHelper) containerExists(containerClient *container.Client) bool {
	_, err := containerClient.GetProperties(context.Background(), nil)
	if err == nil {
		return true
	}
	return false
}

func runSyncAndVerify(a *assert.Assertions, raw rawSyncCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	a.Nil(err)

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func runCopyAndVerify(a *assert.Assertions, raw rawCopyCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	if err == nil {
		err = cooked.makeTransferEnum()
	}
	if err != nil {
		verifier(err)
		return
	}

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func validateUploadTransfersAreScheduled(a *assert.Assertions, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(a, false, true, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateDownloadTransfersAreScheduled(a *assert.Assertions, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(a, true, false, sourcePrefix, destinationPrefix, expectedTransfers, mockedRPC)
}

func validateS2SSyncTransfersAreScheduled(a *assert.Assertions, expectedTransfers []string, mockedRPC interceptor) {
	validateCopyTransfersAreScheduled(a, true, true, common.AZCOPY_PATH_SEPARATOR_STRING, common.AZCOPY_PATH_SEPARATOR_STRING, expectedTransfers, mockedRPC)
}

func validateCopyTransfersAreScheduled(a *assert.Assertions, isSrcEncoded bool, isDstEncoded bool, sourcePrefix string, destinationPrefix string, expectedTransfers []string, mockedRPC interceptor) {
	// validate that the right number of transfers were scheduled
	a.Equal(len(expectedTransfers), len(mockedRPC.transfers))

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
				invalidChars := `<>\/:"|?*` + string(rune(0x00))

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
		a.Equal(dstRelativeFilePath, srcRelativeFilePath)

		// look up the path from the expected transfers, make sure it exists
		_, transferExist := lookupMap[srcRelativeFilePath]
		a.True(transferExist)
	}
}

func validateRemoveTransfersAreScheduled(a *assert.Assertions, isSrcEncoded bool, expectedTransfers []string, mockedRPC interceptor) {

	// validate that the right number of transfers were scheduled
	a.Equal(len(expectedTransfers), len(mockedRPC.transfers))

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		srcRelativeFilePath := strings.TrimPrefix(transfer.Source, "/")

		if isSrcEncoded {
			srcRelativeFilePath, _ = url.PathUnescape(srcRelativeFilePath)
		}

		// look up the source from the expected transfers, make sure it exists
		_, srcExist := lookupMap[srcRelativeFilePath]
		a.True(srcExist, srcRelativeFilePath)

		delete(lookupMap, srcRelativeFilePath)
	}
	// if len(lookupMap) > 0 {
	//	panic("set breakpoint here to debug")
	// }
}

func getDefaultSyncRawInput(sra, dst string) rawSyncCmdArgs {
	deleteDestination := common.EDeleteDestination.True()

	return rawSyncCmdArgs{
		src:                  sra,
		dst:                  dst,
		recursive:            true,
		deleteDestination:    deleteDestination.String(),
		md5ValidationOption:  common.DefaultHashValidationOption.String(),
		compareHash:          common.ESyncHashType.None().String(),
		localHashStorageMode: common.EHashStorageMode.Default().String(),
	}
}

func getDefaultCopyRawInput(src string, dst string) rawCopyCmdArgs {
	return rawCopyCmdArgs{
		src:                            src,
		dst:                            dst,
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		asSubdir:                       true,
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

func getDefaultSetPropertiesRawInput(src string, params transferParams) rawCopyCmdArgs {
	fromTo := common.EFromTo.BlobNone()
	srcURL, _ := url.Parse(src)

	srcLocationType := InferArgumentLocation(src)
	switch srcLocationType {
	case common.ELocation.Blob():
		fromTo = common.EFromTo.BlobNone()
	case common.ELocation.BlobFS():
		fromTo = common.EFromTo.BlobFSNone()
	case common.ELocation.File():
		fromTo = common.EFromTo.FileNone()
	default:
		panic(fmt.Sprintf("invalid source type %s to delete. azcopy support removing blobs/files/adls gen2", srcLocationType.String()))

	}

	if strings.Contains(srcURL.Host, "file") {
		fromTo = common.EFromTo.FileNone()
	} else if strings.Contains(srcURL.Host, "dfs") {
		fromTo = common.EFromTo.BlobFSNone()
	}

	rawArgs := rawCopyCmdArgs{
		src:                            src,
		fromTo:                         fromTo.String(),
		blobType:                       common.EBlobType.Detect().String(),
		blockBlobTier:                  common.EBlockBlobTier.None().String(),
		pageBlobTier:                   common.EPageBlobTier.None().String(),
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		includeDirectoryStubs:          true,
	}

	if params.blockBlobTier != common.EBlockBlobTier.None() {
		rawArgs.blockBlobTier = params.blockBlobTier.String()
	}
	if params.pageBlobTier != common.EPageBlobTier.None() {
		rawArgs.pageBlobTier = params.pageBlobTier.String()
	}
	if params.metadata != "" {
		rawArgs.metadata = params.metadata
	}
	if params.blobTags != nil {
		rawArgs.blobTags = params.blobTags.ToString()
	}

	return rawArgs
}
