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

// TODO this file was forked from the cmd package, it needs to cleaned to keep only the necessary part

package e2etest

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"io"
	"math/rand"
	"mime"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/minio/minio-go/pkg/credentials"
	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/minio/minio-go"
)

var ctx = context.Background()

const (
	blockBlobDefaultData = "AzCopy Random Test Data"

	bucketPrefix      = "s3bucket"
	objectPrefix      = "s3object"
	objectDefaultData = "AzCopy default data for S3 object"

	fileDefaultData             = "AzCopy Random Test Data"
	sharePrefix                 = "share"
	azureFilePrefix             = "azfile"
	defaultAzureFileSizeInBytes = 1000

	blobfsPrefix                 = "blobfs"
	defaultBlobFSFileSizeInBytes = 1000
)

// if S3_TESTS_OFF is set at all, S3 tests are disabled.
func isS3Disabled() bool {
	return strings.ToLower(os.Getenv("S3_TESTS_OFF")) != ""
}

func skipIfS3Disabled(c asserter) {
	if isS3Disabled() {
		c.Skip("S3 testing is disabled for this unit test suite run.")
	}
}

func generateContainerName(c asserter) string {
	return generateName(c, containerPrefix, 63)
}

func generateBlobName(c asserter) string {
	return generateName(c, blobPrefix, 0)
}

func generateBucketName(c asserter) string {
	return generateName(c, bucketPrefix, 63)
}

func generateBucketNameWithCustomizedPrefix(c asserter, customizedPrefix string) string {
	return generateName(c, customizedPrefix, 63)
}

func generateObjectName(c asserter) string {
	return generateName(c, objectPrefix, 0)
}

func generateShareName(c asserter) string {
	return generateName(c, sharePrefix, 63)
}

func generateFilesystemName(c asserter) string {
	return generateName(c, blobfsPrefix, 63)
}

func getShareClient(c asserter, fsc *fileservice.Client) (sc *share.Client, name string) {
	name = generateShareName(c)
	sc = fsc.NewShareClient(name)
	return
}

func generateAzureFileName(c asserter) string {
	return generateName(c, azureFilePrefix, 0)
}

func generateBfsFileName(c asserter) string {
	return generateName(c, blobfsPrefix, 0)
}

func getContainerURL(c asserter, bsc *blobservice.Client) (cc *container.Client, name string) {
	name = generateContainerName(c)
	cc = bsc.NewContainerClient(name)
	return
}

func getFilesystemURL(c asserter, bfssu azbfs.ServiceURL) (filesystem azbfs.FileSystemURL, name string) {
	name = generateFilesystemName(c)
	filesystem = bfssu.NewFileSystemURL(name)

	return
}

func getBlockBlobURL(c asserter, cc *container.Client, prefix string) (bc *blockblob.Client, name string) {
	name = prefix + generateBlobName(c)
	bc = cc.NewBlockBlobClient(name)

	return bc, name
}

func getBfsFileURL(c asserter, filesystemURL azbfs.FileSystemURL, prefix string) (file azbfs.FileURL, name string) {
	name = prefix + generateBfsFileName(c)
	file = filesystemURL.NewRootDirectoryURL().NewFileURL(name)

	return
}

func getAppendBlobURL(c asserter, cc *container.Client, prefix string) (bc *appendblob.Client, name string) {
	name = generateBlobName(c)
	bc = cc.NewAppendBlobClient(prefix + name)
	return
}

func getPageBlobURL(c asserter, cc *container.Client, prefix string) (bc *pageblob.Client, name string) {
	name = generateBlobName(c)
	bc = cc.NewPageBlobClient(prefix + name)
	return
}

func getAzureFileURL(c asserter, sc *share.Client, prefix string) (fc *file.Client, name string) {
	name = prefix + generateAzureFileName(c)
	fc = sc.NewRootDirectoryClient().NewFileClient(name)
	return
}

// todo: consider whether to replace with common.NewRandomDataGenerator, which is
//    believed to be faster
func getRandomDataAndReader(n int) (io.ReadSeekCloser, []byte) {
	data := make([]byte, n)
	rand.Read(data)
	return streaming.NopCloser(bytes.NewReader(data)), data
}

func createNewContainer(c asserter, bsc *blobservice.Client) (cc *container.Client, name string) {
	cc, name = getContainerURL(c, bsc)

	_, err := cc.Create(ctx, nil)
	c.AssertNoErr(err)
	return
}

func createNewFilesystem(c asserter, bfssu azbfs.ServiceURL) (filesystem azbfs.FileSystemURL, name string) {
	filesystem, name = getFilesystemURL(c, bfssu)

	cResp, err := filesystem.Create(ctx)
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)
	return
}

func createNewBfsFile(c asserter, filesystem azbfs.FileSystemURL, prefix string) (file azbfs.FileURL, name string) {
	file, name = getBfsFileURL(c, filesystem, prefix)

	// Create the file
	cResp, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)

	aResp, err := file.AppendData(ctx, 0, strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes))))
	c.AssertNoErr(err)
	c.Assert(aResp.StatusCode(), equals(), 202)

	fResp, err := file.FlushData(ctx, defaultBlobFSFileSizeInBytes, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
	c.AssertNoErr(err)
	c.Assert(fResp.StatusCode(), equals(), 200)
	return
}

func createNewBlockBlob(c asserter, cc *container.Client, prefix string) (bc *blockblob.Client, name string) {
	bc, name = getBlockBlobURL(c, cc, prefix)

	_, err := bc.Upload(ctx, streaming.NopCloser(strings.NewReader(blockBlobDefaultData)), nil)
	c.AssertNoErr(err)

	return
}

func createNewAzureShare(c asserter, fsc *fileservice.Client) (sc *share.Client, name string) {
	sc, name = getShareClient(c, fsc)

	_, err := sc.Create(ctx, nil)
	c.AssertNoErr(err)

	return
}

func createNewAzureFile(c asserter, sc *share.Client, prefix string) (fc *file.Client, name string) {
	fc, name = getAzureFileURL(c, sc, prefix)

	// generate parents first
	generateParentsForAzureFile(c, fc)

	_, err := fc.Create(ctx, defaultAzureFileSizeInBytes, nil)
	c.AssertNoErr(err)

	return
}

func newNullFolderCreationTracker() ste.FolderCreationTracker {
	return ste.NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), nil)
}

func getFileServiceClient() *fileservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	u := fmt.Sprintf("https://%s.file.core.windows.net/", accountName)

	credential, err := fileservice.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	client, err := fileservice.NewClientWithSharedKeyCredential(u, credential, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func generateParentsForAzureFile(c asserter, fc *file.Client) {
	fsc := getFileServiceClient()
	err := ste.AzureFileParentDirCreator{}.CreateParentDirToRoot(ctx, fc, fsc, newNullFolderCreationTracker())
	c.AssertNoErr(err)
}

func createNewAppendBlob(c asserter, cc *container.Client, prefix string) (bc *appendblob.Client, name string) {
	bc, name = getAppendBlobURL(c, cc, prefix)

	_, err := bc.Create(ctx, nil)
	c.AssertNoErr(err)
	return
}

func createNewPageBlob(c asserter, cc *container.Client, prefix string) (bc *pageblob.Client, name string) {
	bc, name = getPageBlobURL(c, cc, prefix)

	_, err := bc.Create(ctx, pageblob.PageBytes*10, nil)
	c.AssertNoErr(err)
	return
}

func deleteContainer(c asserter, cc *container.Client) {
	_, err := cc.Delete(ctx, nil)
	c.AssertNoErr(err)
}

func deleteFilesystem(c asserter, filesystem azbfs.FileSystemURL) {
	resp, err := filesystem.Delete(ctx)
	c.AssertNoErr(err)
	c.Assert(resp.StatusCode(), equals(), 202)
}

type createS3ResOptions struct {
	Location string
}

func createS3ClientWithMinio(c asserter, o createS3ResOptions) (*minio.Client, error) {
	skipIfS3Disabled(c)

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKeyID == "" || secretAccessKey == "" {
		cred := credentials.NewStatic("", "", "", credentials.SignatureAnonymous)
		return minio.NewWithOptions("s3.amazonaws.com", &minio.Options{Creds: cred, Secure: true, Region: o.Location})
	}

	s3Client, err := minio.NewWithRegion("s3.amazonaws.com", accessKeyID, secretAccessKey, true, o.Location)
	if err != nil {
		return nil, err
	}
	return s3Client, nil
}

func createNewBucket(c asserter, client *minio.Client, o createS3ResOptions) string {
	bucketName := generateBucketName(c)
	err := client.MakeBucket(bucketName, o.Location)
	c.AssertNoErr(err)

	return bucketName
}

func createNewBucketWithName(c asserter, client *minio.Client, bucketName string, o createS3ResOptions) {
	err := client.MakeBucket(bucketName, o.Location)
	c.AssertNoErr(err)
}

func createNewObject(c asserter, client *minio.Client, bucketName string, prefix string) (objectKey string) {
	objectKey = prefix + generateObjectName(c)

	size := int64(len(objectDefaultData))
	n, err := client.PutObject(bucketName, objectKey, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
	c.AssertNoErr(err)

	c.Assert(n, equals(), size)

	return
}

func deleteBucket(_ asserter, client *minio.Client, bucketName string, waitQuarterMinute bool) {
	// If we error out in this function, simply just skip over deleting the bucket.
	// Some of our buckets have become "ghost" buckets in the past.
	// Ghost buckets show up in list calls but can't actually be interacted with.
	// Some ghost buckets are temporary, others are permanent.
	// As such, we need a way to deal with them when they show up.
	// By doing this, they'll just be cleaned up the next test run instead of failing all tests.
	objectsCh := make(chan string)

	go func() {
		defer close(objectsCh)

		// List all objects from a bucket-name with a matching prefix.
		for object := range client.ListObjectsV2(bucketName, "", true, context.Background().Done()) {
			if object.Err != nil {
				return
			}

			objectsCh <- object.Key
		}
	}()

	// List bucket, and delete all the objects in the bucket
	errChn := client.RemoveObjects(bucketName, objectsCh)
	var err error

	for rmObjErr := range errChn {
		if rmObjErr.Err != nil {
			return
		}
	}

	// Remove the bucket.
	err = client.RemoveBucket(bucketName)

	if err != nil {
		return
	}

	if waitQuarterMinute {
		time.Sleep(time.Second * 15)
	}
}

func cleanS3Account(c asserter, client *minio.Client) {
	buckets, err := client.ListBuckets()
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		if strings.Contains(bucket.Name, "elastic") {
			continue
		}
		deleteBucket(c, client, bucket.Name, false)
	}

	time.Sleep(time.Minute)
}

func cleanBlobAccount(c asserter, sc *blobservice.Client) {
	pager := sc.NewListContainersPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		c.AssertNoErr(err)

		for _, v := range resp.ContainerItems {
			_, err = sc.NewContainerClient(*v.Name).Delete(ctx, nil)
			c.AssertNoErr(err)
		}
	}
}

func cleanFileAccount(c asserter, sc *fileservice.Client) {
	pager := sc.NewListSharesPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		c.AssertNoErr(err)

		for _, v := range resp.Shares {
			_, err = sc.NewShareClient(*v.Name).Delete(ctx, nil)
			c.AssertNoErr(err)
		}
	}

	time.Sleep(time.Minute)
}

func deleteShare(c asserter, sc *share.Client) {
	_, err := sc.Delete(ctx, &share.DeleteOptions{DeleteSnapshots: to.Ptr(share.DeleteSnapshotsOptionTypeInclude)})
	c.AssertNoErr(err)
}

// Some tests require setting service properties. It can take up to 30 seconds for the new properties to be reflected across all FEs.
// We will enable the necessary property and try to run the test implementation. If it fails with an error that should be due to
// those changes not being reflected yet, we will wait 30 seconds and try the test again. If it fails this time for any reason,
// we fail the test. It is the responsibility of the the testImplFunc to determine which error string indicates the test should be retried.
// There can only be one such string. All errors that cannot be due to this detail should be asserted and not returned as an error string.
func runTestRequiringServiceProperties(c *chk.C, bsc *blobservice.Client, code string,
	enableServicePropertyFunc func(*chk.C, *blobservice.Client),
	testImplFunc func(*chk.C, *blobservice.Client) error,
	disableServicePropertyFunc func(*chk.C, *blobservice.Client)) {
	enableServicePropertyFunc(c, bsc)
	defer disableServicePropertyFunc(c, bsc)
	err := testImplFunc(c, bsc)
	// We cannot assume that the error indicative of slow update will necessarily be a StorageError. As in ListBlobs.
	if err != nil && err.Error() == code {
		time.Sleep(time.Second * 30)
		err = testImplFunc(c, bsc)
		c.Assert(err, chk.IsNil)
	}
}

func getContainerURLWithSAS(c asserter, credential *blob.SharedKeyCredential, containerName string) *container.Client {
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s",
		credential.AccountName(), containerName)
	cc, err := container.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	sasURL, err := cc.GetSASURL(blobsas.ContainerPermissions{Read: true, Add: true, Write: true, Create: true, Delete: true, List: true, Tag: true},
		time.Now().UTC().Add(48 * time.Hour), nil)
	c.AssertNoErr(err)

	cc, err = container.NewClientWithNoCredential(sasURL, nil)
	c.AssertNoErr(err)
	return cc
}

func getBlobServiceURLWithSAS(c asserter, credential *blob.SharedKeyCredential) *blobservice.Client {
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/",
		credential.AccountName())
	bsc, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	sasURL, err := bsc.GetSASURL(blobsas.AccountResourceTypes{Service: true, Container: true, Object: true},
		blobsas.AccountPermissions{Read: true, List: true, Write: true, Delete: true, DeletePreviousVersion: true, Add: true, Create: true, Update: true, Process: true},
		time.Now().UTC().Add(48 * time.Hour), nil)
	c.AssertNoErr(err)

	bsc, err = blobservice.NewClientWithNoCredential(sasURL, nil)
	c.AssertNoErr(err)
	return bsc
}

func getFileServiceURLWithSAS(c asserter, credential *file.SharedKeyCredential) *fileservice.Client {
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/",
		credential.AccountName())
	fsc, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	sasURL, err := fsc.GetSASURL(filesas.AccountResourceTypes{Service: true, Container: true, Object: true},
		filesas.AccountPermissions{Read: true, List: true, Write: true, Delete: true, Create: true},
		time.Now().UTC().Add(48 * time.Hour), nil)
	c.AssertNoErr(err)

	fsc, err = fileservice.NewClientWithNoCredential(sasURL, nil)
	c.AssertNoErr(err)
	return fsc
}

func getShareURLWithSAS(c asserter, credential *file.SharedKeyCredential, shareName string) *share.Client {
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/%s",
		credential.AccountName(), shareName)
	sc, err := share.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	sasURL, err := sc.GetSASURL(filesas.SharePermissions{Read: true, Write: true, Create: true, Delete: true, List: true},
		time.Now().UTC().Add(48 * time.Hour), nil)
	c.AssertNoErr(err)

	sc, err = share.NewClientWithNoCredential(sasURL, nil)
	c.AssertNoErr(err)
	return sc
}

func getAdlsServiceURLWithSAS(c asserter, credential azbfs.SharedKeyCredential) azbfs.ServiceURL {
	sasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   "rwdlacup",
		Services:      "bqf",
		ResourceTypes: "sco",
	}.NewSASQueryParameters(&credential)
	c.AssertNoErr(err)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/?%s",
		credential.AccountName(), qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return azbfs.NewServiceURL(*fullURL, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))
}

// check.v1 style "StringContains" checker
type stringContainsChecker struct {
	*chk.CheckerInfo
}

// Check
func (checker *stringContainsChecker) Check(params []interface{}, _ []string) (result bool, error string) {
	if len(params) < 2 {
		return false, "StringContains requires two parameters"
	} // Ignore extra parameters

	// Assert that params[0] and params[1] are strings
	aStr, aOK := params[0].(string)
	bStr, bOK := params[1].(string)
	if !aOK || !bOK {
		return false, "All parameters must be strings"
	}

	if strings.Contains(aStr, bStr) {
		return true, ""
	}

	return false, fmt.Sprintf("Failed to find substring in source string:\n\n"+
		"SOURCE: %s\n"+
		"EXPECTED: %s\n", aStr, bStr)
}

func GetContentTypeMap(fileExtensions []string) map[string]string {

	extensionsMap := make(map[string]string)
	for _, ext := range fileExtensions {
		if guessedType := mime.TypeByExtension(ext); guessedType != "" {
			extensionsMap[ext] = strings.Split(guessedType, ";")[0]
		}
	}
	return extensionsMap
}
