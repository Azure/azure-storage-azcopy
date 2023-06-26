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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"google.golang.org/api/iterator"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/minio/minio-go"
)

var ctx = context.Background()

const (
	containerPrefix      = "container"
	blobPrefix           = "blob"
	blockBlobDefaultData = "AzCopy Random Test Data"
	// 512 bytes of alphanumeric random data
	pageBlobDefaultData   = "lEYvPHhS2c9T7DDNtM7f0gccgbqe7DMYByLj7d1XS6jV5Y0Cuiz5i86e5llkBwzCahnR4n1MUvfpniNBxgRgJ4oNk8oaIlCevtsPaCZgOMpKdPohp7yYTfawiz8MtHlTwM8OmfgngbH2BNiqtSFEx9GArvkwkVF0dPoG6RRBug0BqHiWyMd0mZifrBTneG13bqKg7A8EjRmBHIqCMGoxOYo1ufojJjYKiv8dfBYGib4pNpfrcxlEWrMKEPcgs3YG3AGg2lIKrMVs7yWnSzwqeEnl9oMFjdwc7XB2e7y2IH1JLt8CzaYgW6qvaPzhFXWbUkIJ6KznQAaKExJt9my625REjn8G4WT5tfo82J2gpdJNAveaF1O09Irjb93Yg07CfeSOrUBo4WwORrfJ60O4nc3MWWvHT2CsJ4b3MtjtVR0nb084SQpRycXPSF9rMympZrwmP0mutBYCVOEWDjsaLOQJoHo2UOiBD2sM5rm4N5mqt0mEInyGO8pKnV7NKn0N"
	appendBlobDefaultData = "AzCopy Random Append Test Data"

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

func skipIfS3Disabled(t *testing.T) {
	if isS3Disabled() {
		t.Skip("S3 testing is disabled for this unit test suite run.")
	}
}

// If TEST_GCP == True, we'll run GCP testcases
func gcpTestsDisabled() bool {
	return strings.ToLower(os.Getenv("GCP_TESTS_OFF")) != ""
}

func skipIfGCPDisabled(t *testing.T) {
	if gcpTestsDisabled() {
		t.Skip("GCP testing is disabled for this run")
	}
}

func testDryrunStatements(items, messages []string) bool {
	for _, v := range items {
		for _, m := range messages {
			if strings.HasSuffix(m, v) {
				goto continueBlobs
			}
		}

		return false

	continueBlobs:
	}

	return true
}

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Will truncate the end of the test name, if there is not enough room for it, followed by the time-based suffix,
// with a non-zero maxLen.
func generateName(prefix string, maxLen int) string {
	// The following lines step up the stack find the name of the test method
	// Note: the way to do this changed in go 1.12, refer to release notes for more info
	var pcs [10]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	name := "TestFoo" // default stub "Foo" is used if anything goes wrong with this procedure
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Func.Name(), "Test") {
			name = frame.Func.Name()
			break
		} else if !more {
			break
		}
	}
	funcNameStart := strings.Index(name, "Test")
	name = name[funcNameStart+len("Test"):] // Just get the name of the test and not any of the garbage at the beginning
	name = strings.ToLower(name)            // Ensure it is a valid resource name
	textualPortion := fmt.Sprintf("%s%s", prefix, strings.ToLower(name))
	currentTime := time.Now()
	numericSuffix := fmt.Sprintf("%02d%02d%d", currentTime.Minute(), currentTime.Second(), currentTime.Nanosecond())
	if maxLen > 0 {
		maxTextLen := maxLen - len(numericSuffix)
		if maxTextLen < 1 {
			panic("max len too short")
		}
		if len(textualPortion) > maxTextLen {
			textualPortion = textualPortion[:maxTextLen]
		}
	}
	name = textualPortion + numericSuffix
	return name
}

func generateContainerName() string {
	return generateName(containerPrefix, 63)
}

func generateBlobName() string {
	return generateName(blobPrefix, 0)
}

func generateBucketName() string {
	return generateName(bucketPrefix, 63)
}

func generateBucketNameWithCustomizedPrefix(customizedPrefix string) string {
	return generateName(customizedPrefix, 63)
}

func generateObjectName() string {
	return generateName(objectPrefix, 0)
}

func generateShareName() string {
	return generateName(sharePrefix, 63)
}

func generateFilesystemName() string {
	return generateName(blobfsPrefix, 63)
}

func getShareURL(a *assert.Assertions, fsu azfile.ServiceURL) (share azfile.ShareURL, name string) {
	name = generateShareName()
	share = fsu.NewShareURL(name)

	return share, name
}

func getShareClient(a *assert.Assertions, fsc *fileservice.Client) (share *share.Client, name string) {
	name = generateShareName()
	share = fsc.NewShareClient(name)

	return share, name
}

func generateAzureFileName() string {
	return generateName(azureFilePrefix, 0)
}

func generateBfsFileName() string {
	return generateName(blobfsPrefix, 0)
}

func getContainerClient(a *assert.Assertions, bsc *blobservice.Client) (container *container.Client, name string) {
	name = generateContainerName()
	container = bsc.NewContainerClient(name)
	return
}

func getFilesystemURL(a *assert.Assertions, bfssu azbfs.ServiceURL) (filesystem azbfs.FileSystemURL, name string) {
	name = generateFilesystemName()
	filesystem = bfssu.NewFileSystemURL(name)

	return
}

func getBlockBlobClient(a *assert.Assertions, cc *container.Client, prefix string) (bbc *blockblob.Client, name string) {
	name = prefix + generateBlobName()
	bbc = cc.NewBlockBlobClient(name)
	return
}

func getBfsFileURL(a *assert.Assertions, filesystemURL azbfs.FileSystemURL, prefix string) (file azbfs.FileURL, name string) {
	name = prefix + generateBfsFileName()
	file = filesystemURL.NewRootDirectoryURL().NewFileURL(name)

	return
}

func getAppendBlobClient(a *assert.Assertions, cc *container.Client, prefix string) (abc *appendblob.Client, name string) {
	name = generateBlobName()
	abc = cc.NewAppendBlobClient(prefix + name)
	return
}

func getPageBlobClient(a *assert.Assertions, cc *container.Client, prefix string) (pbc *pageblob.Client, name string) {
	name = generateBlobName()
	pbc = cc.NewPageBlobClient(prefix + name)
	return
}

func getAzureFileURL(a *assert.Assertions, shareURL azfile.ShareURL, prefix string) (fileURL azfile.FileURL, name string) {
	name = prefix + generateAzureFileName()
	fileURL = shareURL.NewRootDirectoryURL().NewFileURL(name)

	return
}

func getReaderToRandomBytes(n int) *bytes.Reader {
	r, _ := getRandomDataAndReader(n)
	return r
}

func getRandomDataAndReader(n int) (*bytes.Reader, []byte) {
	data := make([]byte, n, n)
	rand.Read(data)
	return bytes.NewReader(data), data
}

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

// get blob account service client
func getBlobServiceClient() *blobservice.Client {
	accountName, accountKey := getAccountAndKey()
	u := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	client, err := blobservice.NewClientWithSharedKeyCredential(u, credential, nil)
	if err != nil {
		panic(err)
	}
	return client
}

// get file account service client
func getFileServiceClient() *fileservice.Client {
	accountName, accountKey := getAccountAndKey()
	u := fmt.Sprintf("https://%s.file.core.windows.net/", accountName)

	credential, err := sharefile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	client, err := fileservice.NewClientWithSharedKeyCredential(u, credential, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func getFSU() azfile.ServiceURL {
	accountName, accountKey := getAccountAndKey()
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/", accountName))

	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	pipeline := azfile.NewPipeline(credential, azfile.PipelineOptions{})
	return azfile.NewServiceURL(*u, pipeline)
}

func GetBFSSU() azbfs.ServiceURL {
	accountName, accountKey := getAccountAndKey()
	u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/", accountName))

	cred := azbfs.NewSharedKeyCredential(accountName, accountKey)
	pipeline := azbfs.NewPipeline(cred, azbfs.PipelineOptions{})
	return azbfs.NewServiceURL(*u, pipeline)
}

func createNewContainer(a *assert.Assertions, bsc *blobservice.Client) (cc *container.Client, name string) {
	cc, name = getContainerClient(a, bsc)

	// ignore any errors here, since it doesn't matter if this fails (if it does, it's probably because the container didn't exist)
	_, _ = cc.Delete(ctx, nil)

	_, err := cc.Create(ctx, nil)
	a.Nil(err)
	return cc, name
}

func createNewFilesystem(a *assert.Assertions, bfssu azbfs.ServiceURL) (filesystem azbfs.FileSystemURL, name string) {
	filesystem, name = getFilesystemURL(a, bfssu)

	// ditto
	_, _ = filesystem.Delete(ctx)

	_, err := filesystem.Create(ctx)
	a.Nil(err)
	return
}

func createNewBfsFile(a *assert.Assertions, filesystem azbfs.FileSystemURL, prefix string) (file azbfs.FileURL, name string) {
	file, name = getBfsFileURL(a, filesystem, prefix)

	// Create the file
	_, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	a.Nil(err)

	_, err = file.AppendData(ctx, 0, strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes))))
	a.Nil(err)

	_, err = file.FlushData(ctx, defaultBlobFSFileSizeInBytes, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
	a.Nil(err)
	return
}

func createNewBlockBlob(a *assert.Assertions, cc *container.Client, prefix string) (bbc *blockblob.Client, name string) {
	bbc, name = getBlockBlobClient(a, cc, prefix)

	_, err := bbc.Upload(ctx, streaming.NopCloser(strings.NewReader(blockBlobDefaultData)), nil)
	a.Nil(err)

	return
}

// create metadata indicating that this is a dir
func createNewDirectoryStub(a *assert.Assertions, cc *container.Client, dirPath string) {
	dirClient := cc.NewBlockBlobClient(dirPath)

	_, err := dirClient.Upload(ctx, streaming.NopCloser(bytes.NewReader(nil)),
		&blockblob.UploadOptions{
			Metadata: map[string]*string{"hdi_isfolder": to.Ptr("true")},
		})
	a.Nil(err)

	return
}

func createNewAzureShare(a *assert.Assertions, fsu azfile.ServiceURL) (share azfile.ShareURL, name string) {
	share, name = getShareURL(a, fsu)

	_, err := share.Create(ctx, nil, 0)
	a.Nil(err)

	return share, name
}

func createNewAzureFile(a *assert.Assertions, share azfile.ShareURL, prefix string) (file azfile.FileURL, name string) {
	file, name = getAzureFileURL(a, share, prefix)

	// generate parents first
	generateParentsForAzureFile(a, file)

	_, err := file.Create(ctx, defaultAzureFileSizeInBytes, azfile.FileHTTPHeaders{}, azfile.Metadata{})
	a.Nil(err)

	return
}

func createNewShare(a *assert.Assertions, fsc *fileservice.Client) (sc *share.Client, name string) {
	sc, name = getShareClient(a, fsc)

	_, err := sc.Create(ctx, nil)
	a.Nil(err)

	return sc, name
}

func generateParentsForShareFile(a *assert.Assertions, fileClient *sharefile.Client, serviceClient *fileservice.Client) {
	t := ste.NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), nil)
	err := ste.AzureFileParentDirCreator{}.CreateParentDirToRoot(ctx, fileClient, serviceClient, t)
	a.Nil(err)
}

func generateParentsForAzureFile(a *assert.Assertions, fileURL azfile.FileURL) {
	accountName, accountKey := getAccountAndKey()
	credential, _ := azfile.NewSharedKeyCredential(accountName, accountKey)
	t := ste.NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), nil)
	err := ste.AzureFileParentDirCreator{}.CreateParentDirToRootV1(ctx, fileURL, azfile.NewPipeline(credential, azfile.PipelineOptions{}), t)
	a.Nil(err)
}

func createNewAppendBlob(a *assert.Assertions, cc *container.Client, prefix string) (abc *appendblob.Client, name string) {
	abc, name = getAppendBlobClient(a, cc, prefix)

	_, err := abc.Create(ctx, nil)
	a.Nil(err)

	return
}

func createNewPageBlob(a *assert.Assertions, cc *container.Client, prefix string) (pbc *pageblob.Client, name string) {
	pbc, name = getPageBlobClient(a, cc, prefix)

	_, err := pbc.Create(ctx, pageblob.PageBytes*10, nil)
	a.Nil(err)

	return
}

func deleteContainer(a *assert.Assertions, cc *container.Client) {
	_, err := cc.Delete(ctx, nil)
	a.Nil(err)
}

func deleteFilesystem(a *assert.Assertions, filesystem azbfs.FileSystemURL) {
	_, err := filesystem.Delete(ctx)
	a.Nil(err)
}

func validateStorageError(a *assert.Assertions, err error, code bloberror.Code) {
	a.True(bloberror.HasCode(err, code))
}

func getRelativeTimeGMT(amount time.Duration) time.Time {
	currentTime := time.Now().In(time.FixedZone("GMT", 0))
	currentTime = currentTime.Add(amount * time.Second)
	return currentTime
}

func generateCurrentTimeWithModerateResolution() time.Time {
	highResolutionTime := time.Now().UTC()
	return time.Date(highResolutionTime.Year(), highResolutionTime.Month(), highResolutionTime.Day(), highResolutionTime.Hour(), highResolutionTime.Minute(),
		highResolutionTime.Second(), 0, highResolutionTime.Location())
}

type createS3ResOptions struct {
	Location string
}

func createS3ClientWithMinio(o createS3ResOptions) (*minio.Client, error) {
	if isS3Disabled() {
		return nil, errors.New("s3 testing is disabled")
	}

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should be set before creating the S3 client")
	}

	s3Client, err := minio.NewWithRegion("s3.amazonaws.com", accessKeyID, secretAccessKey, true, o.Location)
	if err != nil {
		return nil, err
	}
	return s3Client, nil
}

func createGCPClientWithGCSSDK() (*gcpUtils.Client, error) {
	jsonKey := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if jsonKey == "" {
		return nil, fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS should be set before creating the GCP Client")
	}
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		return nil, fmt.Errorf("GOOGLE_CLOUD_PROJECT should be set before creating GCP Client for testing")
	}
	ctx := context.Background()
	gcpClient, err := gcpUtils.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return gcpClient, nil
}

func createNewBucket(a *assert.Assertions, client *minio.Client, o createS3ResOptions) string {
	bucketName := generateBucketName()
	err := client.MakeBucket(bucketName, o.Location)
	a.Nil(err)

	return bucketName
}

func createNewGCPBucket(a *assert.Assertions, client *gcpUtils.Client) string {
	bucketName := generateBucketName()
	bkt := client.Bucket(bucketName)
	err := bkt.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
	a.Nil(err)

	return bucketName
}

func createNewBucketWithName(a *assert.Assertions, client *minio.Client, bucketName string, o createS3ResOptions) {
	err := client.MakeBucket(bucketName, o.Location)
	a.Nil(err)
}

func createNewGCPBucketWithName(a *assert.Assertions, client *gcpUtils.Client, bucketName string) {
	bucket := client.Bucket(bucketName)
	err := bucket.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
	a.Nil(err)
}

func createNewObject(a *assert.Assertions, client *minio.Client, bucketName string, prefix string) (objectKey string) {
	objectKey = prefix + generateObjectName()

	size := int64(len(objectDefaultData))
	n, err := client.PutObject(bucketName, objectKey, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
	a.Nil(err)

	a.Equal(size, n)

	return
}

func createNewGCPObject(a *assert.Assertions, client *gcpUtils.Client, bucketName string, prefix string) (objectKey string) {
	objectKey = prefix + generateObjectName()

	size := int64(len(objectDefaultData))
	wc := client.Bucket(bucketName).Object(objectKey).NewWriter(context.Background())
	reader := strings.NewReader(objectDefaultData)
	written, err := io.Copy(wc, reader)
	a.Nil(err)
	a.Equal(size, written)
	err = wc.Close()
	a.Nil(err)
	return objectKey

}

func deleteBucket(client *minio.Client, bucketName string, waitQuarterMinute bool) {
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

func deleteGCPBucket(client *gcpUtils.Client, bucketName string, waitQuarterMinute bool) {
	bucket := client.Bucket(bucketName)
	ctx := context.Background()
	it := bucket.Objects(ctx, &gcpUtils.Query{Prefix: ""})
	for {
		attrs, err := it.Next()
		if err != nil { // if Next returns an error other than iterator.Done, all subsequent calls will return the same error.
			if err == iterator.Done {
				break
			}
			return
		}
		if err == nil {
			err = bucket.Object(attrs.Name).Delete(nil)
			if err != nil {
				return
			}
		}
	}
	err := bucket.Delete(context.Background())
	if err != nil {
		fmt.Println(fmt.Sprintf("Failed to Delete GCS Bucket %v", bucketName))
	}

	if waitQuarterMinute {
		time.Sleep(time.Second * 15)
	}
}

func cleanS3Account(client *minio.Client) {
	buckets, err := client.ListBuckets()
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		if strings.Contains(bucket.Name, "elastic") {
			continue
		}
		deleteBucket(client, bucket.Name, false)
	}

	time.Sleep(time.Minute)
}

func cleanGCPAccount(client *gcpUtils.Client) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		fmt.Println("GOOGLE_CLOUD_PROJECT env variable not set. GCP tests will not run")
		return
	}
	ctx := context.Background()
	it := client.Buckets(ctx, projectID)
	for {
		battrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return
		}
		deleteGCPBucket(client, battrs.Name, false)
	}
}

func cleanBlobAccount(a *assert.Assertions, serviceClient *blobservice.Client) {
	pager := serviceClient.NewListContainersPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		a.Nil(err)

		for _, v := range resp.ContainerItems {
			_, err = serviceClient.NewContainerClient(*v.Name).Delete(ctx, nil)

			if err != nil {
				var respErr *azcore.ResponseError
				if errors.As(err, &respErr) {
					if respErr.ErrorCode == string(bloberror.ContainerNotFound) {
						continue
					}
				}

				a.Nil(err)
			}
		}
	}
}

func cleanFileAccount(a *assert.Assertions, serviceClient *fileservice.Client) {
	pager := serviceClient.NewListSharesPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		a.Nil(err)

		for _, v := range resp.Shares {
			_, err = serviceClient.NewShareClient(*v.Name).Delete(ctx, nil)

			if err != nil {
				var respErr *azcore.ResponseError
				if errors.As(err, &respErr) {
					if respErr.ErrorCode == string(fileerror.ShareNotFound) {
						continue
					}
				}

				a.Nil(err)
			}
		}
	}

	time.Sleep(time.Minute)
}

func getGenericCredentialForFile(accountType string) (*azfile.SharedKeyCredential, error) {
	accountNameEnvVar := accountType + "ACCOUNT_NAME"
	accountKeyEnvVar := accountType + "ACCOUNT_KEY"
	accountName, accountKey := os.Getenv(accountNameEnvVar), os.Getenv(accountKeyEnvVar)
	if accountName == "" || accountKey == "" {
		return nil, errors.New(accountNameEnvVar + " and/or " + accountKeyEnvVar + " environment variables not specified.")
	}
	return azfile.NewSharedKeyCredential(accountName, accountKey)
}

func getAlternateFSU() (azfile.ServiceURL, error) {
	secondaryAccountName, secondaryAccountKey := os.Getenv("SECONDARY_ACCOUNT_NAME"), os.Getenv("SECONDARY_ACCOUNT_KEY")
	if secondaryAccountName == "" || secondaryAccountKey == "" {
		return azfile.ServiceURL{}, errors.New("SECONDARY_ACCOUNT_NAME and/or SECONDARY_ACCOUNT_KEY environment variables not specified.")
	}
	fsURL, _ := url.Parse("https://" + secondaryAccountName + ".file.core.windows.net/")

	credential, err := azfile.NewSharedKeyCredential(secondaryAccountName, secondaryAccountKey)
	if err != nil {
		return azfile.ServiceURL{}, err
	}
	pipeline := azfile.NewPipeline(credential, azfile.PipelineOptions{ /*Log: pipeline.NewLogWrapper(pipeline.LogInfo, log.New(os.Stderr, "", log.LstdFlags))*/ })

	return azfile.NewServiceURL(*fsURL, pipeline), nil
}

func deleteShare(a *assert.Assertions, sc *share.Client) {
	_, err := sc.Delete(ctx, &share.DeleteOptions{DeleteSnapshots: to.Ptr(share.DeleteSnapshotsOptionTypeInclude)})
	a.Nil(err)
}

func deleteShareV1(a *assert.Assertions, share azfile.ShareURL) {
	_, err := share.Delete(ctx, azfile.DeleteSnapshotsOptionInclude)
	a.Nil(err)
}

// Some tests require setting service properties. It can take up to 30 seconds for the new properties to be reflected across all FEs.
// We will enable the necessary property and try to run the test implementation. If it fails with an error that should be due to
// those changes not being reflected yet, we will wait 30 seconds and try the test again. If it fails this time for any reason,
// we fail the test. It is the responsibility of the the testImplFunc to determine which error string indicates the test should be retried.
// There can only be one such string. All errors that cannot be due to this detail should be asserted and not returned as an error string.
func runTestRequiringServiceProperties(a *assert.Assertions, bsc *blobservice.Client, code string,
	enableServicePropertyFunc func(*assert.Assertions, *blobservice.Client),
	testImplFunc func(*assert.Assertions, *blobservice.Client) error,
	disableServicePropertyFunc func(*assert.Assertions, *blobservice.Client)) {
	enableServicePropertyFunc(a, bsc)
	defer disableServicePropertyFunc(a, bsc)
	err := testImplFunc(a, bsc)
	// We cannot assume that the error indicative of slow update will necessarily be a StorageError. As in ListBlobs.
	if err != nil && err.Error() == code {
		time.Sleep(time.Second * 30)
		err = testImplFunc(a, bsc)
		a.Nil(err)
	}
}

func enableSoftDelete(a *assert.Assertions, bsc *blobservice.Client) {
	_, err := bsc.SetProperties(ctx, &blobservice.SetPropertiesOptions{
		DeleteRetentionPolicy: &blobservice.RetentionPolicy{Enabled: to.Ptr(true), Days: to.Ptr(int32(1))},
	})
	a.Nil(err)
}

func disableSoftDelete(a *assert.Assertions, bsc *blobservice.Client) {
	_, err := bsc.SetProperties(ctx, &blobservice.SetPropertiesOptions{
		DeleteRetentionPolicy: &blobservice.RetentionPolicy{Enabled: to.Ptr(false)},
	})
	a.Nil(err)
}

func validateUpload(a *assert.Assertions, bbc *blockblob.Client) {
	resp, err := bbc.DownloadStream(ctx, nil)
	a.Nil(err)
	data, _ := io.ReadAll(resp.Body)
	a.Len(data, 0)
}

func getContainerClientWithSAS(a *assert.Assertions, credential *blob.SharedKeyCredential, containerName string) *container.Client {
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s",
		credential.AccountName(), containerName)
	client, err := container.NewClientWithSharedKeyCredential(rawURL, credential, nil)

	sasURL, err := client.GetSASURL(
		blobsas.ContainerPermissions{Read: true, Add: true, Write: true, Create: true, Delete: true, DeletePreviousVersion: true, List: true, Tag: true},
		time.Now().Add(48*time.Hour),
		nil)
	a.Nil(err)

	client, err = container.NewClientWithNoCredential(sasURL, nil)
	a.Nil(err)

	return client
}

func getBlobServiceClientWithSAS(a *assert.Assertions, credential *blob.SharedKeyCredential) *blobservice.Client {
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/",
		credential.AccountName())
	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)

	sasURL, err := client.GetSASURL(
		blobsas.AccountResourceTypes{Service: true, Container: true, Object: true},
		blobsas.AccountPermissions{Read: true, List: true, Write: true, Delete: true, DeletePreviousVersion: true, Add: true, Create: true, Update: true, Process: true, Tag: true},
		time.Now().Add(48*time.Hour),
		nil)
	a.Nil(err)

	client, err = blobservice.NewClientWithNoCredential(sasURL, nil)
	a.Nil(err)

	return client
}

func getFileServiceClientWithSAS(a *assert.Assertions, credential *sharefile.SharedKeyCredential) *fileservice.Client {
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/",
		credential.AccountName())
	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)

	sasURL, err := client.GetSASURL(
		filesas.AccountResourceTypes{Service: true, Container: true, Object: true},
		filesas.AccountPermissions{Read: true, List: true, Write: true, Delete: true, Create: true},
		time.Now().Add(48*time.Hour),
		nil)
	a.Nil(err)

	client, err = fileservice.NewClientWithNoCredential(sasURL, nil)
	a.Nil(err)

	return client
}

func getShareClientWithSAS(a *assert.Assertions, credential *sharefile.SharedKeyCredential, shareName string) *share.Client {
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/%s",
		credential.AccountName(), shareName)
	client, err := share.NewClientWithSharedKeyCredential(rawURL, credential, nil)

	sasURL, err := client.GetSASURL(
		filesas.SharePermissions{Read: true, Write: true, Create: true, Delete: true, List: true},
		time.Now().Add(48*time.Hour),
		nil)
	a.Nil(err)

	client, err = share.NewClientWithNoCredential(sasURL, nil)
	a.Nil(err)

	return client
}

func getFileServiceURLWithSAS(a *assert.Assertions, credential azfile.SharedKeyCredential) azfile.ServiceURL {
	sasQueryParams, err := azfile.AccountSASSignatureValues{
		Protocol:      azfile.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azfile.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(&credential)
	a.Nil(err)

	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/?%s", credential.AccountName(), qp)

	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return azfile.NewServiceURL(*fullURL, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
}

func getShareURLWithSAS(a *assert.Assertions, credential azfile.SharedKeyCredential, shareName string) azfile.ShareURL {
	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   shareName,
		Permissions: azfile.ShareSASPermissions{Read: true, Write: true, Create: true, Delete: true, List: true}.String(),
	}.NewSASQueryParameters(&credential)
	a.Nil(err)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/%s?%s",
		credential.AccountName(), shareName, qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	// TODO perhaps we need a global default pipeline
	return azfile.NewShareURL(*fullURL, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
}

func getAdlsServiceURLWithSAS(a *assert.Assertions, credential azbfs.SharedKeyCredential) azbfs.ServiceURL {
	sasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azfile.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(&credential)
	a.Nil(err)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.dfs.core.windows.net/?%s",
		credential.AccountName(), qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	a.Nil(err)

	return azbfs.NewServiceURL(*fullURL, azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))
}