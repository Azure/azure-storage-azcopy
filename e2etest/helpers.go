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
	"errors"
	"fmt"
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

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
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
//nolint
func isS3Disabled() bool {
	return strings.ToLower(os.Getenv("S3_TESTS_OFF")) != ""
}

//nolint
func skipIfS3Disabled(c asserter) {
	if isS3Disabled() {
		c.Skip("S3 testing is disabled for this unit test suite run.")
	}
}

//nolint
func generateContainerName(c asserter) string {
	return generateName(c, containerPrefix, 63)
}

//nolint
func generateBlobName(c asserter) string {
	return generateName(c, blobPrefix, 0)
}

//nolint
func generateBucketName(c asserter) string {
	return generateName(c, bucketPrefix, 63)
}

//nolint
func generateBucketNameWithCustomizedPrefix(c asserter, customizedPrefix string) string {
	return generateName(c, customizedPrefix, 63)
}

//nolint
func generateObjectName(c asserter) string {
	return generateName(c, objectPrefix, 0)
}

//nolint
func generateShareName(c asserter) string {
	return generateName(c, sharePrefix, 63)
}

//nolint
func generateFilesystemName(c asserter) string {
	return generateName(c, blobfsPrefix, 63)
}

//nolint
func getShareURL(c asserter, fsu azfile.ServiceURL) (share azfile.ShareURL, name string) {
	name = generateShareName(c)
	share = fsu.NewShareURL(name)

	return share, name
}

//nolint
func generateAzureFileName(c asserter) string {
	return generateName(c, azureFilePrefix, 0)
}

//nolint
func generateBfsFileName(c asserter) string {
	return generateName(c, blobfsPrefix, 0)
}

//nolint
func getContainerURL(c asserter, bsu azblob.ServiceURL) (container azblob.ContainerURL, name string) {
	name = generateContainerName(c)
	container = bsu.NewContainerURL(name)

	return container, name
}

//nolint
func getFilesystemURL(c asserter, bfssu azbfs.ServiceURL) (filesystem azbfs.FileSystemURL, name string) {
	name = generateFilesystemName(c)
	filesystem = bfssu.NewFileSystemURL(name)

	return
}

//nolint
func getBlockBlobURL(c asserter, container azblob.ContainerURL, prefix string) (blob azblob.BlockBlobURL, name string) {
	name = prefix + generateBlobName(c)
	blob = container.NewBlockBlobURL(name)

	return blob, name
}

//nolint
func getBfsFileURL(c asserter, filesystemURL azbfs.FileSystemURL, prefix string) (file azbfs.FileURL, name string) {
	name = prefix + generateBfsFileName(c)
	file = filesystemURL.NewRootDirectoryURL().NewFileURL(name)

	return
}

//nolint
func getAppendBlobURL(c asserter, container azblob.ContainerURL, prefix string) (blob azblob.AppendBlobURL, name string) {
	name = generateBlobName(c)
	blob = container.NewAppendBlobURL(prefix + name)

	return blob, name
}

//nolint
func getPageBlobURL(c asserter, container azblob.ContainerURL, prefix string) (blob azblob.PageBlobURL, name string) {
	name = generateBlobName(c)
	blob = container.NewPageBlobURL(prefix + name)

	return
}

//nolint
func getAzureFileURL(c asserter, shareURL azfile.ShareURL, prefix string) (fileURL azfile.FileURL, name string) {
	name = prefix + generateAzureFileName(c)
	fileURL = shareURL.NewRootDirectoryURL().NewFileURL(name)

	return
}

//nolint
func getReaderToRandomBytes(n int) *bytes.Reader {
	r, _ := getRandomDataAndReader(n)
	return r
}

// todo: consider whether to replace with common.NewRandomDataGenerator, which is
//    believed to be faster
func getRandomDataAndReader(n int) (*bytes.Reader, []byte) {
	data := make([]byte, n)
	rand.Read(data)
	return bytes.NewReader(data), data
}

//nolint
func createNewContainer(c asserter, bsu azblob.ServiceURL) (container azblob.ContainerURL, name string) {
	container, name = getContainerURL(c, bsu)

	cResp, err := container.Create(ctx, nil, azblob.PublicAccessNone)
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)
	return container, name
}

//nolint
func createNewFilesystem(c asserter, bfssu azbfs.ServiceURL) (filesystem azbfs.FileSystemURL, name string) {
	filesystem, name = getFilesystemURL(c, bfssu)

	cResp, err := filesystem.Create(ctx)
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)
	return
}

//nolint
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

//nolint
func createNewBlockBlob(c asserter, container azblob.ContainerURL, prefix string) (blob azblob.BlockBlobURL, name string) {
	blob, name = getBlockBlobURL(c, container, prefix)

	cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{})

	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)

	return
}

//nolint
func createNewAzureShare(c asserter, fsu azfile.ServiceURL) (share azfile.ShareURL, name string) {
	share, name = getShareURL(c, fsu)

	cResp, err := share.Create(ctx, nil, 0)
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)
	return share, name
}

//nolint
func createNewAzureFile(c asserter, share azfile.ShareURL, prefix string) (file azfile.FileURL, name string) {
	file, name = getAzureFileURL(c, share, prefix)

	// generate parents first
	generateParentsForAzureFile(c, file)

	cResp, err := file.Create(ctx, defaultAzureFileSizeInBytes, azfile.FileHTTPHeaders{}, azfile.Metadata{})
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)

	return
}

func newNullFolderCreationTracker() ste.FolderCreationTracker {
	return ste.NewFolderCreationTracker(common.EFolderPropertiesOption.NoFolders(), nil)
}

func generateParentsForAzureFile(c asserter, fileURL azfile.FileURL) {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, _ := azfile.NewSharedKeyCredential(accountName, accountKey)
	err := ste.AzureFileParentDirCreator{}.CreateParentDirToRoot(ctx, fileURL, azfile.NewPipeline(credential, azfile.PipelineOptions{}), newNullFolderCreationTracker())
	c.AssertNoErr(err)
}

//nolint
func createNewAppendBlob(c asserter, container azblob.ContainerURL, prefix string) (blob azblob.AppendBlobURL, name string) {
	blob, name = getAppendBlobURL(c, container, prefix)

	resp, err := blob.Create(ctx, azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})

	c.AssertNoErr(err)
	c.Assert(resp.StatusCode(), equals(), 201)
	return
}

//nolint
func createNewPageBlob(c asserter, container azblob.ContainerURL, prefix string) (blob azblob.PageBlobURL, name string) {
	blob, name = getPageBlobURL(c, container, prefix)

	resp, err := blob.Create(ctx, azblob.PageBlobPageBytes*10, 0, azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{}, azblob.DefaultPremiumBlobAccessTier, nil, azblob.ClientProvidedKeyOptions{})

	c.AssertNoErr(err)
	c.Assert(resp.StatusCode(), equals(), 201)
	return
}

//nolint
func deleteContainer(c asserter, container azblob.ContainerURL) {
	resp, err := container.Delete(ctx, azblob.ContainerAccessConditions{})
	c.AssertNoErr(err)
	c.Assert(resp.StatusCode(), equals(), 202)
}

//nolint
func deleteFilesystem(c asserter, filesystem azbfs.FileSystemURL) {
	resp, err := filesystem.Delete(ctx)
	c.AssertNoErr(err)
	c.Assert(resp.StatusCode(), equals(), 202)
}

//nolint
type createS3ResOptions struct {
	Location string
}

//nolint
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

//nolint
func createNewBucket(c asserter, client *minio.Client, o createS3ResOptions) string {
	bucketName := generateBucketName(c)
	err := client.MakeBucket(bucketName, o.Location)
	c.AssertNoErr(err)

	return bucketName
}

//nolint
func createNewBucketWithName(c asserter, client *minio.Client, bucketName string, o createS3ResOptions) {
	err := client.MakeBucket(bucketName, o.Location)
	c.AssertNoErr(err)
}

//nolint
func createNewObject(c asserter, client *minio.Client, bucketName string, prefix string) (objectKey string) {
	objectKey = prefix + generateObjectName(c)

	size := int64(len(objectDefaultData))
	n, err := client.PutObject(bucketName, objectKey, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
	c.AssertNoErr(err)

	c.Assert(n, equals(), size)

	return
}

//nolint
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

//nolint
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

//nolint
func cleanBlobAccount(c asserter, serviceURL azblob.ServiceURL) {
	marker := azblob.Marker{}
	for marker.NotDone() {
		resp, err := serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})
		c.AssertNoErr(err)

		for _, v := range resp.ContainerItems {
			_, err = serviceURL.NewContainerURL(v.Name).Delete(ctx, azblob.ContainerAccessConditions{})
			c.AssertNoErr(err)
		}

		marker = resp.NextMarker
	}
}

//nolint
func cleanFileAccount(c asserter, serviceURL azfile.ServiceURL) {
	marker := azfile.Marker{}
	for marker.NotDone() {
		resp, err := serviceURL.ListSharesSegment(ctx, marker, azfile.ListSharesOptions{})
		c.AssertNoErr(err)

		for _, v := range resp.ShareItems {
			_, err = serviceURL.NewShareURL(v.Name).Delete(ctx, azfile.DeleteSnapshotsOptionNone)
			c.AssertNoErr(err)
		}

		marker = resp.NextMarker
	}

	time.Sleep(time.Minute)
}

//nolint
func getGenericCredentialForFile(accountType string) (*azfile.SharedKeyCredential, error) {
	accountNameEnvVar := accountType + "ACCOUNT_NAME"
	accountKeyEnvVar := accountType + "ACCOUNT_KEY"
	accountName, accountKey := os.Getenv(accountNameEnvVar), os.Getenv(accountKeyEnvVar)
	if accountName == "" || accountKey == "" {
		return nil, errors.New(accountNameEnvVar + " and/or " + accountKeyEnvVar + " environment variables not specified.")
	}
	return azfile.NewSharedKeyCredential(accountName, accountKey)
}

func deleteShare(c asserter, share azfile.ShareURL) {
	_, err := share.Delete(ctx, azfile.DeleteSnapshotsOptionInclude)
	c.AssertNoErr(err)
}

// Some tests require setting service properties. It can take up to 30 seconds for the new properties to be reflected across all FEs.
// We will enable the necessary property and try to run the test implementation. If it fails with an error that should be due to
// those changes not being reflected yet, we will wait 30 seconds and try the test again. If it fails this time for any reason,
// we fail the test. It is the responsibility of the the testImplFunc to determine which error string indicates the test should be retried.
// There can only be one such string. All errors that cannot be due to this detail should be asserted and not returned as an error string.
//nolint
func runTestRequiringServiceProperties(c asserter, bsu azblob.ServiceURL, code string,
	enableServicePropertyFunc func(asserter, azblob.ServiceURL),
	testImplFunc func(asserter, azblob.ServiceURL) error,
	disableServicePropertyFunc func(asserter, azblob.ServiceURL)) {
	enableServicePropertyFunc(c, bsu)
	defer disableServicePropertyFunc(c, bsu)
	err := testImplFunc(c, bsu)
	// We cannot assume that the error indicative of slow update will necessarily be a StorageError. As in ListBlobs.
	if err != nil && err.Error() == code {
		time.Sleep(time.Second * 30)
		err = testImplFunc(c, bsu)
		c.AssertNoErr(err)
	}
}

//nolint
func getContainerURLWithSAS(c asserter, credential azblob.SharedKeyCredential, containerName string) azblob.ContainerURL {
	sasQueryParams, err := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
		ContainerName: containerName,
		Permissions:   azblob.ContainerSASPermissions{Read: true, Add: true, Write: true, Create: true, Delete: true, List: true, Tag: true}.String(),
	}.NewSASQueryParameters(&credential)
	c.AssertNoErr(err)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s",
		credential.AccountName(), containerName, qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	// TODO perhaps we need a global default pipeline
	return azblob.NewContainerURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}

//nolint
func getBlobServiceURLWithSAS(c asserter, credential azblob.SharedKeyCredential) azblob.ServiceURL {
	sasQueryParams, err := azblob.AccountSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azblob.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, DeletePreviousVersion: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azblob.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azblob.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(&credential)
	c.AssertNoErr(err)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/?%s",
		credential.AccountName(), qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return azblob.NewServiceURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}

//nolint
func getFileServiceURLWithSAS(c asserter, credential azfile.SharedKeyCredential) azfile.ServiceURL {
	sasQueryParams, err := azfile.AccountSASSignatureValues{
		Protocol:      azfile.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azfile.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(&credential)
	c.AssertNoErr(err)

	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/?%s", credential.AccountName(), qp)

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return azfile.NewServiceURL(*fullURL, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
}

//nolint
func getShareURLWithSAS(c asserter, credential azfile.SharedKeyCredential, shareName string) azfile.ShareURL {
	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   shareName,
		Permissions: azfile.ShareSASPermissions{Read: true, Write: true, Create: true, Delete: true, List: true}.String(),
	}.NewSASQueryParameters(&credential)
	c.AssertNoErr(err)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/%s?%s",
		credential.AccountName(), shareName, qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	// TODO perhaps we need a global default pipeline
	return azfile.NewShareURL(*fullURL, azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{}))
}

//nolint
func getAdlsServiceURLWithSAS(c asserter, credential azbfs.SharedKeyCredential) azbfs.ServiceURL {
	sasQueryParams, err := azbfs.AccountSASSignatureValues{
		Protocol:      azbfs.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azfile.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
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
//nolint
type stringContainsChecker struct {
	*chk.CheckerInfo
}

// Check
//nolint
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
