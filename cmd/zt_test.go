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
	"io/ioutil"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	chk "gopkg.in/check.v1"

	"math/rand"

	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
	minio "github.com/minio/minio-go"
)

// Hookup to the testing framework
func Test(t *testing.T) { chk.TestingT(t) }

type cmdIntegrationSuite struct{}

var _ = chk.Suite(&cmdIntegrationSuite{})
var ctx = context.Background()

const (
	containerPrefix      = "container"
	blobPrefix           = "blob"
	blockBlobDefaultData = "AzCopy Random Test Data"

	bucketPrefix      = "s3bucket"
	objectPrefix      = "s3object"
	objectDefaultData = "AzCopy default data for S3 object"
)

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Note that this imposes a restriction on the length of test names
func generateName(prefix string) string {
	// These next lines up through the for loop are obtaining and walking up the stack
	// trace to extrat the test name, which is stored in name
	pc := make([]uintptr, 10)
	runtime.Callers(0, pc)
	f := runtime.FuncForPC(pc[0])
	name := f.Name()
	for i := 0; !strings.Contains(name, "Suite"); i++ { // The tests are all scoped to the suite, so this ensures getting the actual test name
		f = runtime.FuncForPC(pc[i])
		name = f.Name()
	}
	funcNameStart := strings.Index(name, "Test")
	name = name[funcNameStart+len("Test"):] // Just get the name of the test and not any of the garbage at the beginning
	name = strings.ToLower(name)            // Ensure it is a valid resource name
	currentTime := time.Now()
	name = fmt.Sprintf("%s%s%d%d%d", prefix, strings.ToLower(name), currentTime.Minute(), currentTime.Second(), currentTime.Nanosecond())
	return name
}

func generateContainerName() string {
	name := generateName(containerPrefix)
	return name[0:63]
}

func generateBlobName() string {
	return generateName(blobPrefix)
}

func generateBucketName() string {
	name := generateName(bucketPrefix)
	return name[0:63]
}

func generateBucketNameWithCustomizedPrefix(customizedPrefix string) string {
	name := generateName(customizedPrefix)
	return name[0:63]
}

func generateObjectName() string {
	return generateName(objectPrefix)
}

func getContainerURL(c *chk.C, bsu azblob.ServiceURL) (container azblob.ContainerURL, name string) {
	name = generateContainerName()
	container = bsu.NewContainerURL(name)

	return container, name
}

func getBlockBlobURL(c *chk.C, container azblob.ContainerURL, prefix string) (blob azblob.BlockBlobURL, name string) {
	name = prefix + generateBlobName()
	blob = container.NewBlockBlobURL(name)

	return blob, name
}

func getAppendBlobURL(c *chk.C, container azblob.ContainerURL, prefix string) (blob azblob.AppendBlobURL, name string) {
	name = generateBlobName()
	blob = container.NewAppendBlobURL(prefix + name)

	return blob, name
}

func getPageBlobURL(c *chk.C, container azblob.ContainerURL, prefix string) (blob azblob.PageBlobURL, name string) {
	name = generateBlobName()
	blob = container.NewPageBlobURL(prefix + name)

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

func createNewContainer(c *chk.C, bsu azblob.ServiceURL) (container azblob.ContainerURL, name string) {
	container, name = getContainerURL(c, bsu)

	cResp, err := container.Create(ctx, nil, azblob.PublicAccessNone)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return container, name
}

func createNewBlockBlob(c *chk.C, container azblob.ContainerURL, prefix string) (blob azblob.BlockBlobURL, name string) {
	blob, name = getBlockBlobURL(c, container, prefix)

	cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{})

	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return
}

func createNewAppendBlob(c *chk.C, container azblob.ContainerURL, prefix string) (blob azblob.AppendBlobURL, name string) {
	blob, name = getAppendBlobURL(c, container, prefix)

	resp, err := blob.Create(ctx, azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{})

	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, 201)
	return
}

func createNewPageBlob(c *chk.C, container azblob.ContainerURL, prefix string) (blob azblob.PageBlobURL, name string) {
	blob, name = getPageBlobURL(c, container, prefix)

	resp, err := blob.Create(ctx, azblob.PageBlobPageBytes*10, 0, azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{})

	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, 201)
	return
}

func deleteContainer(c *chk.C, container azblob.ContainerURL) {
	resp, err := container.Delete(ctx, azblob.ContainerAccessConditions{})
	c.Assert(err, chk.IsNil)
	c.Assert(resp.StatusCode(), chk.Equals, 202)
}

func getGenericCredential(accountType string) (*azblob.SharedKeyCredential, error) {
	accountNameEnvVar := accountType + "ACCOUNT_NAME"
	accountKeyEnvVar := accountType + "ACCOUNT_KEY"
	accountName, accountKey := os.Getenv(accountNameEnvVar), os.Getenv(accountKeyEnvVar)
	if accountName == "" || accountKey == "" {
		return nil, errors.New(accountNameEnvVar + " and/or " + accountKeyEnvVar + " environment variables not specified.")
	}
	return azblob.NewSharedKeyCredential(accountName, accountKey)
}

func getGenericBSU(accountType string) (azblob.ServiceURL, error) {
	credential, err := getGenericCredential(accountType)
	if err != nil {
		return azblob.ServiceURL{}, err
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	blobPrimaryURL, _ := url.Parse("https://" + credential.AccountName() + ".blob.core.windows.net/")
	return azblob.NewServiceURL(*blobPrimaryURL, pipeline), nil
}

func getBSU() azblob.ServiceURL {
	bsu, _ := getGenericBSU("")
	return bsu
}

func validateStorageError(c *chk.C, err error, code azblob.ServiceCodeType) {
	serr, _ := err.(azblob.StorageError)
	c.Assert(serr.ServiceCode(), chk.Equals, code)
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

func createNewBucket(c *chk.C, client *minio.Client, o createS3ResOptions) string {
	bucketName := generateBucketName()
	err := client.MakeBucket(bucketName, o.Location)
	c.Assert(err, chk.IsNil)

	return bucketName
}

func createNewBucketWithName(c *chk.C, client *minio.Client, bucketName string, o createS3ResOptions) {
	err := client.MakeBucket(bucketName, o.Location)
	c.Assert(err, chk.IsNil)
}

func createNewObject(c *chk.C, client *minio.Client, bucketName string, prefix string) (objectKey string) {
	objectKey = prefix + generateObjectName()

	size := int64(len(objectDefaultData))
	n, err := client.PutObject(bucketName, objectKey, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
	c.Assert(err, chk.IsNil)

	c.Assert(n, chk.Equals, size)

	return
}

func deleteBucket(c *chk.C, client *minio.Client, bucketName string) {
	objectsCh := make(chan string)

	go func() {
		defer close(objectsCh)

		// List all objects from a bucket-name with a matching prefix.
		for object := range client.ListObjectsV2(bucketName, "", true, context.Background().Done()) {
			c.Assert(object.Err, chk.IsNil)
			objectsCh <- object.Key
		}
	}()

	// List bucket, and delete all the objects in the bucket
	errChn := client.RemoveObjects(bucketName, objectsCh)

	for err := range errChn {
		c.Assert(err, chk.IsNil)
	}

	// Remove the bucket.
	err := client.RemoveBucket(bucketName)
	c.Assert(err, chk.IsNil)
}

func getFSU() (azfile.ServiceURL, error) {
	accountName, accountKey := os.Getenv("ACCOUNT_NAME"), os.Getenv("ACCOUNT_KEY")
	if accountName == "" || accountKey == "" {
		return azfile.ServiceURL{}, errors.New("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/", accountName))
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return azfile.ServiceURL{}, err
	}
	pipeline := azfile.NewPipeline(credential, azfile.PipelineOptions{})
	return azfile.NewServiceURL(*u, pipeline), nil
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

// Some tests require setting service properties. It can take up to 30 seconds for the new properties to be reflected across all FEs.
// We will enable the necessary property and try to run the test implementation. If it fails with an error that should be due to
// those changes not being reflected yet, we will wait 30 seconds and try the test again. If it fails this time for any reason,
// we fail the test. It is the responsibility of the the testImplFunc to determine which error string indicates the test should be retried.
// There can only be one such string. All errors that cannot be due to this detail should be asserted and not returned as an error string.
func runTestRequiringServiceProperties(c *chk.C, bsu azblob.ServiceURL, code string,
	enableServicePropertyFunc func(*chk.C, azblob.ServiceURL),
	testImplFunc func(*chk.C, azblob.ServiceURL) error,
	disableServicePropertyFunc func(*chk.C, azblob.ServiceURL)) {
	enableServicePropertyFunc(c, bsu)
	defer disableServicePropertyFunc(c, bsu)
	err := testImplFunc(c, bsu)
	// We cannot assume that the error indicative of slow update will necessarily be a StorageError. As in ListBlobs.
	if err != nil && err.Error() == code {
		time.Sleep(time.Second * 30)
		err = testImplFunc(c, bsu)
		c.Assert(err, chk.IsNil)
	}
}

func enableSoftDelete(c *chk.C, bsu azblob.ServiceURL) {
	days := int32(1)
	_, err := bsu.SetProperties(ctx, azblob.StorageServiceProperties{DeleteRetentionPolicy: &azblob.RetentionPolicy{Enabled: true, Days: &days}})
	c.Assert(err, chk.IsNil)
}

func disableSoftDelete(c *chk.C, bsu azblob.ServiceURL) {
	_, err := bsu.SetProperties(ctx, azblob.StorageServiceProperties{DeleteRetentionPolicy: &azblob.RetentionPolicy{Enabled: false}})
	c.Assert(err, chk.IsNil)
}

func validateUpload(c *chk.C, blobURL azblob.BlockBlobURL) {
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	c.Assert(err, chk.IsNil)
	data, _ := ioutil.ReadAll(resp.Response().Body)
	c.Assert(data, chk.HasLen, 0)
}

func getContainerURLWithSAS(c *chk.C, credential azblob.SharedKeyCredential, containerName string) azblob.ContainerURL {
	sasQueryParams, err := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
		ContainerName: containerName,
		Permissions:   azblob.ContainerSASPermissions{Read: true, Add: true, Write: true, Create: true, Delete: true, List: true}.String(),
		Version:       "2018-03-28",
	}.NewSASQueryParameters(&credential)
	c.Assert(err, chk.IsNil)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s",
		credential.AccountName(), containerName, qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil)

	// TODO perhaps we need a global default pipeline
	return azblob.NewContainerURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}

func getServiceURLWithSAS(c *chk.C, credential azblob.SharedKeyCredential) azblob.ServiceURL {
	sasQueryParams, err := azblob.AccountSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azfile.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(&credential)
	c.Assert(err, chk.IsNil)

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/?%s",
		credential.AccountName(), qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil)

	return azblob.NewServiceURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}
