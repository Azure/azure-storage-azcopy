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

package ste

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
)

// This file covers testing of the GetMD5() method in the various sourceInfoProvider implementations

const (
	containerPrefix        = "container"
	blobPrefix             = "blob"
	crc64Polynomial uint64 = 0x9A6C9329AC4BC9B5
)

var CRC64Table = crc64.MakeTable(crc64Polynomial)

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
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

func getDataAndReader(testName string, n int) (*bytes.Reader, []byte) {
	// Random seed for data generation
	seed := int64(crc64.Checksum([]byte(testName), CRC64Table))
	random := rand.New(rand.NewSource(seed))
	data := make([]byte, n)
	_, _ = random.Read(data)
	return bytes.NewReader(data), data
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

// if S3_TESTS_OFF is set at all, S3 tests are disabled.
func isS3Disabled() bool {
	return strings.ToLower(os.Getenv("S3_TESTS_OFF")) != ""
}

func skipIfS3Disabled(t *testing.T) {
	if isS3Disabled() {
		t.Skip("S3 testing is disabled for this unit test suite run.")
	}
}

func createS3ClientWithMinio() (*minio.Client, error) {
	if isS3Disabled() {
		return nil, errors.New("s3 testing is disabled")
	}

	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY should be set before creating the S3 client")
	}

	s3Client, err := minio.New("s3.amazonaws.com", &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: true,
		Region: "",
	})
	if err != nil {
		return nil, err
	}
	return s3Client, nil
}

func TestBenchmark(t *testing.T) {
	a := assert.New(t)

	jptm := testJobPartTransferManager{
		info:   nil,
		fromTo: common.EFromTo.BenchmarkBlob(),
	}
	benchSIP, err := newBenchmarkSourceInfoProvider(&jptm)
	a.Nil(err)

	_, err = benchSIP.GetMD5(0, 1)
	a.NotNil(err)
}

func TestBlockBlob(t *testing.T) {
	a := assert.New(t)

	// Setup
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	a.Nil(err)

	cName := generateContainerName()
	cc := client.NewContainerClient(cName)
	_, err = cc.Create(context.Background(), nil)
	a.Nil(err)
	defer cc.Delete(context.Background(), nil)

	bName := generateBlobName()
	bc := cc.NewBlockBlobClient(bName)
	size := 1024 * 1024 * 10
	dataReader, data := getDataAndReader(t.Name(), size)
	_, err = bc.Upload(context.Background(), streaming.NopCloser(dataReader), nil)

	sasURL, err := cc.NewBlobClient(bName).GetSASURL(
		blobsas.BlobPermissions{Read: true},
		time.Now().Add(1*time.Hour),
		nil)
	a.Nil(err)

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       sasURL,
			SrcContainer: cName,
			SrcFilePath:  bName,
		}),
		fromTo: common.EFromTo.BlobBlob(),
	}
	blobSIP, err := newBlobSourceInfoProvider(&jptm)
	a.Nil(err)

	// Get MD5 range within service calculation
	offset := rand.Int63n(int64(size) - 1)
	count := int64(common.MaxRangeGetSize)
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 := md5.Sum(data[offset : offset+count])
	computedMd5, err := blobSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))

	// Get MD5 range outside service calculation
	offset = rand.Int63n(int64(size) - int64(common.MaxRangeGetSize) - 1)
	count = int64(common.MaxRangeGetSize) + rand.Int63n(int64(size)-int64(common.MaxRangeGetSize))
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 = md5.Sum(data[offset : offset+count])
	computedMd5, err = blobSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))
}

func TestShareFile(t *testing.T) {
	a := assert.New(t)

	// Setup
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/", accountName)

	credential, err := file.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	a.Nil(err)

	sName := generateContainerName()
	sc := client.NewShareClient(sName)
	_, err = sc.Create(context.Background(), nil)
	a.Nil(err)
	defer sc.Delete(context.Background(), nil)

	fName := generateBlobName()
	fc := sc.NewRootDirectoryClient().NewFileClient(fName)
	size := 1024 * 1024 * 10
	_, err = fc.Create(context.Background(), int64(size), nil)
	a.Nil(err)
	dataReader, data := getDataAndReader(t.Name(), size)
	err = fc.UploadStream(context.Background(), dataReader, nil)
	a.Nil(err)

	sasURL, err := fc.GetSASURL(
		filesas.FilePermissions{Read: true},
		time.Now().Add(1*time.Hour),
		nil)
	a.Nil(err)

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       sasURL,
			SrcContainer: sName,
			SrcFilePath:  fName,
		}),
		fromTo: common.EFromTo.FileBlob(),
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	// Get MD5 range within service calculation
	offset := rand.Int63n(int64(size) - 1)
	count := int64(common.MaxRangeGetSize)
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 := md5.Sum(data[offset : offset+count])
	computedMd5, err := fileSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))

	// Get MD5 range outside service calculation
	offset = rand.Int63n(int64(size) - int64(common.MaxRangeGetSize) - 1)
	count = int64(common.MaxRangeGetSize) + rand.Int63n(int64(size)-int64(common.MaxRangeGetSize))
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 = md5.Sum(data[offset : offset+count])
	computedMd5, err = fileSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))
}

func TestShareDirectory(t *testing.T) {
	a := assert.New(t)

	// Setup
	accountName, accountKey := getAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/", accountName)

	credential, err := file.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)

	client, err := fileservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	a.Nil(err)

	sName := generateContainerName()
	sc := client.NewShareClient(sName)
	_, err = sc.Create(context.Background(), nil)
	a.Nil(err)
	defer sc.Delete(context.Background(), nil)

	dName := generateBlobName()
	dc := sc.NewDirectoryClient(dName)
	_, err = dc.Create(context.Background(), nil)
	a.Nil(err)

	sasURL, err := sc.GetSASURL(
		filesas.SharePermissions{Read: true},
		time.Now().Add(1*time.Hour),
		nil)
	a.Nil(err)

	fileURLParts, err := file.ParseURL(sasURL)
	a.Nil(err)
	fileURLParts.DirectoryOrFilePath = dName
	sasURL = fileURLParts.String()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       sasURL,
			SrcContainer: sName,
			SrcFilePath:  dName,
		}),
		fromTo: common.EFromTo.FileBlob(),
	}
	fileSIP, err := newFileSourceInfoProvider(&jptm)
	a.Nil(err)

	_, err = fileSIP.GetMD5(0, 1)
	a.NotNil(err)
}

func TestGCP(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)

	// Setup
	gcpClient, err := gcpUtils.NewClient(context.Background())
	a.Nil(err)
	bName := generateContainerName()
	bucket := gcpClient.Bucket(bName)
	err = bucket.Create(context.Background(), os.Getenv("GOOGLE_CLOUD_PROJECT"), &gcpUtils.BucketAttrs{})
	a.Nil(err)
	defer bucket.Delete(context.Background())

	oName := generateBlobName()
	oc := bucket.Object(oName)
	size := 1024 * 1024 * 10
	wc := oc.NewWriter(context.Background())
	a.Nil(err)
	dataReader, data := getDataAndReader(t.Name(), size)
	written, err := io.Copy(wc, dataReader)
	a.Nil(err)
	a.Equal(int64(size), written)
	_ = wc.Close()

	rawURL := fmt.Sprintf("https://storage.cloud.google.com/%s/%s", bName, oName)

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: bName,
			SrcFilePath:  oName,
		}),
		fromTo: common.EFromTo.GCPBlob(),
	}
	gcpSIP, err := newGCPSourceInfoProvider(&jptm)
	a.Nil(err)

	// Get MD5 range within service calculation
	offset := rand.Int63n(int64(size) - 1)
	count := int64(common.MaxRangeGetSize)
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 := md5.Sum(data[offset : offset+count])
	computedMd5, err := gcpSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))

	// Get MD5 range outside service calculation
	offset = rand.Int63n(int64(size) - int64(common.MaxRangeGetSize) - 1)
	count = int64(common.MaxRangeGetSize) + rand.Int63n(int64(size)-int64(common.MaxRangeGetSize))
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 = md5.Sum(data[offset : offset+count])
	computedMd5, err = gcpSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))
}

func TestLocal(t *testing.T) {
	a := assert.New(t)

	// Setup
	fName := generateBlobName()
	f, err := os.CreateTemp("", fName)
	a.Nil(err)
	defer os.Remove(f.Name())

	size := 1024 * 1024 * 10
	a.Nil(err)
	dataReader, data := getDataAndReader(t.Name(), size)
	n, err := io.Copy(f, dataReader)
	a.Nil(err)
	a.Equal(int64(size), n)
	f.Close()

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:      f.Name(),
			SrcFilePath: fName,
		}),
		fromTo: common.EFromTo.LocalBlob(),
	}
	localSIP, err := newLocalSourceInfoProvider(&jptm)
	a.Nil(err)

	// Get MD5 range within service calculation
	offset := rand.Int63n(int64(size) - 1)
	count := int64(common.MaxRangeGetSize)
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 := md5.Sum(data[offset : offset+count])
	computedMd5, err := localSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))

	// Get MD5 range outside service calculation
	offset = rand.Int63n(int64(size) - int64(common.MaxRangeGetSize) - 1)
	count = int64(common.MaxRangeGetSize) + rand.Int63n(int64(size)-int64(common.MaxRangeGetSize))
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 = md5.Sum(data[offset : offset+count])
	computedMd5, err = localSIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))
}

func TestS3(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)

	// Setup
	s3Client, err := createS3ClientWithMinio()
	a.Nil(err)
	bName := generateContainerName()
	ctx := context.Background()
	err = s3Client.MakeBucket(ctx, bName, minio.MakeBucketOptions{Region: ""})
	a.Nil(err)
	defer s3Client.RemoveBucket(ctx, bName)

	oName := generateBlobName()
	size := 1024 * 1024 * 10
	a.Nil(err)
	dataReader, data := getDataAndReader(t.Name(), size)
	n, err := s3Client.PutObject(ctx, bName, oName, dataReader, int64(size), minio.PutObjectOptions{})
	a.Nil(err)
	a.Equal(int64(size), n)

	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s/%s", "", bName, oName)

	jptm := testJobPartTransferManager{
		info: to.Ptr(TransferInfo{
			Source:       rawURL,
			SrcContainer: bName,
			SrcFilePath:  oName,
		}),
		fromTo: common.EFromTo.S3Blob(),
	}
	s3SIP, err := newS3SourceInfoProvider(&jptm)
	a.Nil(err)

	// Get MD5 range within service calculation
	offset := rand.Int63n(int64(size) - 1)
	count := int64(common.MaxRangeGetSize)
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 := md5.Sum(data[offset : offset+count])
	computedMd5, err := s3SIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))

	// Get MD5 range outside service calculation
	offset = rand.Int63n(int64(size) - int64(common.MaxRangeGetSize) - 1)
	count = int64(common.MaxRangeGetSize) + rand.Int63n(int64(size)-int64(common.MaxRangeGetSize))
	if offset+count > int64(size) {
		count = int64(size) - offset
	}
	localMd5 = md5.Sum(data[offset : offset+count])
	computedMd5, err = s3SIP.GetMD5(offset, count)
	a.Nil(err)
	a.True(bytes.Equal(localMd5[:], computedMd5))
}
