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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	datalakedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	"github.com/stretchr/testify/assert"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Additional S2S migration cases, besides E2E smoke testing cases for S3/blob/file source contained in test_service_to_service_copy.py

const (
	defaultLogVerbosityForCopy       = "WARNING"
	defaultOutputFormatForCopy       = "text"
	defaultBlobTypeForCopy           = "Detect"
	defaultBlockBlobTierForCopy      = "None"
	defaultPageBlobTierForCopy       = "None"
	defaultS2SPreserveProperties     = true
	defaultS2SPreserveAccessTier     = true
	defaultS2SGetPropertiesInBackend = true
	defaultS2SSourceChangeValidation = false
	debugMode                        = false // keep the debugMode temporarily, as merging happens frequently, and this might be useful for solving potential issue.
)

var defaultS2SInvalideMetadataHandleOption = common.DefaultInvalidMetadataHandleOption

func TestMain(m *testing.M) {
	if !isS3Disabled() {
		if s3Client, err := createS3ClientWithMinio(createS3ResOptions{}); err == nil {
			cleanS3Account(s3Client)
		} else {
			// If S3 credentials aren't supplied, we're probably only trying to run Azure tests.
			// As such, gracefully return here instead of cancelling every test because we couldn't clean up S3.
			fmt.Println("S3 client could not be successfully initialised")
		}
	}

	if !gcpTestsDisabled() {
		if gcpClient, err := createGCPClientWithGCSSDK(); err == nil {
			cleanGCPAccount(gcpClient)
		} else {
			fmt.Println("GCP client could not be successfully initialised")
		}
	}
	os.Exit(m.Run())
}

func getDefaultRawCopyInput(src, dst string) rawCopyCmdArgs {
	return rawCopyCmdArgs{
		src:                            src,
		dst:                            dst,
		recursive:                      true,
		output:                         defaultOutputFormatForCopy,
		blobType:                       defaultBlobTypeForCopy,
		blockBlobTier:                  defaultBlockBlobTierForCopy,
		pageBlobTier:                   defaultPageBlobTierForCopy,
		md5ValidationOption:            common.DefaultHashValidationOption.String(),
		s2sGetPropertiesInBackend:      defaultS2SGetPropertiesInBackend,
		s2sPreserveAccessTier:          defaultS2SPreserveAccessTier,
		s2sPreserveProperties:          defaultS2SPreserveProperties,
		s2sSourceChangeValidation:      defaultS2SSourceChangeValidation,
		s2sInvalidMetadataHandleOption: defaultS2SInvalideMetadataHandleOption.String(),
		forceWrite:                     common.EOverwriteOption.True().String(),
		preserveOwner:                  common.PreserveOwnerDefault,
		asSubdir:                       true,
	}
}

func validateS2STransfersAreScheduled(a *assert.Assertions, srcDirName string, dstDirName string, expectedTransfers []string, mockedRPC interceptor) {
	// validate that the right number of transfers were scheduled
	a.Equal(len(expectedTransfers), len(mockedRPC.transfers))
	debugMode := true
	if debugMode {
		fmt.Println("expectedTransfers: ")
		printTransfers(expectedTransfers)
		fmt.Println("srcDirName: ", srcDirName)
		fmt.Println("dstDirName: ", dstDirName)
	}

	// validate that the right transfers were sent
	lookupMap := scenarioHelper{}.convertListToMap(expectedTransfers)
	for _, transfer := range mockedRPC.transfers {
		if debugMode {
			fmt.Println("transfer.Source: ", transfer.Source)
			fmt.Println("transfer.Destination: ", transfer.Destination)
		}

		srcRelativeFilePath, _ := url.PathUnescape(transfer.Source)
		dstRelativeFilePath, _ := url.PathUnescape(transfer.Destination)

		unescapedSrcDir, _ := url.PathUnescape(srcDirName)
		unescapedDstDir, _ := url.PathUnescape(dstDirName)

		srcRelativeFilePath = strings.Replace(srcRelativeFilePath, unescapedSrcDir, "", 1)
		dstRelativeFilePath = strings.Replace(dstRelativeFilePath, unescapedDstDir, "", 1)
		if unescapedDstDir == dstRelativeFilePath+"/" {
			// Thing we were searching for is bigger than what we are searching in, due to ending end a /
			// Happens for root dir
			dstRelativeFilePath = ""
		}

		if debugMode {
			fmt.Println("srcRelativeFilePath: ", srcRelativeFilePath)
			fmt.Println("dstRelativeFilePath: ", dstRelativeFilePath)
		}

		// the relative paths should be equal
		a.Equal(dstRelativeFilePath, srcRelativeFilePath)

		// look up the transfer is expected
		_, dstExist := lookupMap[dstRelativeFilePath]
		a.True(dstExist)
	}
}

func printTransfers(ts []string) {
	for _, t := range ts {
		fmt.Println(t)
	}
}

func TestS2SCopyFromS3ToBlobWithBucketNameNeedBeResolved(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	invalidPrefix := "invalid---bucketname.for---azure"
	resolvedPrefix := "invalid-3-bucketname-for-3-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName, "", false)
	a.NotZero(len(objectList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	raw := getDefaultRawCopyInput(rawSrcS3BucketURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(objectList), len(mockedRPC.transfers))

		// Check container with resolved name has been created
		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		bsc := scenarioHelper{}.getBlobServiceClient(a)
		cc := bsc.NewContainerClient(resolvedBucketName)
		a.True(scenarioHelper{}.containerExists(cc))
		defer deleteContainer(a, cc)

		// Check correct entry are scheduled.
		// Example:
		// sourceURL pass to azcopy:  https://s3.amazonaws.com/invalid---bucketname.for---azures2scopyfroms3toblobwithbucketna
		// destURL pass to azcopy:  https://jiacstgcanary01.blob.core.windows.net
		// transfer.Source by design be scheduled:  /tops3objects2scopyfroms3toblobwithbucketnameneedberesolved4243293354900
		// transfer.Destination by design be scheduled:  /invalid-3-bucketname-for-3-azures2scopyfroms3toblobwithbucketna/tops3objects2scopyfroms3toblobwithbucketnameneedberesolved4243293354900
		// Nothing should be replaced during matching for source, and resolved bucket name should be replaced for destination.
		validateS2STransfersAreScheduled(a, "", common.AZCOPY_PATH_SEPARATOR_STRING+resolvedBucketName, objectList, mockedRPC)
	})
}

func TestS2SCopyFromS3ToBlobWithWildcardInSrcAndBucketNameNeedBeResolved(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	invalidPrefix := "invalid----bucketname.for-azure"
	resolvedPrefix := "invalid-4-bucketname-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName, "", false)
	a.NotZero(len(objectList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	rawSrcS3BucketStrWithWirdcard := strings.Replace(rawSrcS3BucketURL.String(), invalidPrefix, "invalid----*", 1)
	raw := getDefaultRawCopyInput(rawSrcS3BucketStrWithWirdcard, rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(objectList), len(mockedRPC.transfers))

		// Check container with resolved name has been created
		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		bsc := scenarioHelper{}.getBlobServiceClient(a)
		cc := bsc.NewContainerClient(resolvedBucketName)
		a.True(scenarioHelper{}.containerExists(cc))
		defer deleteContainer(a, cc)

		// Check correct entry are scheduled.
		// Example:
		// sourceURL pass to azcopy:  https://s3.amazonaws.com/invalid*s2scopyfroms3toblobwithwildcardi
		// destURL pass to azcopy:  https://jiacstgcanary01.blob.core.windows.net
		// transfer.Source by design be scheduled:  /invalid----bucketname.for-azures2scopyfroms3toblobwithwildcardi/sub1/sub3/sub5/s3objects2scopyfroms3toblobwithwildcardinsrcandbucketnameneedberesolved435110281300
		// transfer.Destination by design be scheduled:  /invalid-4-bucketname-for-azures2scopyfroms3toblobwithwildcardi/sub1/sub3/sub5/s3objects2scopyfroms3toblobwithwildcardinsrcandbucketnameneedberesolved435110281300
		// org bucket name should be replaced during matching for source, and resolved bucket name should be replaced for destination.
		validateS2STransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING+bucketName, common.AZCOPY_PATH_SEPARATOR_STRING+resolvedBucketName, objectList, mockedRPC)
	})
}

// This is negative because generateBucketNameWithCustomizedPrefix will return a bucket name with length 63,
// and resolving logic will resolve -- to -2- which means the length to be 64. This exceeds valid container name, so error will be returned.
func TestS2SCopyFromS3ToBlobWithBucketNameNeedBeResolvedNegative(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	invalidPrefix := "invalid.bucketname--for.azure"
	// resolvedPrefix := "invalid-bucketname-2-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})

	defer deleteBucket(s3Client, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName, "", false)
	a.NotZero(len(objectList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	raw := getDefaultRawCopyInput(rawSrcS3BucketURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should not be resolved, and objects should not be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)

		loggedError := false
		log := glcm.(*mockedLifecycleManager).infoLog
		count := len(log)
		for count > 0 {
			x := <-log
			if strings.Contains(x, "invalid name") {
				loggedError = true
			}
			count = len(log)
		}

		a.True(loggedError)
	})
}

// Copy from virtual directory to container, with normal encoding ' ' as ' '.
func TestS2SCopyFromS3ToBlobWithSpaceInSrcNotEncoded(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"space dir/space object"}
	scenarioHelper{}.generateObjects(a, s3Client, bucketName, objectList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName) // Use default region
	rawSrcS3DirStr := rawSrcS3BucketURL.String() + "/space dir"
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3DirStr, rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))
		// common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.
		// The destination is URL encoded, as go's URL method do the encoding.
		a.Equal("/space%20dir/space%20object", mockedRPC.transfers[0].Destination)
	})
}

// Copy from virtual directory to container, with special encoding ' ' to '+' by S3 management portal.
// '+' is handled in copy.go before extract the SourceRoot.
// The scheduled transfer would be URL encoded no matter what's the raw source/destination provided by user.
func TestS2SCopyFromS3ToBlobWithSpaceInSrcEncodedAsPlus(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"space dir/space object"}
	scenarioHelper{}.generateObjects(a, s3Client, bucketName, objectList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName) // Use default region
	rawSrcS3DirStr := rawSrcS3BucketURL.String() + "/space+dir"
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3DirStr, rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))
		// common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.
		// The destination is URL encoded, as go's URL method do the encoding.
		a.Equal("/space%20dir/space%20object", mockedRPC.transfers[0].Destination)
	})
}

// By design, when source directory contains objects with suffix ‘/’, objects with suffix ‘/’ should be ignored.
func TestS2SCopyFromS3ToBlobWithObjectUsingSlashAsSuffix(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"fileConsiderdAsDirectory/", "file", "sub1/file"}
	scenarioHelper{}.generateObjects(a, s3Client, bucketName, objectList)

	validateObjectList := []string{"/file", "/sub1/file"} // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(a, "", bucketName) // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3BucketURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(validateObjectList), len(mockedRPC.transfers))

		validateS2STransfersAreScheduled(a, "", "/"+bucketName, validateObjectList, mockedRPC)
	})
}

func TestS2SCopyFromS3AccountWithBucketInDifferentRegionsAndListUseDefaultEndpoint(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName1 := generateBucketNameWithCustomizedPrefix("default-region")
	createNewBucketWithName(a, s3Client, bucketName1, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName1, true)

	bucketName2 := generateBucketNameWithCustomizedPrefix("us-west-2-region")
	bucketRegion2 := "us-west-1" // Use different region than other regional test to avoid conflicting
	createNewBucketWithName(a, s3Client, bucketName2, createS3ResOptions{Location: bucketRegion2})
	defer deleteBucket(s3Client, bucketName2, true)

	objectList1 := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName1, "", true)
	a.NotZero(len(objectList1))

	objectList2 := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName2, "", true)
	a.NotZero(len(objectList2))

	validateObjectList := append(objectList1, objectList2...)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3AccountURL := scenarioHelper{}.getRawS3AccountURL(a, "") // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	raw := getDefaultRawCopyInput(rawSrcS3AccountURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateS2STransfersAreScheduled(a, "", "", validateObjectList, mockedRPC)
	})
}

func TestS2SCopyFromS3AccountWithBucketInDifferentRegionsAndListUseSpecificRegion(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	specificRegion := "us-west-2"
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName1 := generateBucketNameWithCustomizedPrefix("default-region")
	createNewBucketWithName(a, s3Client, bucketName1, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName1, true)

	bucketName2 := generateBucketNameWithCustomizedPrefix(specificRegion)
	createNewBucketWithName(a, s3Client, bucketName2, createS3ResOptions{Location: specificRegion})
	defer deleteBucket(s3Client, bucketName2, true)

	time.Sleep(30 * time.Second) // TODO: review and remove this, which was put here as a workaround to issues with buckets being reported as not existing

	objectList1 := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName1, "", true)
	a.NotZero(len(objectList1))

	objectList2 := scenarioHelper{}.generateCommonRemoteScenarioForS3(a, s3Client, bucketName2, "", true)
	a.NotZero(len(objectList2))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3AccountURL := scenarioHelper{}.getRawS3AccountURL(a, specificRegion)
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	raw := getDefaultRawCopyInput(rawSrcS3AccountURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateS2STransfersAreScheduled(a, "", "", objectList2, mockedRPC)
	})
}

func TestS2SCopyFromS3ObjectToBlobContainer(t *testing.T) {
	a := assert.New(t)
	skipIfS3Disabled(t)
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		t.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(a, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateObjects(a, s3Client, bucketName, objectList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(a, "", bucketName, "file") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3ObjectURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	mockedRPC.reset()

	rawSrcS3ObjectURL = scenarioHelper{}.getRawS3ObjectURL(a, "", bucketName, "sub/file2") // Use default region
	rawDstContainerURLWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcS3ObjectURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file2", mockedRPC.transfers[0].Destination)
	})
}

func TestS2SCopyFromGCPToBlobWithBucketNameNeedBeResolved(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)

	gcpClient, err := createGCPClientWithGCSSDK()

	if err != nil {
		t.Skip("GCP credentials not supplied")
	}

	invalidPrefix := "invalid---bucket_name_for-azure"
	resolvedPrefix := "invalid-3-bucket-name-for-azure"

	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewGCPBucketWithName(a, gcpClient, bucketName)
	defer deleteGCPBucket(gcpClient, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForGCP(a, gcpClient, bucketName, "", false)
	a.NotZero(len(objectList))

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawSrcGCPBucketURL := scenarioHelper{}.getRawGCPBucketURL(a, bucketName)
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)

	raw := getDefaultRawCopyInput(rawSrcGCPBucketURL.String(), rawDstBlobServiceURLWithSAS.String())

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(objectList), len(mockedRPC.transfers))

		// Check container with resolved name has been created
		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		bsc := scenarioHelper{}.getBlobServiceClient(a)
		cc := bsc.NewContainerClient(resolvedBucketName)
		a.True(scenarioHelper{}.containerExists(cc))
		defer deleteContainer(a, cc)

		// Check correct entry are scheduled.
		// Example:
		// sourceURL pass to azcopy:  https://storage.cloud.google.om/invalid---bucket__name_for---azures2scopyfroms3toblobwithbucketna
		// destURL pass to azcopy:  https://jiacstgcanary01.blob.core.windows.net
		// transfer.Source by design be scheduled:  /tops3objects2scopyfroms3toblobwithbucketnameneedberesolved4243293354900
		// transfer.Destination by design be scheduled:  /invalid-3-bucketname-for-3-azures2scopyfroms3toblobwithbucketna/tops3objects2scopyfroms3toblobwithbucketnameneedberesolved4243293354900
		// Nothing should be replaced during matching for source, and resolved bucket name should be replaced for destination.
		validateS2STransfersAreScheduled(a, "", common.AZCOPY_PATH_SEPARATOR_STRING+resolvedBucketName, objectList, mockedRPC)
	})
}

func TestS2SCopyFromGCPToBlobWithWildcardInSrcAndBucketNameNeedBeResolved(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)
	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		t.Skip("GCP Credentials not Supplied")
	}
	invalidPrefix := "invalid----bucketname_for-azure"
	resolvedPrefix := "invalid-4-bucketname-for-azure"

	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewGCPBucketWithName(a, gcpClient, bucketName)
	defer deleteGCPBucket(gcpClient, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForGCP(a, gcpClient, bucketName, "", false)
	a.NotZero(len(objectList))

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawSrcGCPBucketURL := scenarioHelper{}.getRawGCPBucketURL(a, bucketName)
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	rawSrcGCPBucketStrWithWildcard := strings.Replace(rawSrcGCPBucketURL.String(), invalidPrefix, "invalid----*", 1)
	raw := getDefaultRawCopyInput(rawSrcGCPBucketStrWithWildcard, rawDstBlobServiceURLWithSAS.String())
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(objectList), len(mockedRPC.transfers))

		// Check container with resolved name has been created
		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		bsc := scenarioHelper{}.getBlobServiceClient(a)
		cc := bsc.NewContainerClient(resolvedBucketName)
		a.True(scenarioHelper{}.containerExists(cc))
		defer deleteContainer(a, cc)

		validateS2STransfersAreScheduled(a, common.AZCOPY_PATH_SEPARATOR_STRING+bucketName, common.AZCOPY_PATH_SEPARATOR_STRING+resolvedBucketName, objectList, mockedRPC)
	})
}

func TestS2SCopyFromGCPToBlobWithBucketNameNeedBeResolvedNegative(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)
	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		t.Skip("GCP client credentials not supplied")
	}

	invalidPrefix := "invalid_bucketname--for_azure"
	// resolvedPrefix := "invalid-bucketname-2-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewGCPBucketWithName(a, gcpClient, bucketName)
	defer deleteGCPBucket(gcpClient, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForGCP(a, gcpClient, bucketName, "", false)
	a.NotZero(len(objectList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcGCPBucketURL := scenarioHelper{}.getRawGCPBucketURL(a, bucketName)
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(a)
	raw := getDefaultRawCopyInput(rawSrcGCPBucketURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should not be resolved, and objects should not be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.NotNil(err)

		loggedError := false
		log := glcm.(*mockedLifecycleManager).infoLog
		count := len(log)
		for count > 0 {
			x := <-log
			if strings.Contains(x, "invalid name") {
				loggedError = true
			}
			count = len(log)
		}

		a.True(loggedError)
	})
}

func TestS2SCopyFromGCPToBlobWithObjectUsingSlashAsSuffix(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)
	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		t.Skip("GCP client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewGCPBucketWithName(a, gcpClient, bucketName)
	defer deleteGCPBucket(gcpClient, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"fileConsiderdAsDirectory/", "file", "sub1/file"}
	scenarioHelper{}.generateGCPObjects(a, gcpClient, bucketName, objectList)

	validateObjectList := []string{"/file", "/sub1/file"} // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcGCPBucketURL := scenarioHelper{}.getRawGCPBucketURL(a, bucketName) // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcGCPBucketURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(validateObjectList), len(mockedRPC.transfers))

		validateS2STransfersAreScheduled(a, "", "/"+bucketName, validateObjectList, mockedRPC)
	})
}

func TestS2SCopyFromGCPObjectToBlobContainer(t *testing.T) {
	a := assert.New(t)
	skipIfGCPDisabled(t)
	gcpClient, err := createGCPClientWithGCSSDK()
	if err != nil {
		t.Skip("GCP client credentials not supplied")
	}

	bucketName := generateBucketName()
	createNewGCPBucketWithName(a, gcpClient, bucketName)
	defer deleteGCPBucket(gcpClient, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateGCPObjects(a, gcpClient, bucketName, objectList)

	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	rawSrcGCPObjectURL := scenarioHelper{}.getRawGCPObjectURL(a, bucketName, "file") // Use default region

	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcGCPObjectURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	mockedRPC.reset()

	rawSrcGCPObjectURL = scenarioHelper{}.getRawGCPObjectURL(a, bucketName, "sub/file2") // Use default region
	rawDstContainerURLWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcGCPObjectURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file2", mockedRPC.transfers[0].Destination)
	})
}

// Copy from container to container, preserve blob tier.
func TestS2SCopyFromContainerToContainerPreserveBlobTier(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)

	blobName := "blobWithCoolTier"
	scenarioHelper{}.generateBlockBlobWithAccessTier(a, srcContainerClient, blobName, to.Ptr(blob.AccessTierCool))

	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateS2STransfersAreScheduled(a,
			"", "/"+srcContainerName, []string{common.AZCOPY_PATH_SEPARATOR_STRING + blobName}, mockedRPC) // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.
		a.Equal(blob.AccessTierCool, mockedRPC.transfers[0].BlobTier)
	})
}

// Copy from container to container, and don't preserve blob tier.
func TestS2SCopyFromContainerToContainerNoPreserveBlobTier(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)

	blobName := "blobWithCoolTier"
	scenarioHelper{}.generateBlockBlobWithAccessTier(a, srcContainerClient, blobName, to.Ptr(blob.AccessTierCool))

	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())
	raw.s2sPreserveAccessTier = false

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		validateS2STransfersAreScheduled(a,
			"", "/"+srcContainerName, []string{common.AZCOPY_PATH_SEPARATOR_STRING + blobName}, mockedRPC) // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.

		a.Equal(blob.AccessTier(""), mockedRPC.transfers[0].BlobTier)
	})
}

// Attempt to copy from a page blob to a block blob
func TestS2SCopyFromPageToBlockBlob(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generatePageBlobsFromList(a, srcContainerClient, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))

		a.Equal("/file2", mockedRPC.transfers[1].Destination)
	})
}

// Attempt to copy from a block blob to a page blob
func TestS2SCopyFromBlockToPageBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))

		a.Equal("/file2", mockedRPC.transfers[1].Destination)
	})
}

// Attempt to copy from a block blob to an append blob
func TestS2SCopyFromBlockToAppendBlob(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, objectList, blockBlobDefaultData)

	// Create destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))

		a.Equal("/file2", mockedRPC.transfers[1].Destination)
	})
}

// Attempt to copy from an append blob to a block blob
func TestS2SCopyFromAppendToBlockBlob(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateAppendBlobsFromList(a, srcContainerClient, objectList, appendBlobDefaultData)

	// Create destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))

		a.Equal("file2/", mockedRPC.transfers[1].Destination)
	})
}

// Attempt to copy from a page blob to an append blob
func TestS2SCopyFromPageToAppendBlob(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generatePageBlobsFromList(a, srcContainerClient, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))

		a.Equal("file2/", mockedRPC.transfers[1].Destination)
	})
}

// Attempt to copy from an append blob to a page blob
func TestS2SCopyFromAppendToPageBlob(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()

	// Generate source container and blobs
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateAppendBlobsFromList(a, srcContainerClient, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		a.Equal(2, len(mockedRPC.transfers))

		a.Equal("file2/", mockedRPC.transfers[1].Destination)
	})
}

func TestS2SCopyFromSingleBlobToBlobContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, srcContainerClient)
	a.NotNil(srcContainerClient)

	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateBlobsFromList(a, srcContainerClient, objectList, blockBlobDefaultData)

	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "file") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	mockedRPC.reset()

	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, "sub/file2") // Use default region
	rawDstContainerURLWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file2", mockedRPC.transfers[0].Destination)
	})
}

func TestS2SCopyFromSingleAzureFileToBlobContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	fsc := getFileServiceClient()

	srcShareClient, srcShareName := createNewShare(a, fsc)
	defer deleteShare(a, srcShareClient)
	a.NotNil(srcShareClient)

	scenarioHelper{}.generateFlatFiles(a, srcShareClient, []string{"file"})

	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, dstContainerClient)
	a.NotNil(dstContainerClient)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcFileURL := scenarioHelper{}.getRawFileURLWithSAS(a, srcShareName, "file") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcFileURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(1, len(mockedRPC.transfers))

		a.Equal("/file", mockedRPC.transfers[0].Destination)
	})
}

func TestCopyWithDFSResource(t *testing.T) {
	a := assert.New(t)
	// invoke the interceptor so lifecycle manager does not shut down the tests
	ctx := context.Background()

	// get service SAS for raw input
	serviceClientWithSAS := scenarioHelper{}.getDatalakeServiceClientWithSAS(a)

	// Set up source
	// set up the file system
	bfsServiceClientSource := getDatalakeServiceClient()
	fsClientSource, fsNameSource := createNewFilesystem(a, bfsServiceClientSource)
	defer deleteFilesystem(a, fsClientSource)

	// set up the parent
	parentDirNameSource := generateName("dir", 0)
	parentDirClientSource := fsClientSource.NewDirectoryClient(parentDirNameSource)
	_, err := parentDirClientSource.Create(ctx, &datalakedirectory.CreateOptions{AccessConditions: &datalakedirectory.AccessConditions{ModifiedAccessConditions: &datalakedirectory.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}})
	a.Nil(err)

	// set up the file
	fileNameSource := generateName("file", 0)
	fileClientSource, err := parentDirClientSource.NewFileClient(fileNameSource)
	a.Nil(err)
	_, err = fileClientSource.Create(ctx, nil)
	a.Nil(err)

	dirClientWithSASSource := serviceClientWithSAS.NewFileSystemClient(fsNameSource).NewDirectoryClient(parentDirNameSource)

	// Set up destination
	// set up the file system
	bfsServiceClient := getDatalakeServiceClient()
	fsClient, fsName := createNewFilesystem(a, bfsServiceClient)
	defer deleteFilesystem(a, fsClient)

	// set up the parent
	parentDirName := generateName("dir", 0)
	parentDirClient := fsClient.NewDirectoryClient(parentDirName)
	_, err = parentDirClient.Create(ctx, &datalakedirectory.CreateOptions{AccessConditions: &datalakedirectory.AccessConditions{ModifiedAccessConditions: &datalakedirectory.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}})
	a.Nil(err)

	dirClientWithSAS := serviceClientWithSAS.NewFileSystemClient(fsName).NewDirectoryClient(parentDirName)
	// =====================================

	// 1. Verify that copy between dfs and dfs works.

	rawCopy := getDefaultRawCopyInput(dirClientWithSASSource.DFSURL(), dirClientWithSAS.DFSURL())
	rawCopy.recursive = true

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	runCopyAndVerify(a, rawCopy, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(2, len(mockedRPC.transfers))

		// a.Equal("/file", mockedRPC.transfers[0].Destination)
	})

	// 2. Verify Sync between dfs and dfs works.
	mockedRPC.reset()
	// set up the file
	fileNameSource = generateName("file2", 0)
	fileClientSource, err = parentDirClientSource.NewFileClient(fileNameSource)
	a.Nil(err)
	_, err = fileClientSource.Create(ctx, nil)
	a.Nil(err)

	rawSync := getDefaultSyncRawInput(dirClientWithSASSource.DFSURL(), dirClientWithSAS.DFSURL())
	runSyncAndVerify(a, rawSync, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(2, len(mockedRPC.transfers))

		// c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file2")
	})

}
