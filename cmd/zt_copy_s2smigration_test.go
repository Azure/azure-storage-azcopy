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
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
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
	defaultS2SSourceChangeValidation = true
	debugMode                        = true // keep the debugMode temporarily, as merging happens frequently, and this might be useful for solving potential issue.
)

var defaultS2SInvalideMetadataHandleOption = common.DefaultInvalidMetadataHandleOption

func (s *cmdIntegrationSuite) SetUpSuite(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})

	// If S3 credentials aren't supplied, we're probably only trying to run Azure tests.
	// As such, gracefully return here instead of cancelling every test because we couldn't clean up S3.
	if err != nil {
		return
	}

	// Cleanup the source S3 account
	cleanS3Account(c, s3Client)
}

func getDefaultRawCopyInput(src, dst string) rawCopyCmdArgs {
	return rawCopyCmdArgs{
		src:                            src,
		dst:                            dst,
		recursive:                      true,
		logVerbosity:                   defaultLogVerbosityForCopy,
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
	}
}

func validateS2STransfersAreScheduled(c *chk.C, srcDirName string, dstDirName string, expectedTransfers []string, mockedRPC interceptor) {
	// validate that the right number of transfers were scheduled
	c.Assert(len(mockedRPC.transfers), chk.Equals, len(expectedTransfers))

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

		if debugMode {
			fmt.Println("srcRelativeFilePath: ", srcRelativeFilePath)
			fmt.Println("dstRelativeFilePath: ", dstRelativeFilePath)
		}

		// the relative paths should be equal
		c.Assert(srcRelativeFilePath, chk.Equals, dstRelativeFilePath)

		// look up the transfer is expected
		_, dstExist := lookupMap[dstRelativeFilePath]
		c.Assert(dstExist, chk.Equals, true)
	}
}

func printTransfers(ts []string) {
	for _, t := range ts {
		fmt.Println(t)
	}
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithBucketNameNeedBeResolved(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	invalidPrefix := "invalid---bucketname.for---azure"
	resolvedPrefix := "invalid-3-bucketname-for-3-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName, "", false)
	c.Assert(len(objectList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	raw := getDefaultRawCopyInput(rawSrcS3BucketURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(objectList))

		// Check container with resolved name has been created
		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		blobServiceURL := scenarioHelper{}.getBlobServiceURL(c)
		containerURL := blobServiceURL.NewContainerURL(resolvedBucketName)
		c.Assert(scenarioHelper{}.containerExists(containerURL), chk.Equals, true)
		defer deleteContainer(c, containerURL)

		// Check correct entry are scheduled.
		// Example:
		// sourceURL pass to azcopy:  https://s3.amazonaws.com/invalid---bucketname.for---azures2scopyfroms3toblobwithbucketna
		// destURL pass to azcopy:  https://jiacstgcanary01.blob.core.windows.net
		// transfer.Source by design be scheduled:  /tops3objects2scopyfroms3toblobwithbucketnameneedberesolved4243293354900
		// transfer.Destination by design be scheduled:  /invalid-3-bucketname-for-3-azures2scopyfroms3toblobwithbucketna/tops3objects2scopyfroms3toblobwithbucketnameneedberesolved4243293354900
		// Nothing should be replaced during matching for source, and resolved bucket name should be replaced for destination.
		validateS2STransfersAreScheduled(c, "", common.AZCOPY_PATH_SEPARATOR_STRING+resolvedBucketName, objectList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithWildcardInSrcAndBucketNameNeedBeResolved(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	invalidPrefix := "invalid----bucketname.for-azure"
	resolvedPrefix := "invalid-4-bucketname-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName, "", false)
	c.Assert(len(objectList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	rawSrcS3BucketStrWithWirdcard := strings.Replace(rawSrcS3BucketURL.String(), invalidPrefix, "invalid----*", 1)
	raw := getDefaultRawCopyInput(rawSrcS3BucketStrWithWirdcard, rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(objectList))

		// Check container with resolved name has been created
		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		blobServiceURL := scenarioHelper{}.getBlobServiceURL(c)
		containerURL := blobServiceURL.NewContainerURL(resolvedBucketName)
		c.Assert(scenarioHelper{}.containerExists(containerURL), chk.Equals, true)
		defer deleteContainer(c, containerURL)

		// Check correct entry are scheduled.
		// Example:
		// sourceURL pass to azcopy:  https://s3.amazonaws.com/invalid*s2scopyfroms3toblobwithwildcardi
		// destURL pass to azcopy:  https://jiacstgcanary01.blob.core.windows.net
		// transfer.Source by design be scheduled:  /invalid----bucketname.for-azures2scopyfroms3toblobwithwildcardi/sub1/sub3/sub5/s3objects2scopyfroms3toblobwithwildcardinsrcandbucketnameneedberesolved435110281300
		// transfer.Destination by design be scheduled:  /invalid-4-bucketname-for-azures2scopyfroms3toblobwithwildcardi/sub1/sub3/sub5/s3objects2scopyfroms3toblobwithwildcardinsrcandbucketnameneedberesolved435110281300
		// org bucket name should be replaced during matching for source, and resolved bucket name should be replaced for destination.
		validateS2STransfersAreScheduled(c, common.AZCOPY_PATH_SEPARATOR_STRING+bucketName, common.AZCOPY_PATH_SEPARATOR_STRING+resolvedBucketName, objectList, mockedRPC)
	})
}

// This is negative because generateBucketNameWithCustomizedPrefix will return a bucket name with length 63,
// and resolving logic will resolve -- to -2- which means the length to be 64. This exceeds valid container name, so error will be returned.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithBucketNameNeedBeResolvedNegative(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	invalidPrefix := "invalid.bucketname--for.azure"
	// resolvedPrefix := "invalid-bucketname-2-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})

	defer deleteBucket(c, s3Client, bucketName, true)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName, "", false)
	c.Assert(len(objectList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	raw := getDefaultRawCopyInput(rawSrcS3BucketURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.NotNil)
		c.Assert(err.Error(), StringIncludes, "some of the buckets have invalid names for the destination")
	})
}

// Copy from virtual directory to container, with normal encoding ' ' as ' '.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithSpaceInSrcNotEncoded(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"space dir/space object"}
	scenarioHelper{}.generateObjects(c, s3Client, bucketName, objectList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawSrcS3DirStr := rawSrcS3BucketURL.String() + "/space dir"
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3DirStr, rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
		// common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.
		// The destination is URL encoded, as go's URL method do the encoding.
		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/space%20dir/space%20object")
	})
}

// Copy from virtual directory to container, with special encoding ' ' to '+' by S3 management portal.
// '+' is handled in copy.go before extract the SourceRoot.
// The scheduled transfer would be URL encoded no matter what's the raw source/destination provided by user.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithSpaceInSrcEncodedAsPlus(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"space dir/space object"}
	scenarioHelper{}.generateObjects(c, s3Client, bucketName, objectList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawSrcS3DirStr := rawSrcS3BucketURL.String() + "/space+dir"
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3DirStr, rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)
		// common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.
		// The destination is URL encoded, as go's URL method do the encoding.
		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/space%20dir/space%20object")
	})
}

// By design, when source directory contains objects with suffix ‘/’, objects with suffix ‘/’ should be ignored.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithObjectUsingSlashAsSuffix(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"fileConsiderdAsDirectory/", "file", "sub1/file"}
	scenarioHelper{}.generateObjects(c, s3Client, bucketName, objectList)

	validateObjectList := []string{"/file", "/sub1/file"} // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3BucketURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(validateObjectList))

		validateS2STransfersAreScheduled(c, "", "/"+bucketName, validateObjectList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3AccountWithBucketInDifferentRegionsAndListUseDefaultEndpoint(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName1 := generateBucketNameWithCustomizedPrefix("default-region")
	createNewBucketWithName(c, s3Client, bucketName1, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName1, true)

	bucketName2 := generateBucketNameWithCustomizedPrefix("us-west-2-region")
	bucketRegion2 := "us-west-1" // Use different region than other regional test to avoid conflicting
	createNewBucketWithName(c, s3Client, bucketName2, createS3ResOptions{Location: bucketRegion2})
	defer deleteBucket(c, s3Client, bucketName2, true)

	objectList1 := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName1, "", true)
	c.Assert(len(objectList1), chk.Not(chk.Equals), 0)

	objectList2 := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName2, "", true)
	c.Assert(len(objectList2), chk.Not(chk.Equals), 0)

	validateObjectList := append(objectList1, objectList2...)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3AccountURL := scenarioHelper{}.getRawS3AccountURL(c, "") // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	raw := getDefaultRawCopyInput(rawSrcS3AccountURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		validateS2STransfersAreScheduled(c, "", "", validateObjectList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3AccountWithBucketInDifferentRegionsAndListUseSpecificRegion(c *chk.C) {
	specificRegion := "us-west-2"
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName1 := generateBucketNameWithCustomizedPrefix("default-region")
	createNewBucketWithName(c, s3Client, bucketName1, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName1, true)

	bucketName2 := generateBucketNameWithCustomizedPrefix(specificRegion)
	createNewBucketWithName(c, s3Client, bucketName2, createS3ResOptions{Location: specificRegion})
	defer deleteBucket(c, s3Client, bucketName2, true)

	time.Sleep(30 * time.Second) // TODO: review and remove this, which was put here as a workaround to issues with buckets being reported as not existing

	objectList1 := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName1, "", true)
	c.Assert(len(objectList1), chk.Not(chk.Equals), 0)

	objectList2 := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName2, "", true)
	c.Assert(len(objectList2), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3AccountURL := scenarioHelper{}.getRawS3AccountURL(c, specificRegion)
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	raw := getDefaultRawCopyInput(rawSrcS3AccountURL.String(), rawDstBlobServiceURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		validateS2STransfersAreScheduled(c, "", "", objectList2, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3ObjectToBlobContainer(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	if err != nil {
		c.Skip("S3 client credentials not supplied")
	}

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName, true)

	dstContainerName := generateContainerName()

	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateObjects(c, s3Client, bucketName, objectList)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3ObjectURL := scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, "file") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcS3ObjectURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	mockedRPC.reset()

	rawSrcS3ObjectURL = scenarioHelper{}.getRawS3ObjectURL(c, "", bucketName, "sub/file2") // Use default region
	rawDstContainerURLWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcS3ObjectURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file2")
	})
}

// Copy from container to container, preserve blob tier.
func (s *cmdIntegrationSuite) TestS2SCopyFromContainerToContainerPreserveBlobTier(c *chk.C) {
	bsu := getBSU()

	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)

	blobName := "blobWithCoolTier"
	scenarioHelper{}.generateBlockBlobWithAccessTier(c, srcContainerURL, blobName, azblob.AccessTierCool)

	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		validateS2STransfersAreScheduled(c,
			"", "/"+srcContainerName, []string{common.AZCOPY_PATH_SEPARATOR_STRING + blobName}, mockedRPC) // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.

		c.Assert(mockedRPC.transfers[0].BlobTier, chk.Equals, azblob.AccessTierCool)
	})
}

// Copy from container to container, and don't preserve blob tier.
func (s *cmdIntegrationSuite) TestS2SCopyFromContainerToContainerNoPreserveBlobTier(c *chk.C) {
	bsu := getBSU()

	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)

	blobName := "blobWithCoolTier"
	scenarioHelper{}.generateBlockBlobWithAccessTier(c, srcContainerURL, blobName, azblob.AccessTierCool)

	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, srcContainerName)
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcContainerURLWithSAS.String(), rawDstContainerURLWithSAS.String())
	raw.s2sPreserveAccessTier = false

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		validateS2STransfersAreScheduled(c,
			"", "/"+srcContainerName, []string{common.AZCOPY_PATH_SEPARATOR_STRING + blobName}, mockedRPC) // common.AZCOPY_PATH_SEPARATOR_STRING added for JobPartPlan file change.

		c.Assert(mockedRPC.transfers[0].BlobTier, chk.Equals, azblob.AccessTierNone)
	})
}

//Attempt to copy from a page blob to a block blob
func (s *cmdIntegrationSuite) TestS2SCopyFromPageToBlockBlob(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generatePageBlobsFromList(c, srcContainerURL, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)

		c.Assert(mockedRPC.transfers[1].Destination, chk.Equals, "/file2")
	})
}

//Attempt to copy from a block blob to a page blob
func (s *cmdIntegrationSuite) TestS2SCopyFromBlockToPageBlob(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)

		c.Assert(mockedRPC.transfers[1].Destination, chk.Equals, "/file2")
	})
}

//Attempt to copy from a block blob to an append blob
func (s *cmdIntegrationSuite) TestS2SCopyFromBlockToAppendBlob(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, objectList, blockBlobDefaultData)

	// Create destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)

		c.Assert(mockedRPC.transfers[1].Destination, chk.Equals, "/file2")
	})
}

//Attempt to copy from an append blob to a block blob
func (s *cmdIntegrationSuite) TestS2SCopyFromAppendToBlockBlob(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateAppendBlobsFromList(c, srcContainerURL, objectList, appendBlobDefaultData)

	// Create destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "BlockBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)

		c.Assert(mockedRPC.transfers[1].Destination, chk.Equals, "/file2")
	})
}

//Attempt to copy from a page blob to an append blob
func (s *cmdIntegrationSuite) TestS2SCopyFromPageToAppendBlob(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generatePageBlobsFromList(c, srcContainerURL, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "AppendBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)

		c.Assert(mockedRPC.transfers[1].Destination, chk.Equals, "/file2")
	})
}

//Attempt to copy from an append blob to a page blob
func (s *cmdIntegrationSuite) TestS2SCopyFromAppendToPageBlob(c *chk.C) {
	bsu := getBSU()

	// Generate source container and blobs
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)
	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateAppendBlobsFromList(c, srcContainerURL, objectList, pageBlobDefaultData)

	// Create destination container
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// Set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// Prepare copy command
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file")
	rawDstContainerUrlWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	// Prepare copy command
	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2")
	rawDstContainerUrlWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerUrlWithSAS.String())
	raw.blobType = "PageBlob"

	// Run copy command
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		c.Assert(len(mockedRPC.transfers), chk.Equals, 2)

		c.Assert(mockedRPC.transfers[1].Destination, chk.Equals, "/file2")
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromSingleBlobToBlobContainer(c *chk.C) {
	bsu := getBSU()

	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, srcContainerURL)
	c.Assert(srcContainerURL, chk.NotNil)

	objectList := []string{"file", "sub/file2"}
	scenarioHelper{}.generateBlobsFromList(c, srcContainerURL, objectList, blockBlobDefaultData)

	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcBlobURL := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "file") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})

	mockedRPC.reset()

	rawSrcBlobURL = scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, "sub/file2") // Use default region
	rawDstContainerURLWithSAS = scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw = getDefaultRawCopyInput(rawSrcBlobURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file2")
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromSingleAzureFileToBlobContainer(c *chk.C) {
	bsu := getBSU()
	fsu := getFSU()

	srcShareURL, srcShareName := createNewShare(c, fsu)
	defer deleteShare(c, srcShareURL)
	c.Assert(srcShareURL, chk.NotNil)

	scenarioHelper{}.generateFlatFiles(c, srcShareURL, []string{"file"})

	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	defer deleteContainer(c, dstContainerURL)
	c.Assert(dstContainerURL, chk.NotNil)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcFileURL := scenarioHelper{}.getRawFileURLWithSAS(c, srcShareName, "file") // Use default region
	rawDstContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(c, dstContainerName)
	raw := getDefaultRawCopyInput(rawSrcFileURL.String(), rawDstContainerURLWithSAS.String())

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, 1)

		c.Assert(mockedRPC.transfers[0].Destination, chk.Equals, "/file")
	})
}
