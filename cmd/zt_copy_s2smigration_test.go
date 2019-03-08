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
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/jiacfan/azure-storage-blob-go/azblob"
	chk "gopkg.in/check.v1"
)

// Additional S2S migration cases, besides E2E smoke testing cases for S3/blob/file source contained in test_service_to_service_copy.py

const (
	defaultLogVerbosityForCopy         = "WARNING"
	defaultOutputFormatForCopy         = "text"
	defaultBlobTypeForCopy             = "None"
	defaultBlockBlobTierForCopy        = "None"
	defaultPageBlobTierForCopy         = "None"
	defaultS2SPreserveProperties       = true
	defaultS2SPreserveAccessTier       = true
	defaultS2SGetS3PropertiesInBackend = true
	defaultS2SSourceChangeValidation = true
)

func getDefaultRawCopyInput(src, dst string) rawCopyCmdArgs {
	return rawCopyCmdArgs{
		src:                         src,
		dst:                         dst,
		recursive:                   true,
		logVerbosity:                defaultLogVerbosityForSync,
		output:                      defaultOutputFormatForSync,
		blobType:                    defaultBlobTypeForCopy,
		blockBlobTier:               defaultBlockBlobTierForCopy,
		pageBlobTier:                defaultPageBlobTierForCopy,
		md5ValidationOption:         common.DefaultHashValidationOption.String(),
		s2sGetS3PropertiesInBackend: defaultS2SGetS3PropertiesInBackend,
		s2sPreserveAccessTier:       defaultS2SPreserveAccessTier,
		s2sPreserveProperties:       defaultS2SPreserveProperties,
		s2sSourceChangeValidation: defaultS2SSourceChangeValidation,
	}
}

func runCopyAndVerify(c *chk.C, raw rawCopyCmdArgs, verifier func(err error)) {
	// the simulated user input should parse properly
	cooked, err := raw.cook()
	c.Assert(err, chk.IsNil)

	// the enumeration ends when process() returns
	err = cooked.process()

	// the err is passed to verified, which knows whether it is expected or not
	verifier(err)
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithBucketNameNeedBeResolved(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	invalidPrefix := "invalid---bucketname.for---azure"
	resolvedPrefix := "invalid-3-bucketname-for-3-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName, "")
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

		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		blobServiceURL := scenarioHelper{}.getBlobServiceURL(c)
		containerURL := blobServiceURL.NewContainerURL(resolvedBucketName)
		c.Assert(scenarioHelper{}.containerExists(containerURL), chk.Equals, true)
		defer deleteContainer(c, containerURL)

		validateS2SCopyTransfersAreScheduled(c, rawSrcS3BucketURL.String(), containerURL.String(), objectList, mockedRPC)
	})
}

func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithWildcardInSrcAndBucketNameNeedBeResolved(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	invalidPrefix := "invalid----bucketname.for-azure"
	resolvedPrefix := "invalid-4-bucketname-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName, "")
	c.Assert(len(objectList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcS3BucketURL := scenarioHelper{}.getRawS3BucketURL(c, "", bucketName) // Use default region
	rawDstBlobServiceURLWithSAS := scenarioHelper{}.getRawBlobServiceURLWithSAS(c)
	rawSrcS3BucketStrWithWirdcard := strings.Replace(rawSrcS3BucketURL.String(), invalidPrefix, "invalid*", 1)
	raw := getDefaultRawCopyInput(rawSrcS3BucketStrWithWirdcard, rawDstBlobServiceURLWithSAS.String())

	fmt.Println(raw.src)

	// bucket should be resolved, and objects should be scheduled for transfer
	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(objectList))

		resolvedBucketName := strings.Replace(bucketName, invalidPrefix, resolvedPrefix, 1)
		blobServiceURL := scenarioHelper{}.getBlobServiceURL(c)
		containerURL := blobServiceURL.NewContainerURL(resolvedBucketName)
		c.Assert(scenarioHelper{}.containerExists(containerURL), chk.Equals, true)
		defer deleteContainer(c, containerURL)

		validateS2SCopyTransfersAreScheduled(c, rawSrcS3BucketURL.String(), containerURL.String(), objectList, mockedRPC)
	})
}

// This is negative because generateBucketNameWithCustomizedPrefix will return a bucket name with length 63,
// and resolving logic will resolve -- to -2- which means the length to be 64. This exceeds valid container name, so error will be returned.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithBucketNameNeedBeResolvedNegative(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	invalidPrefix := "invalid.bucketname--for.azure"
	// resolvedPrefix := "invalid-bucketname-2-for-azure"

	// Generate source bucket
	bucketName := generateBucketNameWithCustomizedPrefix(invalidPrefix)
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName)

	objectList := scenarioHelper{}.generateCommonRemoteScenarioForS3(c, s3Client, bucketName, "")
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
		// TODO: possibly check detailed error messages
	})
}

// Copy from virtual directory to container, with normal encoding ' ' as ' '.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithSpaceInSrcNotEncoded(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName)

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

		containerURL := scenarioHelper{}.getContainerURL(c, dstContainerName)
		// Note: for copy if recursive is turned on, source dir will be created in destination, so use bucket URL as base for comparison.
		validateS2SCopyTransfersAreScheduled(c, rawSrcS3BucketURL.String(), containerURL.String(), objectList, mockedRPC)
	})
}

// Copy from virtual directory to container, with special encoding ' ' to '+' by S3 management portal.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithSpaceInSrcEncodedAsPlus(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName)

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

		containerURL := scenarioHelper{}.getContainerURL(c, dstContainerName)
		// Note: for copy if recursive is turned on, source dir will be created in destination, so use bucket URL as base for comparison.
		validateS2SCopyTransfersAreScheduled(c, rawSrcS3BucketURL.String(), containerURL.String(), objectList, mockedRPC)
	})
}

// By design, when source directory contains objects with suffix ‘/’, objects with suffix ‘/’ should be ignored.
func (s *cmdIntegrationSuite) TestS2SCopyFromS3ToBlobWithObjectUsingSlashAsSuffix(c *chk.C) {
	s3Client, err := createS3ClientWithMinio(createS3ResOptions{})
	c.Assert(err, chk.IsNil)

	// Generate source bucket
	bucketName := generateBucketName()
	createNewBucketWithName(c, s3Client, bucketName, createS3ResOptions{})
	defer deleteBucket(c, s3Client, bucketName)

	dstContainerName := generateContainerName()

	objectList := []string{"fileConsiderdAsDirectory/", "file", "sub1/file"}
	scenarioHelper{}.generateObjects(c, s3Client, bucketName, objectList)

	validateObjectList := []string{"file", "sub1/file"}

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

		containerURL := scenarioHelper{}.getContainerURL(c, dstContainerName)
		validateS2SCopyTransfersAreScheduled(c, rawSrcS3BucketURL.String(), containerURL.String(), validateObjectList, mockedRPC)
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

		validateS2SCopyTransfersAreScheduled(c,
			srcContainerURL.String(), dstContainerURL.String(), []string{blobName}, mockedRPC)

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

		validateS2SCopyTransfersAreScheduled(c,
			srcContainerURL.String(), dstContainerURL.String(), []string{blobName}, mockedRPC)

		c.Assert(mockedRPC.transfers[0].BlobTier, chk.Equals, azblob.AccessTierNone)
	})
}
