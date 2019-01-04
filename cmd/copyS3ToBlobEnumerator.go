package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/s3utils"
)

// copyS3ToBlobEnumerator enumerates S3 source, and submit request for copy S3 to Blob.
// The source could be point to S3 object or bucket or service.
// This enumerator can be easily extend to copyS3ToNEnumerator.
type copyS3ToBlobEnumerator struct {
	copyS2SEnumerator

	// source S3 resources
	s3Client   *minio.Client
	s3URLParts S3URLParts
}

// By default presign expires after 7 days, which should be enough for millions of files transfer.
const defaultPresignExpires = time.Hour * 24 * 7

func (e *copyS3ToBlobEnumerator) initEnumerator(ctx context.Context, cca *cookedCopyCmdArgs) (err error) {
	// attempt to parse the source and destination url
	if e.sourceURL, err = url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.source)); err != nil {
		return errors.New("cannot parse source URL")
	}
	if e.destURL, err = url.Parse(gCopyUtil.replaceBackSlashWithSlash(cca.destination)); err != nil {
		return errors.New("cannot parse destination URL")
	}
	e.destURL = gCopyUtil.appendQueryParamToUrl(e.destURL, cca.destinationSAS)

	// Check whether the source URL is a valid S3 URL, and parse URL parts.
	if e.s3URLParts, err = NewS3URLParts(*e.sourceURL); err != nil {
		return err
	}

	if e.s3Client, err = createS3Client(ctx, common.CredentialInfo{
		CredentialType: common.ECredentialType.S3AccessKey(), // Currently only support access key
		S3CredentialInfo: common.S3CredentialInfo{
			Endpoint: e.s3URLParts.Endpoint,
			Region:   e.s3URLParts.Region,
		},
	}); err != nil {
		return err
	}

	if err = e.initDestPipeline(ctx); err != nil {
		return err
	}

	return
}

func (e *copyS3ToBlobEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	ctx := context.TODO() // This would better be singleton in cmd module, and passed from caller.

	if err := e.initEnumerator(ctx, cca); err != nil {
		return err
	}

	// Start enumerating.
	// Case-1: Source is a single object
	// Verify if source is a single object, note that s3URLParts only verifies resource type from URL syntax.
	if e.s3URLParts.IsObject() && !e.s3URLParts.IsDirectory() {
		// TODO: Consider to add new method with context to minio and pass-in valid ctx.
		if objectInfo, err := e.s3Client.StatObject(e.s3URLParts.BucketName, e.s3URLParts.ObjectKey, minio.StatObjectOptions{}); err == nil {
			// Note: Currently only support single to single, and not support single to directory.
			if endWithSlashOrBackSlash(e.destURL.Path) {
				return errors.New("invalid source and destination combination for service to service copy: " +
					"destination must point to a single file, when source is a single file.")
			}
			err := e.createDestBucket(ctx, *e.destURL, nil)
			if err != nil {
				return err
			}

			// directly use destURL as destination
			if err := e.addObjectToNTransfer(*e.sourceURL, *e.destURL, &objectInfo, cca); err != nil {
				return err
			}
			return e.dispatchFinalPart(cca)
		}
	}

	// Case-2: Source is a service endpoint
	if isServiceLevel, bucketPrefix := e.s3URLParts.IsServiceLevelSearch(); isServiceLevel {
		if !cca.recursive {
			return fmt.Errorf("cannot copy the entire account without recursive flag. Please use --recursive flag")
		}

		// Validate if destination is service level account.
		if err := e.validateDestIsService(ctx, *e.destURL); err != nil {
			return err
		}

		objectPrefix, objectPattern, _ := e.s3URLParts.searchObjectPrefixAndPatternFromS3URL()
		// List buckets and add transfers for these buckets.
		if err := e.addTransferFromService(ctx, e.s3Client, *e.destURL,
			bucketPrefix, objectPrefix, objectPattern, cca); err != nil {
			return err
		}
	} else { // Case-3: Source is a bucket or virutal directory
		// Ensure there is a valid bucket name in this case.
		if err := s3utils.CheckValidBucketNameStrict(e.s3URLParts.BucketName); err != nil {
			return err
		}

		objectPrefix, objectPattern, isWildcardSearch := e.s3URLParts.searchObjectPrefixAndPatternFromS3URL()
		if objectPattern == "*" && !cca.recursive && !isWildcardSearch {
			return fmt.Errorf("cannot copy the entire bucket or directory without recursive flag. Please use --recursive flag")
		}

		// Check if destination is point to an Azure service.
		// If destination is an Azure service, azcopy tries to create a bucket(container, share or etc) with source's bucket name,
		// and then copy from source bucket to created destination bucket(container, share or etc).
		// Otherwise, if source is a bucket/virtual directory and destination is a bucket/virtual directory,
		// azcopy do copy from source bucket/virtual directory to destination bucket/virtual directory.
		if err := e.validateDestIsService(ctx, *e.destURL); err == nil {
			// name resolver is used only when the target URL is inferred from source URL.
			s3BucketNameResolver := NewS3BucketNameToAzureResourcesResolver([]string{e.s3URLParts.BucketName})
			resolvedBucketName, err := s3BucketNameResolver.ResolveName(e.s3URLParts.BucketName)
			if err != nil {
				glcm.Error(err.Error())
				return errors.New("fail to add transfer, the source bucket has invalid name for Azure. \n" +
					"please copy from bucket to Azure with customized container/share/filesystem name.")
			}

			*e.destURL = urlExtension{*e.destURL}.generateObjectPath(resolvedBucketName)
		}

		// create bucket for destination, in case bucket doesn't exist.
		if err := e.createDestBucket(ctx, *e.destURL, nil); err != nil {
			return err
		}

		if err := e.addTransfersFromBucket(ctx, e.s3Client, *e.destURL,
			e.s3URLParts.BucketName,
			objectPrefix,
			objectPattern,
			e.s3URLParts.getParentSourcePath(),
			false,
			isWildcardSearch,
			cca); err != nil {
			return err
		}
	}

	// If part number is 0 && number of transfer queued is 0
	// it means that no job part has been dispatched and there are no
	// transfer in Job to dispatch a JobPart.
	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return fmt.Errorf("no transfer queued to copy. Please verify the source / destination")
	}

	// dispatch the JobPart as Final Part of the Job
	return e.dispatchFinalPart(cca)
}

// addTransferFromService enumerates buckets in service, and adds matched file into transfer.
func (e *copyS3ToBlobEnumerator) addTransferFromService(ctx context.Context,
	s3Client *minio.Client, destBaseURL url.URL,
	bucketPrefix, objectPrefix, objectPattern string, cca *cookedCopyCmdArgs) error {

	// Bucket name resolving, if there is any successful or failed naming resolution, print to customer.
	bucketsResolveFunc := func(bucketInfos []minio.BucketInfo) ([]minio.BucketInfo, error) {
		var bucketNames []string
		for _, bucketInfo := range bucketInfos {
			bucketNames = append(bucketNames, bucketInfo.Name)
		}
		r := NewS3BucketNameToAzureResourcesResolver(bucketNames)

		resolveErr := false
		for _, bucketInfo := range bucketInfos {
			if resolvedName, err := r.ResolveName(bucketInfo.Name); err != nil {
				// For resolving failure, show it to customer.
				glcm.Error(err.Error())
				resolveErr = true
			} else {
				if resolvedName != bucketInfo.Name {
					glcm.Info(fmt.Sprintf("s3 bucket name %q is invalid for Azure container/share/filesystem, and has been renamed to %q", bucketInfo.Name, resolvedName))
				}
				bucketInfo.Name = resolvedName
			}
		}

		if resolveErr {
			return nil, errors.New("fail to add transfers from service, some of the buckets have invalid names for Azure. \n" +
				"please exclude the invalid buckets in service to service copy, and copy them use bucket to container/share/filesystem copy " +
				"with customized destination name after the service to service copy finished")
		}

		return bucketInfos, nil
	}

	bucketFilter := func(bucketInfo minio.BucketInfo) bool {
		if strings.HasPrefix(bucketInfo.Name, bucketPrefix) {
			return true
		}
		return false
	}

	bucketAction := func(bucketInfo minio.BucketInfo) error {
		// Whatever the destination type is, it should be equivalent to account level,
		// so directly append bucket name to it.
		tmpDestURL := urlExtension{URL: destBaseURL}.generateObjectPath(bucketInfo.Name)
		// create bucket for destination, in case bucket doesn't exist.
		if err := e.createDestBucket(ctx, tmpDestURL, nil); err != nil {
			return err
		}

		// Two cases for exclude/include which need to match bucket names in account:
		// a. https://<service>/bucket*/obj*
		// b. https://<service>/ which equals to https://<service>/*
		return e.addTransfersFromBucket(ctx, s3Client, tmpDestURL, bucketInfo.Name, objectPrefix, objectPattern, "", true, true, cca)
	}

	return enumerateBucketsInServiceWithMinio(ctx, s3Client, bucketsResolveFunc, bucketFilter, bucketAction)
}

// addTransfersFromBucket enumerates objects in bucket,
// and adds matched objects into transfer.
func (e *copyS3ToBlobEnumerator) addTransfersFromBucket(ctx context.Context,
	s3Client *minio.Client, destBaseURL url.URL,
	bucketName, objectNamePrefix, objectNamePattern, parentSourcePath string,
	includExcludeBucket, isWildcardSearch bool, cca *cookedCopyCmdArgs) error {

	// object filter selects objects need to be transferred.
	objectFilter := func(objectInfo minio.ObjectInfo) bool {
		// As design discussion, skip the object with suffix "/", which indicates the object represents a directory in S3 management console,
		// considering there is no directory in Azure blob.
		if strings.HasSuffix(objectInfo.Key, "/") {
			return false
		}

		// Check if object name matches pattern.
		if !gCopyUtil.matchBlobNameAgainstPattern(objectNamePattern, objectInfo.Key, cca.recursive) {
			return false
		}

		includeExcludeMatchPath := common.IffString(includExcludeBucket,
			bucketName+"/"+objectInfo.Key,
			objectInfo.Key)
		// Check the object should be included or not.
		if !gCopyUtil.resourceShouldBeIncluded(parentSourcePath, e.Include, includeExcludeMatchPath) {
			return false
		}

		// Check the object should be excluded or not.
		if gCopyUtil.resourceShouldBeExcluded(parentSourcePath, e.Exclude, includeExcludeMatchPath) {
			return false
		}

		return true
	}

	// defines action need be fulfilled to add selected object into transfer
	objectAction := func(objectInfo minio.ObjectInfo) error {
		var objectRelativePath = ""
		if isWildcardSearch {
			objectRelativePath = strings.Replace(objectInfo.Key,
				objectNamePrefix[:strings.LastIndex(objectNamePrefix, common.AZCOPY_PATH_SEPARATOR_STRING)+1], "", 1)
		} else {
			objectRelativePath = gCopyUtil.getRelativePath(objectNamePrefix, objectInfo.Key)
		}

		// S3's list operations doesn't return object's properties, such as: content-encoding and etc.
		// So azcopy need additional get request to collect these properties.
		if cca.preserveProperties {
			var err error
			objectInfo, err = s3Client.StatObject(bucketName, objectInfo.Key, minio.StatObjectOptions{})
			if err != nil {
				return err
			}
		}

		// Presign the get URL for S2S copy
		presignedURL, err := s3Client.PresignedGetObject(bucketName, objectInfo.Key, defaultPresignExpires, url.Values{})
		if err != nil {
			return err
		}

		return e.addObjectToNTransfer(
			*presignedURL,
			urlExtension{URL: destBaseURL}.generateObjectPath(objectRelativePath),
			&objectInfo,
			cca)
	}

	// enumerate objects in bucket, and add matched objects into transfer.
	err := enumerateObjectsInBucketWithMinio(ctx, s3Client, bucketName, objectNamePrefix, objectFilter, objectAction)
	if err != nil {
		// Handle the error that fail to list objects in bucket due to Location mismatch, which is caused by source endpoint doesn't match S3 buckets' regions
		if strings.Contains(err.Error(), "301 response missing Location header") {
			glcm.Info(fmt.Sprintf("skip the bucket %q, as it's not in the region specified by source URL", bucketName))
		} else {
			return err
		}
	}

	return nil
}

func (e *copyS3ToBlobEnumerator) addObjectToNTransfer(srcURL, destURL url.URL, objectInfo *minio.ObjectInfo,
	cca *cookedCopyCmdArgs) error {
	oie := objectInfoExtension{*objectInfo}

	copyTransfer := common.CopyTransfer{
		Source:             srcURL.String(),
		Destination:        gCopyUtil.stripSASFromBlobUrl(destURL).String(),
		LastModifiedTime:   objectInfo.LastModified,
		SourceSize:         objectInfo.Size,
		ContentType:        objectInfo.ContentType,
		ContentEncoding:    oie.ContentEncoding(),
		ContentDisposition: oie.ContentDisposition(),
		ContentLanguage:    oie.ContentLanguage(),
		CacheControl:       oie.CacheControl(),
		ContentMD5:         oie.ContentMD5(),
		Metadata:           oie.NewCommonMetadata()}

	fmt.Println("Trying to schedule CopyTransfer: ", copyTransfer)

	return e.addTransfer(copyTransfer, cca)
}

func (e *copyS3ToBlobEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer(&(e.CopyJobPartOrderRequest), transfer, cca)
}

func (e *copyS3ToBlobEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart(&(e.CopyJobPartOrderRequest), cca)
}

func (e *copyS3ToBlobEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
