// Copyright © 2019 Microsoft <wastore@microsoft.com>
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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const customCreds string = "customS3Creds" //key for custom credentials in stored in context

type s3Traverser struct {
	rawURL        *url.URL // No pipeline needed for S3
	ctx           context.Context
	recursive     bool
	getProperties bool

	s3URLParts s3URLPartsExtension
	s3Client   *minio.Client

	// A generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	errorChannel chan TraverserErrorItemInfo
}

// ErrorFileInfo holds information about files and folders that failed enumeration.
type ErrorS3Info struct {
	S3Path             string
	S3Size             int64
	S3Name             string
	S3LastModifiedTime time.Time
	ErrorMsg           error
	Source             bool
	Dir                bool
}

// Compile-time check to ensure ErrorFileInfo implements TraverserErrorItemInfo
var _ TraverserErrorItemInfo = (*ErrorS3Info)(nil)

///////////////////////////////////////////////////////////////////////////
// START - Implementing methods defined in TraverserErrorItemInfo

func (e ErrorS3Info) FullPath() string {
	return e.S3Path
}

func (e ErrorS3Info) Name() string {
	return e.S3Name
}

func (e ErrorS3Info) Size() int64 {
	return e.S3Size
}

func (e ErrorS3Info) LastModifiedTime() time.Time {
	return e.S3LastModifiedTime
}

func (e ErrorS3Info) IsDir() bool {
	return e.Dir
}

func (e ErrorS3Info) ErrorMessage() error {
	return e.ErrorMsg
}

func (e ErrorS3Info) IsSource() bool {
	return e.Source
}

// END - Implementing methods defined in TraverserErrorItemInfo
// /////////////////////////////////////////////////////////////////////////
func writeToS3ErrorChannel(errorChannel chan TraverserErrorItemInfo, err ErrorS3Info) {
	if errorChannel != nil {
		select {
		case errorChannel <- err:
		default:
			// Channel might be full, log the error instead
			WarnStdoutAndScanningLog(fmt.Sprintf("Failed to send error to channel: %v", err.ErrorMessage()))
		}
	}
}

func (t *s3Traverser) IsDirectory(isSource bool) (bool, error) {
	// Do a basic syntax check
	isDirDirect := !t.s3URLParts.IsObjectSyntactically() && (t.s3URLParts.IsDirectorySyntactically() || t.s3URLParts.IsBucketSyntactically())

	// S3 can convert directories and objects sharing names as well.
	if !isSource {
		return isDirDirect, nil
	}

	_, err := t.s3Client.StatObject(context.Background(), t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})

	if err != nil {
		return true, err
	}

	return false, nil
}

func (t *s3Traverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	invalidAzureBlobName := func(objectKey string) bool {
		/* S3 object name is invalid if it ends with period or
		   one of virtual directories in path ends with period.
		   This list is not exhaustive
		*/
		return strings.HasSuffix(objectKey, ".") ||
			strings.Contains(objectKey, "./")
	}
	invalidNameErrorMsg := "Skipping S3 object %s, as it is not a valid Blob name. Rename the object and retry the transfer"
	// Check if resource is a single object.
	if t.s3URLParts.IsObjectSyntactically() && !t.s3URLParts.IsDirectorySyntactically() && !t.s3URLParts.IsBucketSyntactically() {
		objectPath := strings.Split(t.s3URLParts.ObjectKey, "/")
		objectName := objectPath[len(objectPath)-1]
		oi, err := t.s3Client.StatObject(context.Background(), t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})
		if invalidAzureBlobName(t.s3URLParts.ObjectKey) {
			errorS3Info := ErrorS3Info{
				S3Name:             objectName,
				S3Path:             t.s3URLParts.ObjectKey,
				S3LastModifiedTime: oi.LastModified,
				S3Size:             oi.Size,
				ErrorMsg:           err,
				Source:             true,
			}
			writeToS3ErrorChannel(t.errorChannel, errorS3Info)
			WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, t.s3URLParts.ObjectKey))
			return common.EAzError.InvalidBlobName()
		}

		// If we actually got object properties, process them.
		// Otherwise, treat it as a directory.
		// According to IsDirectorySyntactically, objects and folders can share names
		if err == nil {
			// We had to statObject anyway, get ALL the info.
			oie := common.ObjectInfoExtension{ObjectInfo: oi}
			storedObject := newStoredObject(
				preprocessor,
				objectName,
				"",
				common.EEntityType.File(),
				oi.LastModified,
				oi.Size,
				&oie,
				noBlobProps,
				oie.NewCommonMetadata(),
				t.s3URLParts.BucketName)

			err = processIfPassedFilters(
				filters,
				storedObject,
				processor)
			_, err = getProcessingError(err)
			if err != nil {
				errorS3Info := ErrorS3Info{
					S3Name:             objectName,
					S3Path:             t.s3URLParts.ObjectKey,
					ErrorMsg:           err,
					Source:             true,
					S3LastModifiedTime: oi.LastModified,
					S3Size:             oi.Size,
				}
				writeToS3ErrorChannel(t.errorChannel, errorS3Info)
				return err
			}

			return nil
		}
	}

	// Append a trailing slash if it is missing.
	if !strings.HasSuffix(t.s3URLParts.ObjectKey, "/") && t.s3URLParts.ObjectKey != "" {
		t.s3URLParts.ObjectKey += "/"
	}

	// Ignore *s in URLs and treat them as normal characters
	// This is because * is both a valid URL path character and a valid portion of an object key in S3.
	searchPrefix := t.s3URLParts.ObjectKey

	opts := minio.ListObjectsOptions{
		Recursive: t.recursive,
		Prefix:    searchPrefix,
	}

	// It's a bucket or virtual directory.
	for objectInfo := range t.s3Client.ListObjects(t.ctx, t.s3URLParts.BucketName, opts) {

		// re-join the unescaped path.
		relativePath := strings.TrimPrefix(objectInfo.Key, searchPrefix)

		// Ignoring this object because it is a zero-byte placeholder typically created to simulate a folder in S3-compatible storage.
		// These objects have an empty RelativePath and are marked as files, but they do not represent actual user data.
		// Including them in processing could lead to incorrect assumptions about file presence or structure.
		if len(relativePath) == 0 && objectInfo.StorageClass != "" {
			continue
		}

		if objectInfo.Err != nil {
			errorS3Info := ErrorS3Info{
				S3Name:             objectInfo.Key,
				Source:             true,
				S3Size:             objectInfo.Size,
				S3LastModifiedTime: objectInfo.LastModified,
				S3Path:             t.s3URLParts.ObjectKey,
				ErrorMsg:           objectInfo.Err,
			}
			writeToS3ErrorChannel(t.errorChannel, errorS3Info)
			return fmt.Errorf("cannot list objects, %v", objectInfo.Err)
		}

		if invalidAzureBlobName(objectInfo.Key) {
			//Throw a warning on console and continue
			WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, objectInfo.Key))
			errorS3Info := ErrorS3Info{
				S3Name:             objectInfo.Key,
				Source:             true,
				S3Size:             objectInfo.Size,
				S3LastModifiedTime: objectInfo.LastModified,
				S3Path:             t.s3URLParts.ObjectKey,
				ErrorMsg:           objectInfo.Err,
			}
			writeToS3ErrorChannel(t.errorChannel, errorS3Info)
			continue
		}

		objectPath := strings.Split(objectInfo.Key, "/")
		objectName := objectPath[len(objectPath)-1]

		var storedObject StoredObject
		if objectInfo.StorageClass == "" {

			// Directories are the only objects without storage classes.
			if !UseSyncOrchestrator {
				// Skip directories if not using sync orchestrator
				continue
			} else {
				// For sync orchestrator, we need to treat directories as objects.
				storedObject = newStoredObject(
					preprocessor,
					objectName,
					relativePath,
					common.EEntityType.Folder(),
					objectInfo.LastModified,
					0,
					noContentProps,
					noBlobProps,
					noMetadata,
					t.s3URLParts.BucketName)
			}

		} else {
			if strings.HasSuffix(relativePath, "/") {
				// If a file has a suffix of /, it's still treated as a folder.
				// Thus, akin to the old code. skip it.
				// NOTE: In case of UseSyncOrchestrator, what do we do??
				continue
			}

			// default to empty props, but retrieve real ones if required
			oie := common.ObjectInfoExtension{ObjectInfo: minio.ObjectInfo{}}

			// If we are using sync orchestrator, we don't need to retrieve properties.
			if !UseSyncOrchestrator && t.getProperties {
				oi, err := t.s3Client.StatObject(context.Background(), t.s3URLParts.BucketName, objectInfo.Key, minio.StatObjectOptions{})
				if err != nil {
					errorS3Info := ErrorS3Info{
						S3Name:             objectName,
						S3Path:             t.s3URLParts.ObjectKey,
						ErrorMsg:           err,
						Source:             true,
						S3LastModifiedTime: oi.LastModified,
						S3Size:             oi.Size,
					}
					writeToS3ErrorChannel(t.errorChannel, errorS3Info)
					return err
				}
				oie = common.ObjectInfoExtension{ObjectInfo: oi}
			}

			storedObject = newStoredObject(
				preprocessor,
				objectName,
				relativePath,
				common.EEntityType.File(),
				objectInfo.LastModified,
				objectInfo.Size,
				&oie,
				noBlobProps,
				oie.NewCommonMetadata(),
				t.s3URLParts.BucketName)
		}

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(storedObject.entityType)
		}
		err = processIfPassedFilters(filters,
			storedObject,
			processor)
		_, err = getProcessingError(err)
		if err != nil {
			errorS3Info := ErrorS3Info{
				S3Name:             objectName,
				S3Path:             t.s3URLParts.ObjectKey,
				ErrorMsg:           err,
				Source:             true,
				S3LastModifiedTime: storedObject.lastModifiedTime,
				S3Size:             storedObject.size,
			}
			writeToS3ErrorChannel(t.errorChannel, errorS3Info)
			return
		}
	}
	return
}

func newS3Traverser(credentialType common.CredentialType, rawURL *url.URL, ctx context.Context, recursive, getProperties bool,
	incrementEnumerationCounter enumerationCounterFunc) (t *s3Traverser, err error) {
	t = &s3Traverser{rawURL: rawURL, ctx: ctx, recursive: recursive, getProperties: getProperties,
		incrementEnumerationCounter: incrementEnumerationCounter}

	// initialize S3 client and URL parts
	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*t.rawURL)

	if err != nil {
		return
	} else {
		t.s3URLParts = s3URLPartsExtension{s3URLParts}
	}

	showS3UrlTypeWarning(s3URLParts)

	//Optional check for custom credential provider
	var credProvider credentials.Provider = nil
	creds := ctx.Value(customCreds)
	if creds != nil {
		credProvider = creds.(credentials.Provider) //if passed through context, use custom provider
	}

	t.s3Client, err = common.CreateS3Client(t.ctx, common.CredentialInfo{
		CredentialType: credentialType,
		S3CredentialInfo: common.S3CredentialInfo{
			Endpoint: t.s3URLParts.Endpoint,
			Region:   t.s3URLParts.Region,
			Provider: credProvider, //will pass nil in most cases, but if provider is implemented and passed explicitly, it will be used
		},
	}, common.CredentialOpOptions{
		LogError: glcm.Error,
	}, azcopyScanningLogger)

	return
}

// Discourage the non-region aware URL type. (but don't ban it, because that breaks almost all our S3 automated tests)
// Reason is that we had intermittent bucket location lookup issues when using that technique
// with the apparent cause being the lookup of bucket locations, which we need to change to use minio's BucketExists.
// For info see: https://github.com/aws/aws-sdk-go/issues/720#issuecomment-243891223
// Once we change to bucketExists, assuming its reliable, we will be able to re allow this URL type.
func showS3UrlTypeWarning(s3URLParts common.S3URLParts) {
	if strings.EqualFold(s3URLParts.Host, "s3.amazonaws.com") {
		s3UrlWarningOncer.Do(func() {
			glcm.Info("Instead of transferring from the 's3.amazonaws.com' URL, in this version of AzCopy we recommend you " +
				"use a region-specific endpoint to transfer from one specific region. E.g. s3.us-east-1.amazonaws.com or a virtual-hosted reference to a single bucket.")
		})
	}
}

var s3UrlWarningOncer = &sync.Once{}
