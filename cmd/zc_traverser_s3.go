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

	"github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type s3Traverser struct {
	rawURL        *url.URL // No pipeline needed for S3
	ctx           context.Context
	recursive     bool
	getProperties bool

	s3URLParts s3URLPartsExtension
	s3Client   *minio.Client

	// A generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc
}

func (t *s3Traverser) IsDirectory(isSource bool) (bool, error) {
	// Do a basic syntax check
	isDirDirect := !t.s3URLParts.IsObjectSyntactically() && (t.s3URLParts.IsDirectorySyntactically() || t.s3URLParts.IsBucketSyntactically())

	// S3 can convert directories and objects sharing names as well.
	if !isSource {
		return isDirDirect, nil
	}

	_, err := t.s3Client.StatObject(t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})

	if err != nil {
		return true, err
	}

	return false, nil
}

func (t *s3Traverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) (err error) {
	p := processor
	processor = func(storedObject StoredObject) error {
		t.incrementEnumerationCounter(storedObject.entityType)

		return p(storedObject)
	}

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

		oi, err := t.s3Client.StatObject(t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})
		if invalidAzureBlobName(t.s3URLParts.ObjectKey) {
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

	// It's a bucket or virtual directory.
	for objectInfo := range t.s3Client.ListObjectsV2(t.s3URLParts.BucketName, searchPrefix, t.recursive, t.ctx.Done()) {
		if objectInfo.Err != nil {
			return fmt.Errorf("cannot list objects, %v", objectInfo.Err)
		}

		if objectInfo.StorageClass == "" {
			// Directories are the only objects without storage classes.
			continue
		}

		if invalidAzureBlobName(objectInfo.Key) {
			//Throw a warning on console and continue
			WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, objectInfo.Key))
			continue
		}

		objectPath := strings.Split(objectInfo.Key, "/")
		objectName := objectPath[len(objectPath)-1]

		// re-join the unescaped path.
		relativePath := strings.TrimPrefix(objectInfo.Key, searchPrefix)

		if strings.HasSuffix(relativePath, "/") {
			// If a file has a suffix of /, it's still treated as a folder.
			// Thus, akin to the old code. skip it.
			continue
		}

		// default to empty props, but retrieve real ones if required
		oie := common.ObjectInfoExtension{ObjectInfo: minio.ObjectInfo{}}
		if t.getProperties {
			oi, err := t.s3Client.StatObject(t.s3URLParts.BucketName, objectInfo.Key, minio.StatObjectOptions{})
			if err != nil {
				return err
			}
			oie = common.ObjectInfoExtension{ObjectInfo: oi}
		}
		storedObject := newStoredObject(
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

		err = processIfPassedFilters(filters,
			storedObject,
			processor)
		_, err = getProcessingError(err)
		if err != nil {
			return
		}
	}
	return
}

func newS3Traverser(rawURL *url.URL, opts InitResourceTraverserOptions) (t *s3Traverser, err error) {
	t = &s3Traverser{rawURL: rawURL, ctx: opts.Context, recursive: opts.Recursive, getProperties: opts.GetPropertiesInFrontend,
		incrementEnumerationCounter: opts.IncrementEnumeration}

	// initialize S3 client and URL parts
	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*t.rawURL)

	if err != nil {
		return
	} else {
		t.s3URLParts = s3URLPartsExtension{s3URLParts}
	}

	showS3UrlTypeWarning(s3URLParts)

	t.s3Client, err = common.CreateS3Client(t.ctx, common.CredentialInfo{
		CredentialType: opts.Credential.CredentialType,
		S3CredentialInfo: common.S3CredentialInfo{
			Endpoint: t.s3URLParts.Endpoint,
			Region:   t.s3URLParts.Region,
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
