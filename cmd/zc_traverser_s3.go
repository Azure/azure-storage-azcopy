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

	"github.com/Azure/azure-storage-azcopy/common"
)

type s3Traverser struct {
	rawURL        *url.URL // No pipeline needed for S3
	ctx           context.Context
	recursive     bool
	getProperties bool

	s3URLParts s3URLPartsExtension
	s3Client   *minio.Client

	// A generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *s3Traverser) isDirectory(isSource bool) bool {
	// Do a basic syntax check
	isDirDirect := !t.s3URLParts.IsObjectSyntactically() && (t.s3URLParts.IsDirectorySyntactically() || t.s3URLParts.IsBucketSyntactically())

	// S3 can convert directories and objects sharing names as well.
	if !isSource {
		return isDirDirect
	}

	_, err := t.s3Client.StatObject(t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})

	if err != nil {
		return true
	}

	return false
}

func (t *s3Traverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	// Check if resource is a single object.
	if t.s3URLParts.IsObjectSyntactically() && !t.s3URLParts.IsDirectorySyntactically() && !t.s3URLParts.IsBucketSyntactically() {
		objectPath := strings.Split(t.s3URLParts.ObjectKey, "/")
		objectName := objectPath[len(objectPath)-1]

		oi, err := t.s3Client.StatObject(t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})

		// If we actually got object properties, process them.
		// Otherwise, treat it as a directory.
		// According to IsDirectorySyntactically, objects and folders can share names
		if err == nil {
			storedObject := newStoredObject(
				preprocessor,
				objectName,
				"",
				oi.LastModified,
				oi.Size,
				nil,
				blobTypeNA,
				t.s3URLParts.BucketName)

			// We had to statObject anyway, get ALL the info.
			oie := common.ObjectInfoExtension{ObjectInfo: oi}

			storedObject.contentType = oi.ContentType
			storedObject.md5 = oie.ContentMD5()
			storedObject.cacheControl = oie.CacheControl()
			storedObject.contentLanguage = oie.ContentLanguage()
			storedObject.contentDisposition = oie.ContentDisposition()
			storedObject.contentEncoding = oie.ContentEncoding()
			storedObject.Metadata = oie.NewCommonMetadata()

			err = processIfPassedFilters(
				filters,
				storedObject,
				processor)

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

		objectPath := strings.Split(objectInfo.Key, "/")
		objectName := objectPath[len(objectPath)-1]

		// re-join the unescaped path.
		relativePath := strings.TrimPrefix(objectInfo.Key, searchPrefix)

		if strings.HasSuffix(relativePath, "/") {
			// If a file has a suffix of /, it's still treated as a folder.
			// Thus, akin to the old code. skip it.
			continue
		}

		storedObject := newStoredObject(
			preprocessor,
			objectName,
			relativePath,
			objectInfo.LastModified,
			objectInfo.Size,
			nil,
			blobTypeNA,
			t.s3URLParts.BucketName)

		if t.getProperties {
			oi, err := t.s3Client.StatObject(t.s3URLParts.BucketName, objectInfo.Key, minio.StatObjectOptions{})

			if err != nil {
				return err
			}

			oie := common.ObjectInfoExtension{ObjectInfo: oi}

			storedObject.contentType = oi.ContentType
			storedObject.md5 = oie.ContentMD5()
			storedObject.cacheControl = oie.CacheControl()
			storedObject.contentLanguage = oie.ContentLanguage()
			storedObject.contentDisposition = oie.ContentDisposition()
			storedObject.contentEncoding = oie.ContentEncoding()
			storedObject.Metadata = oie.NewCommonMetadata()
		}

		err = processIfPassedFilters(filters,
			storedObject,
			processor)

		if err != nil {
			return
		}
	}
	return
}

func newS3Traverser(rawURL *url.URL, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter func()) (t *s3Traverser, err error) {
	t = &s3Traverser{rawURL: rawURL, ctx: ctx, recursive: recursive, getProperties: getProperties, incrementEnumerationCounter: incrementEnumerationCounter}

	// initialize S3 client and URL parts
	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*t.rawURL)

	if err != nil {
		return
	} else {
		t.s3URLParts = s3URLPartsExtension{s3URLParts}
	}

	showS3UrlTypeWarning(s3URLParts)

	t.s3Client, err = common.CreateS3Client(
		t.ctx,
		common.CredentialInfo{
			CredentialType: common.ECredentialType.S3AccessKey(),
			S3CredentialInfo: common.S3CredentialInfo{
				Endpoint: t.s3URLParts.Endpoint,
				Region:   t.s3URLParts.Region,
			},
		},
		common.CredentialOpOptions{
			LogError: glcm.Error,
		})

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
