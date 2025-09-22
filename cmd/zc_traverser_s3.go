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

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

	errorChannel chan<- TraverserErrorItemInfo

	// includeDirectoryOrPrefix is used to determine if we should enqueue directories or prefixes
	// in a non-recursive traversal process. If true, prefixes will be enqueued as well even if location
	// is not folder aware.
	includeDirectoryOrPrefix bool
}

// ErrorFileInfo holds information about files and folders that failed enumeration.
type ErrorS3Info struct {
	S3Path             string
	S3Size             int64
	S3Name             string
	S3LastModifiedTime time.Time
	ErrorMsg           error
	Dir                bool
}

// Compile-time check to ensure ErrorFileInfo implements TraverserErrorItemInfo
var _ TraverserErrorItemInfo = (*ErrorS3Info)(nil)

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

func (e ErrorS3Info) Location() common.Location {
	return common.ELocation.S3()
}

// END - Implementing methods defined in TraverserErrorItemInfo

func (t *s3Traverser) writeToS3ErrorChannel(err ErrorS3Info) {
	if t.errorChannel != nil {
		select {
		case t.errorChannel <- err:
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

	_, err := t.s3Client.StatObject(t.ctx, t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})

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

		oi, err := t.s3Client.StatObject(t.ctx, t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})
		if invalidAzureBlobName(t.s3URLParts.ObjectKey) {

			t.writeToS3ErrorChannel(ErrorS3Info{
				S3Name:             objectName,
				S3Path:             t.s3URLParts.ObjectKey,
				S3LastModifiedTime: oi.LastModified,
				S3Size:             oi.Size,
				ErrorMsg:           err,
			})
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
				t.writeToS3ErrorChannel(ErrorS3Info{
					S3Name:             objectName,
					S3Path:             t.s3URLParts.ObjectKey,
					ErrorMsg:           err,
					S3LastModifiedTime: oi.LastModified,
					S3Size:             oi.Size,
				})
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
	listObjectOptions := minio.ListObjectsOptions{Prefix: searchPrefix, Recursive: t.recursive}
	for objectInfo := range t.s3Client.ListObjects(t.ctx, t.s3URLParts.BucketName, listObjectOptions) {
		// re-join the unescaped path.
		relativePath := strings.TrimPrefix(objectInfo.Key, searchPrefix)

		// Ignoring this object because it is a zero-byte placeholder typically created to simulate a folder in S3-compatible storage.
		// These objects have an empty RelativePath and are marked as files, but they do not represent actual user data.
		// Including them in processing could lead to incorrect assumptions about file presence or structure.
		if len(relativePath) == 0 && objectInfo.StorageClass != "" {
			continue
		}

		errInfo := ErrorS3Info{
			S3Name:             objectInfo.Key,
			S3Size:             objectInfo.Size,
			S3LastModifiedTime: objectInfo.LastModified,
			S3Path:             t.s3URLParts.ObjectKey,
			ErrorMsg:           objectInfo.Err,
		}

		if objectInfo.Err != nil {
			t.writeToS3ErrorChannel(errInfo)
			return fmt.Errorf("cannot list objects, %v", objectInfo.Err)
		}

		if objectInfo.StorageClass == "" && !t.includeDirectoryOrPrefix {
			// Directories are the only objects without storage classes.
			// Skip directories if not using sync orchestrator
			continue
		}

		if invalidAzureBlobName(objectInfo.Key) {
			//Throw a warning on console and continue
			WarnStdoutAndScanningLog(fmt.Sprintf(invalidNameErrorMsg, objectInfo.Key))
			t.writeToS3ErrorChannel(errInfo)
			continue
		}

		objectPath := strings.Split(objectInfo.Key, "/")
		objectName := objectPath[len(objectPath)-1]
		var storedObject StoredObject
		if objectInfo.StorageClass == "" {

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

		} else {
			if strings.HasSuffix(relativePath, "/") {
				// If a file has a suffix of /, it's still treated as a folder.
				// Thus, akin to the old code. skip it.
				// XDM: What do we do for SyncOrchrestrator?
				continue
			}

			// default to empty props, but retrieve real ones if required
			oie := common.ObjectInfoExtension{ObjectInfo: minio.ObjectInfo{}}
			if t.getProperties {
				oi, err := t.s3Client.StatObject(t.ctx, t.s3URLParts.BucketName, objectInfo.Key, minio.StatObjectOptions{})
				if err != nil {
					t.writeToS3ErrorChannel(ErrorS3Info{
						S3Name:             objectName,
						S3Path:             t.s3URLParts.ObjectKey,
						ErrorMsg:           err,
						S3LastModifiedTime: oi.LastModified,
						S3Size:             oi.Size,
					})
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

		err = processIfPassedFilters(filters,
			storedObject,
			processor)
		_, err = getProcessingError(err)
		if err != nil {
			t.writeToS3ErrorChannel(ErrorS3Info{
				S3Name:             objectName,
				S3Path:             t.s3URLParts.ObjectKey,
				ErrorMsg:           err,
				S3LastModifiedTime: storedObject.lastModifiedTime,
				S3Size:             storedObject.size,
			})
			return
		}
	}
	return
}

func newS3Traverser(rawURL *url.URL, ctx context.Context, opts InitResourceTraverserOptions) (t *s3Traverser, err error) {
	t = &s3Traverser{
		rawURL:                      rawURL,
		ctx:                         ctx,
		recursive:                   opts.Recursive,
		getProperties:               opts.GetPropertiesInFrontend,
		incrementEnumerationCounter: opts.IncrementEnumeration}

	if buildmode.IsMover {
		// If we are using this in context of Mover flow, set getProperties to false.
		// Individual getProperties have a significant performance impact in traversing S3.
		// This can be adopted by default but keeping it scoped to Mover flow for now.

		if t.getProperties {
			// Skipping logging to reduce noise in the logs.
			// WarnStdoutAndScanningLog("getProperties is being changed to false for S3 traverser for performance improvement.")
		}

		t.getProperties = false
	}

	t.includeDirectoryOrPrefix = UseSyncOrchestrator && !t.recursive

	// initialize S3 client and URL parts
	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*t.rawURL)

	if err != nil {
		return
	}

	t.s3URLParts = s3URLPartsExtension{s3URLParts}

	showS3UrlTypeWarning(s3URLParts)

	t.s3Client, err = GetS3TraverserGlobalClientManager().GetS3Client(ctx, s3URLParts, *opts.Credential)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 client from global manager: %w", err)
	}

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

// Global S3 client manager for reusing clients across operations
// This is particularly useful for sync orchestrator which creates many traversers for different path prefixes
// This allows us to avoid creating a new S3 client for each traverser, improving performance and reducing resource usage.
// This is a singleton instance, so it can be shared across multiple traversers.
// It uses sync.Once to ensure that the client is created only once, even if multiple traversers are created concurrently.
var s3TraverserGlobalClientManager = &S3ClientManager{}

type S3ClientManager struct {
	client *minio.Client
	once   sync.Once
	err    error
}

func (m *S3ClientManager) GetS3Client(ctx context.Context, s3URLParts common.S3URLParts, credInfo common.CredentialInfo) (*minio.Client, error) {
	m.once.Do(func() {
		// XDM: Do we need retry here?
		m.client, m.err = CreateSharedS3Client(ctx, s3URLParts, credInfo.CredentialType)
	})
	return m.client, m.err
}

// GetS3TraverserGlobalClientManager returns the global S3 client manager instance
// This is particularly useful for sync orchestrator which creates many traversers for different path prefixes
// This allows us to avoid creating a new S3 client for each traverser, improving performance and reducing resource usage.
// This is a singleton instance, so it can be shared across multiple traversers.
// It uses sync.Once to ensure that the client is created only once, even if multiple traversers are created concurrently.
func GetS3TraverserGlobalClientManager() *S3ClientManager {
	return s3TraverserGlobalClientManager
}

// CreateSharedS3Client creates a shared S3 client that can be reused across multiple traversers
// This is particularly useful for sync orchestrator which creates many traversers for different path prefixes
func CreateSharedS3Client(ctx context.Context, s3URLParts common.S3URLParts, credentialType common.CredentialType) (*minio.Client, error) {
	//Optional check for custom credential provider
	var credProvider credentials.Provider = nil
	creds := ctx.Value(customCreds)
	if creds != nil {
		credProvider = creds.(credentials.Provider) //if passed through context, use custom provider
	}

	return common.CreateS3Client(ctx, common.CredentialInfo{
		CredentialType: credentialType,
		S3CredentialInfo: common.S3CredentialInfo{
			Endpoint: s3URLParts.Endpoint,
			Region:   s3URLParts.Region,
			Provider: credProvider,
		},
	}, common.CredentialOpOptions{
		LogError: glcm.Error,
	}, azcopyScanningLogger)
}
