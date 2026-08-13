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

package ste

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	minio "github.com/minio/minio-go/v7"
)

// Source info provider for S3
type s3SourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo *TransferInfo

	rawSourceURL *url.URL

	s3Client  *minio.Client
	s3URLPart common.S3URLParts
	credType  common.CredentialType

	// For Google Cloud Storage S3-compatible endpoints
	isGoogleEndpoint bool
	gcpClient        *gcpUtils.Client
	gcpJSONKey       []byte
}

// By default presign expires after 7 days, which is considered enough for large amounts of files transfer.
// This value could be further tuned, or exposed to user for customization, according to user feedback.
const defaultPresignExpires = time.Hour * 7 * 24

var s3ClientFactory = common.NewS3ClientFactory()
var gcpClientFactoryForS3 = common.NewGCPClientFactory()

func newS3SourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	var err error
	p := s3SourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}

	p.rawSourceURL, err = url.Parse(p.transferInfo.Source)
	if err != nil {
		return nil, err
	}

	p.s3URLPart, err = common.NewS3URLParts(*p.rawSourceURL)
	if err != nil {
		return nil, err
	}

	// Check if this is a Google Cloud Storage endpoint accessed via S3-compatible API
	p.isGoogleEndpoint = p.s3URLPart.IsGoogleCloudStorage()

	ctx := jptm.Context()
	ctx = withPipelineNetworkStats(ctx, nil)

	// For Google endpoints, try to use GCP credentials; otherwise fall back to S3
	if p.isGoogleEndpoint {
		googleAppCredentials := common.GetEnvironmentVariable(common.EEnvironmentVariable.GoogleAppCredentials())
		if googleAppCredentials != "" {
			// Use GCP signing for this Google endpoint
			p.credType = common.ECredentialType.GoogleAppCredentials()

			// Create GCP client for metadata operations
			p.gcpClient, err = gcpClientFactoryForS3.GetGCPClient(
				ctx,
				common.CredentialInfo{
					CredentialType:    common.ECredentialType.GoogleAppCredentials(),
					GCPCredentialInfo: common.GCPCredentialInfo{},
				},
				common.CredentialOpOptions{
					LogInfo:  func(str string) { p.jptm.Log(common.LogInfo, str) },
					LogError: func(str string) { p.jptm.Log(common.LogError, str) },
					Panic:    func(err error) { panic(err) },
				})
			if err != nil {
				return nil, err
			}

			// Read and store the JSON key for signing
			p.gcpJSONKey, err = os.ReadFile(googleAppCredentials)
			if err != nil {
				return nil, fmt.Errorf("Cannot read JSON key file. Please verify you have correctly set GOOGLE_APPLICATION_CREDENTIALS environment variable")
			}

			return &p, nil
		}
	}

	// Fall back to S3-style auth for Google endpoints without GOOGLE_APPLICATION_CREDENTIALS
	// or for non-Google S3-compatible endpoints
	if p.transferInfo.Provider != nil { //add check for if we want to use provider case
		p.credType = common.ECredentialType.S3AccessKey()
	} else if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		p.credType = common.ECredentialType.S3PublicBucket()
	} else {
		p.credType = common.ECredentialType.S3AccessKey()
	}

	p.s3Client, err = s3ClientFactory.GetS3Client(ctx, common.CredentialInfo{
		CredentialType: p.credType,
		S3CredentialInfo: common.S3CredentialInfo{
			Endpoint:   p.s3URLPart.Endpoint,
			Region:     p.s3URLPart.Region,
			BucketName: p.s3URLPart.BucketName,
			Provider:   p.transferInfo.Provider,
		},
	}, common.CredentialOpOptions{
		LogInfo:  func(str string) { p.jptm.Log(common.LogInfo, str) },
		LogError: func(str string) { p.jptm.Log(common.LogError, str) },
		Panic:    func(err error) { panic(err) },
	}, jptm)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func (p *s3SourceInfoProvider) PreSignedSourceURL() (string, error) {
	// For Google endpoints with GCP credentials, use GCS V4 signing
	if p.isGoogleEndpoint && p.credType == common.ECredentialType.GoogleAppCredentials() {
		return p.preSignedSourceURLGCP()
	}

	if p.credType == common.ECredentialType.S3PublicBucket() {
		return p.rawSourceURL.String(), nil
	}

	source, err := p.s3Client.PresignedGetObject(context.Background(), p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, defaultPresignExpires, url.Values{})
	if err != nil {
		return "", err
	}
	return source.String(), nil
}

// preSignedSourceURLGCP generates a GCS V4 signed URL for Google endpoints
func (p *s3SourceInfoProvider) preSignedSourceURLGCP() (string, error) {
	return signGCPObjectURLV4(p.gcpJSONKey, p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, defaultPresignExpires)
}

func (p *s3SourceInfoProvider) Properties() (*SrcProperties, error) {
	srcProperties := SrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
		SrcBlobTags:    p.transferInfo.SrcBlobTags,
	}

	if p.isGoogleEndpoint && p.credType == common.ECredentialType.GoogleAppCredentials() {
		if p.transferInfo.S2SGetPropertiesInBackend {
			objectInfo, err := p.gcpClient.Bucket(p.s3URLPart.BucketName).Object(p.s3URLPart.ObjectKey).Attrs(context.Background())
			if err != nil {
				return nil, err
			}

			oie := common.GCPObjectInfoExtension{ObjectInfo: *objectInfo}
			srcProperties = SrcProperties{
				SrcHTTPHeaders: common.ResourceHTTPHeaders{
					ContentType:        objectInfo.ContentType,
					ContentEncoding:    oie.ContentEncoding(),
					ContentDisposition: oie.ContentDisposition(),
					ContentLanguage:    oie.ContentLanguage(),
					CacheControl:       oie.CacheControl(),
					ContentMD5:         oie.ContentMD5(),
				},
				SrcMetadata: oie.NewCommonMetadata(),
			}
		}

		resolvedMetadata, err := p.handleInvalidMetadataKeys(srcProperties.SrcMetadata)
		if err != nil {
			return nil, err
		}
		srcProperties.SrcMetadata = resolvedMetadata
		return &srcProperties, nil
	}

	// Get properties in backend.
	if p.transferInfo.S2SGetPropertiesInBackend {
		objectInfo, err := p.s3Client.StatObject(context.Background(), p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, minio.StatObjectOptions{})
		if err != nil {
			return nil, err
		}

		// For mover builds, skip objects in archive storage classes (GLACIER, DEEP_ARCHIVE) unless restored.
		// This check piggybacks on the existing StatObject call (no extra API call) and runs with
		// 64-way parallelism in the STE, avoiding the single-threaded enumeration bottleneck.
		// Note: minio-go's StatObject (HEAD) does NOT populate ObjectInfo.StorageClass directly;
		// the storage class is only available via the X-Amz-Storage-Class header in Metadata.
		storageClass := objectInfo.Metadata.Get("X-Amz-Storage-Class")
		if buildmode.IsMover && isArchiveStorageClass(storageClass) && !isRestoredFromArchive(objectInfo.Restore) {
			return nil, fmt.Errorf("%w: object %q is in %s storage class",
				common.ErrS3ArchiveObjectNotRestored, p.s3URLPart.ObjectKey, storageClass)
		}

		oie := common.ObjectInfoExtension{ObjectInfo: objectInfo}

		srcProperties = SrcProperties{
			SrcHTTPHeaders: common.ResourceHTTPHeaders{
				ContentType:        objectInfo.ContentType,
				ContentEncoding:    oie.ContentEncoding(),
				ContentDisposition: oie.ContentDisposition(),
				ContentLanguage:    oie.ContentLanguage(),
				CacheControl:       oie.CacheControl(),
				ContentMD5:         oie.ContentMD5(),
			},
			SrcMetadata: oie.NewCommonMetadata(),
		}
	}

	// Handle invalid metadata.
	// Note: Only handle metadata's key, as metadata's value must conform to US-ASCII for both S3 and Azure.
	resolvedMetadata, err := p.handleInvalidMetadataKeys(srcProperties.SrcMetadata)
	if err != nil {
		return nil, err
	}
	srcProperties.SrcMetadata = resolvedMetadata

	return &srcProperties, nil
}

// handleInvalidMetadataKeys handles invalid metadata for S3 source.
func (p *s3SourceInfoProvider) handleInvalidMetadataKeys(m common.Metadata) (common.Metadata, error) {
	if m == nil {
		return m, nil
	}

	switch p.transferInfo.S2SInvalidMetadataHandleOption {
	case common.EInvalidMetadataHandleOption.ExcludeIfInvalid():
		retainedMetadata, excludedMetadata, invalidKeyExists := m.ExcludeInvalidKey()
		if invalidKeyExists && p.jptm.ShouldLog(common.LogWarning) {
			p.jptm.Log(common.LogWarning,
				fmt.Sprintf("METADATAWARNING: For source %q, invalid metadata with keys %s are excluded", p.transferInfo.Source, excludedMetadata.ConcatenatedKeys()))
		}
		return retainedMetadata, nil

	case common.EInvalidMetadataHandleOption.FailIfInvalid():
		_, invalidMetdata, invalidKeyExists := m.ExcludeInvalidKey()
		if invalidKeyExists {
			return nil, fmt.Errorf("metadata with keys %s in source is invalid, and application parameters specify that error should be reported when invalid keys are found", invalidMetdata.ConcatenatedKeys())
		}
		return m, nil

	case common.EInvalidMetadataHandleOption.RenameIfInvalid():
		return m.ResolveInvalidKey()
	}

	return m, nil
}

func (p *s3SourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *s3SourceInfoProvider) RawSource() string {
	return p.transferInfo.Source
}

func (p *s3SourceInfoProvider) IsLocal() bool {
	return false
}

func (p *s3SourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	if p.isGoogleEndpoint && p.credType == common.ECredentialType.GoogleAppCredentials() {
		objectInfo, err := p.gcpClient.Bucket(p.s3URLPart.BucketName).Object(p.s3URLPart.ObjectKey).Attrs(context.Background())
		if err != nil {
			return time.Time{}, err
		}
		return objectInfo.Updated, nil
	}

	objectInfo, err := p.s3Client.StatObject(context.Background(), p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, minio.StatObjectOptions{})
	if err != nil {
		return time.Time{}, err
	}
	return objectInfo.LastModified, nil
}

func (p *s3SourceInfoProvider) EntityType() common.EntityType {
	return common.EEntityType.File() // no real folders exist in S3
}

func (p *s3SourceInfoProvider) GetObjectRange(offset, length int64) (io.ReadCloser, error) {
	if p.isGoogleEndpoint && p.credType == common.ECredentialType.GoogleAppCredentials() {
		return p.gcpClient.Bucket(p.s3URLPart.BucketName).Object(p.s3URLPart.ObjectKey).NewRangeReader(context.Background(), offset, length)
	}

	options := minio.GetObjectOptions{}

	// Set the range header for the bytes we want to retrieve
	r := formatHTTPRange(offset, length)
	if r != nil {
		options.Set("Range", *r)
	}

	ctx := context.Background()
	// Get the object with the specified range
	body, err := p.s3Client.GetObject(ctx, p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, options)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (p *s3SourceInfoProvider) GetMD5(offset, count int64) ([]byte, error) {
	if p.isGoogleEndpoint && p.credType == common.ECredentialType.GoogleAppCredentials() {
		body, err := p.gcpClient.Bucket(p.s3URLPart.BucketName).Object(p.s3URLPart.ObjectKey).NewRangeReader(context.Background(), offset, count)
		if err != nil {
			return nil, err
		}
		defer body.Close() //nolint:staticcheck
		h := md5.New()
		if _, err = io.Copy(h, body); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}

	options := minio.GetObjectOptions{}
	r := formatHTTPRange(offset, count)
	if r != nil {
		options.Set("Range", *r)
	}

	// s3 does not support getting range md5
	body, err := p.s3Client.GetObject(context.Background(), p.s3URLPart.BucketName, p.s3URLPart.ObjectKey, options)
	if err != nil {
		return nil, err
	}
	// compute md5
	defer body.Close() //nolint:staticcheck
	h := md5.New()
	if _, err = io.Copy(h, body); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// isArchiveStorageClass checks if the given S3 storage class is an archive tier
// that requires restoration before objects can be accessed.
//
// AWS S3 Glacier storage classes that require restoration:
//   - GLACIER: Glacier Flexible Retrieval (formerly just "Glacier")
//   - DEEP_ARCHIVE: Glacier Deep Archive
//
// Note: GLACIER_IR (AWS) provides millisecond access without restoration, so it is NOT included.
func isArchiveStorageClass(storageClass string) bool {
	return storageClass == "GLACIER" || storageClass == "DEEP_ARCHIVE"
}

// isRestoredFromArchive checks whether an archived S3 object has been successfully
// restored and is currently accessible. A restore is considered ready when:
//   - RestoreInfo is not nil (x-amz-restore header was present)
//   - OngoingRestore is false (restore operation has completed)
//   - ExpiryTime is set (the temporary copy has an expiry, confirming availability)
func isRestoredFromArchive(restore *minio.RestoreInfo) bool {
	return restore != nil && !restore.OngoingRestore && !restore.ExpiryTime.IsZero()
}
