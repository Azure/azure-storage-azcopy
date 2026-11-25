package e2etest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/api/iterator"
)

// CloudBucketResourceManager is an Amazon S3 or Google Cloud Storage bucket, or an object in one, used as a copy
// source. The New E2E resource managers only cover Azure and local storage, so the bucket is created and filled through
// the provider's SDK and deleted at cleanup. RunAzCopy passes the provider credentials from the test environment to
// AzCopy (see AzCopyCommand.applyTargetAuth).
type CloudBucketResourceManager struct {
	location common.Location
	bucket   string
	// object is empty for the bucket itself.
	object string
}

var _ ResourceManager = &CloudBucketResourceManager{}

// cloudSourceSkipReason returns why S3 or GCS sources can't be used in this run, or "" when they can. The pipeline
// sets the same variables as for the legacy S3 and GCS tests.
func cloudSourceSkipReason(location common.Location) string {
	switch location {
	case common.ELocation.S3():
		if isS3Disabled() {
			return "S3 testing is disabled (S3_TESTS_OFF)"
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
			return "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set"
		}
	case common.ELocation.GCP():
		if os.Getenv("GCP_TESTS_OFF") != "" {
			return "GCS testing is disabled (GCP_TESTS_OFF)"
		}
		if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" || os.Getenv("GOOGLE_CLOUD_PROJECT") == "" {
			return "GOOGLE_APPLICATION_CREDENTIALS and GOOGLE_CLOUD_PROJECT are not set"
		}
	default:
		return fmt.Sprintf("%s is not a third-party cloud location", location)
	}
	return ""
}

// CreateCloudBucket creates an S3 or GCS bucket holding objects (key to content) and deletes it at cleanup. It skips the
// scenario when the provider's credentials aren't configured.
func CreateCloudBucket(svm *ScenarioVariationManager, location common.Location, objects map[string]ObjectContentContainer) *CloudBucketResourceManager {
	if reason := cloudSourceSkipReason(location); reason != "" {
		svm.Skip(reason)
	}
	ctx := context.Background()
	// Bucket names are global to each provider and allow lower-case letters, digits and hyphens.
	bucket := "azcopy-e2e-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24]
	rm := &CloudBucketResourceManager{location: location, bucket: bucket}
	switch location {
	case common.ELocation.S3():
		client, err := minio.New("s3.amazonaws.com", &minio.Options{
			Creds:  credentials.NewStaticV4(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")),
			Secure: true,
		})
		svm.NoError("create S3 client", err, true)
		svm.NoError("create S3 bucket "+bucket, client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}), true)
		svm.Cleanup(func(a Asserter) { deleteS3TestBucket(a, client, bucket) })
		for key, body := range objects {
			_, err = client.PutObject(ctx, bucket, key, body.Reader(), body.Size(), minio.PutObjectOptions{})
			svm.NoError("upload S3 object "+key, err, true)
		}
	case common.ELocation.GCP():
		client, err := gcpUtils.NewClient(ctx)
		svm.NoError("create GCS client", err, true)
		handle := client.Bucket(bucket)
		svm.NoError("create GCS bucket "+bucket, handle.Create(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"), nil), true)
		svm.Cleanup(func(a Asserter) { deleteGCSTestBucket(a, client, bucket) })
		for key, body := range objects {
			writer := handle.Object(key).NewWriter(ctx)
			_, err = io.Copy(writer, body.Reader())
			svm.NoError("upload GCS object "+key, errors.Join(err, writer.Close()), true)
		}
	}
	return rm
}

// deleteS3TestBucket empties and deletes a test bucket. Failures are only logged, as in the legacy S3 tests, so a
// bucket the service is slow to release doesn't fail the scenario.
func deleteS3TestBucket(a Asserter, client *minio.Client, bucket string) {
	ctx := context.Background()
	for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			a.Log("listing S3 test bucket %s: %v", bucket, object.Err)
			return
		}
		if err := client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			a.Log("deleting S3 object %s/%s: %v", bucket, object.Key, err)
		}
	}
	if err := client.RemoveBucket(ctx, bucket); err != nil {
		a.Log("deleting S3 test bucket %s: %v", bucket, err)
	}
}

// deleteGCSTestBucket empties and deletes a test bucket, logging failures like deleteS3TestBucket.
func deleteGCSTestBucket(a Asserter, client *gcpUtils.Client, bucket string) {
	ctx := context.Background()
	defer func() { _ = client.Close() }()
	handle := client.Bucket(bucket)
	objects := handle.Objects(ctx, nil)
	for {
		attrs, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			a.Log("listing GCS test bucket %s: %v", bucket, err)
			return
		}
		if err = handle.Object(attrs.Name).Delete(ctx); err != nil {
			a.Log("deleting GCS object %s/%s: %v", bucket, attrs.Name, err)
		}
	}
	if err := handle.Delete(ctx); err != nil {
		a.Log("deleting GCS test bucket %s: %v", bucket, err)
	}
}

// Object returns the object with the given key in this bucket.
func (c *CloudBucketResourceManager) Object(key string) *CloudBucketResourceManager {
	return &CloudBucketResourceManager{location: c.location, bucket: c.bucket, object: key}
}

// BucketName returns the bucket's name.
func (c *CloudBucketResourceManager) BucketName() string {
	return c.bucket
}

func (c *CloudBucketResourceManager) Location() common.Location {
	return c.location
}

func (c *CloudBucketResourceManager) Level() azcopy.LocationLevel {
	if c.object != "" {
		return azcopy.ELocationLevel.Object()
	}
	return azcopy.ELocationLevel.Container()
}

// URI returns the path-style URL AzCopy accepts for the provider: s3.amazonaws.com for S3 (buckets are created in
// us-east-1, which that endpoint serves) and storage.cloud.google.com for GCS.
func (c *CloudBucketResourceManager) URI(opts ...GetURIOptions) string {
	host := "s3.amazonaws.com"
	if c.location == common.ELocation.GCP() {
		host = "storage.cloud.google.com"
	}
	uri := "https://" + host + "/" + c.bucket
	if c.object != "" {
		uri += "/" + c.object
	}
	if len(opts) > 0 && opts[0].Wildcard != "" {
		uri += "/" + strings.TrimPrefix(opts[0].Wildcard, "/")
	}
	return uri
}

func (c *CloudBucketResourceManager) Parent() ResourceManager {
	if c.object != "" {
		return &CloudBucketResourceManager{location: c.location, bucket: c.bucket}
	}
	return nil
}

func (c *CloudBucketResourceManager) Account() AccountResourceManager {
	return nil
}

func (c *CloudBucketResourceManager) Canon() string {
	canon := "accountless/" + c.location.String() + "/" + c.bucket
	if c.object != "" {
		canon += "/" + c.object
	}
	return canon
}
