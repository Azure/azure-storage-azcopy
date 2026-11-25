package e2etest

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudBucketResourceManager(t *testing.T) {
	const destination = "https://account.blob.core.windows.net/container"
	for _, test := range []struct {
		location         common.Location
		host, sourceType string
	}{
		{common.ELocation.S3(), "s3.amazonaws.com", "S3"},
		{common.ELocation.GCP(), "storage.cloud.google.com", "GCP"},
	} {
		t.Run(test.sourceType, func(t *testing.T) {
			bucket := &CloudBucketResourceManager{location: test.location, bucket: "bucket"}
			object := bucket.Object("folder/key.bin")
			assert.Equal(t, "https://"+test.host+"/bucket", bucket.URI())
			assert.Equal(t, "https://"+test.host+"/bucket/folder/key.bin", object.URI())
			assert.Equal(t, "https://"+test.host+"/bucket/*", bucket.URI(GetURIOptions{Wildcard: "*"}))
			assert.Equal(t, azcopy.ELocationLevel.Container(), bucket.Level())
			assert.Equal(t, azcopy.ELocationLevel.Object(), object.Level())
			assert.Nil(t, bucket.Parent())
			assert.Equal(t, bucket.URI(), object.Parent().URI())
			assert.Nil(t, object.Account())
			assert.Equal(t, "accountless/"+test.sourceType+"/bucket/folder/key.bin", object.Canon())

			// AzCopy must read each URL as this provider at the same level, which sets the scope telemetry reports, and
			// the E2E manifest derives the source type from the same URLs.
			for _, rm := range []*CloudBucketResourceManager{bucket, object} {
				level, err := azcopy.DetermineLocationLevel(rm.URI(), test.location, true)
				require.NoError(t, err)
				assert.Equal(t, rm.Level(), level)
				sourceType, destType, err := telemetryEndpointTypes([]string{rm.URI(), destination}, map[string]string{})
				require.NoError(t, err)
				assert.Equal(t, test.sourceType, sourceType)
				assert.Equal(t, "Blob", destType)
			}
		})
	}
}

func TestCloudSourceCredentials(t *testing.T) {
	for _, name := range []string{"S3_TESTS_OFF", "GCP_TESTS_OFF", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT"} {
		t.Setenv(name, "")
	}
	for _, test := range []struct {
		location  common.Location
		variables []string
		inherited []string
	}{
		{common.ELocation.S3(), []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}, []string{"aws_access_key_id", "aws_secret_access_key", "aws_session_token"}},
		{common.ELocation.GCP(), []string{"GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT"}, []string{"google_application_credentials", "google_cloud_project"}},
	} {
		t.Run(test.location.String(), func(t *testing.T) {
			assert.NotEmpty(t, cloudSourceSkipReason(test.location), "no credentials")
			t.Setenv(test.variables[0], "set")
			assert.NotEmpty(t, cloudSourceSkipReason(test.location), "partial credentials")
			t.Setenv(test.variables[1], "set")
			assert.Empty(t, cloudSourceSkipReason(test.location))

			// AzCopy reads the provider credentials from its environment, which RunAzCopy builds from an allow list.
			bucket := &CloudBucketResourceManager{location: test.location, bucket: "bucket"}
			command := AzCopyCommand{Environment: &AzCopyEnvironment{}}
			assert.Equal(t, bucket.URI(), command.applyTargetAuth(NewFrameworkAsserter(t), bucket))
			for _, name := range test.inherited {
				assert.True(t, command.Environment.InheritEnvironment[name], name)
			}
		})
	}
	t.Setenv("S3_TESTS_OFF", "true")
	assert.NotEmpty(t, cloudSourceSkipReason(common.ELocation.S3()), "S3 tests switched off")
	t.Setenv("GCP_TESTS_OFF", "true")
	assert.NotEmpty(t, cloudSourceSkipReason(common.ELocation.GCP()), "GCS tests switched off")
	assert.NotEmpty(t, cloudSourceSkipReason(common.ELocation.Blob()), "not a third-party cloud")
}
