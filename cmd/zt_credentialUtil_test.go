// Copyright © Microsoft <wastore@microsoft.com>
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
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestCheckAuthSafeForTarget(t *testing.T) {
	a := assert.New(t)
	tests := []struct {
		ct               enum.CredentialType
		resourceType     common.Location
		resource         string
		extraSuffixesAAD string
		expectedOK       bool
	}{
		// these auth types deliberately don't get checked, i.e. always should be considered safe
		// invalid URLs are supposedly overridden as the resource type specified via --fromTo in this scenario
		{enum.ECredentialType.Unknown(), common.ELocation.Blob(), "http://nowhere.com", "", true},
		{enum.ECredentialType.Anonymous(), common.ELocation.Blob(), "http://nowhere.com", "", true},

		// these ones get checked, so these should pass:
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.windows.net", "", true},
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.chinacloudapi.cn", "", true},
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.cloudapi.de", "", true},
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.core.usgovcloudapi.net", "", true},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.windows.net", "", true},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.chinacloudapi.cn", "", true},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.cloudapi.de", "", true},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.core.usgovcloudapi.net", "", true},
		{enum.ECredentialType.SharedKey(), common.ELocation.BlobFS(), "http://myaccount.dfs.core.windows.net", "", true},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://something.s3.eu-central-1.amazonaws.com", "", true},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://something.s3.cn-north-1.amazonaws.com.cn", "", true},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.eu-central-1.amazonaws.com", "", true},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.cn-north-1.amazonaws.com.cn", "", true},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.amazonaws.com", "", true},
		{enum.ECredentialType.GoogleAppCredentials(), common.ELocation.GCP(), "http://storage.cloud.google.com", "", true},

		// These should fail (they are not storage)
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://somethingelseinazure.windows.net", "", false},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://somethingelseinazure.windows.net", "", false},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://somethingelseinaws.amazonaws.com", "", false},
		{enum.ECredentialType.GoogleAppCredentials(), common.ELocation.GCP(), "http://appengine.google.com", "", false},

		// As should these (they are nothing to do with the expected URLs)
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{enum.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://abc.example.com", "", false},
		{enum.ECredentialType.GoogleAppCredentials(), common.ELocation.GCP(), "http://abc.example.com", "", false},
		// Test that we don't want to send an S3 access key to a blob resource type.
		{enum.ECredentialType.S3AccessKey(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{enum.ECredentialType.GoogleAppCredentials(), common.ELocation.Blob(), "http://abc.example.com", "", false},

		// But the same Azure one should pass if the user opts in to them (we don't support any similar override for S3)
		{enum.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "*.foo.com;*.example.com", true},
		{enum.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "*.foo.com;*.example.com", true},
	}

	for i, t := range tests {
		err := checkAuthSafeForTarget(t.ct, t.resource, t.extraSuffixesAAD, t.resourceType)
		a.Equal(t.expectedOK, err == nil, "Failed on test %d for resource %s", i, t.resource)
	}
}

func TestManagedDiskCredentialPropagatesCPKError(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.CPKEncryptionKey().Name, "")
	t.Setenv(enum.EEnvironmentVariable.CPKEncryptionKeySHA256().Name, "")

	_, err := getBlobCredInfo(common.ResourceString{
		Value: "https://md-example.blob.core.windows.net/container/blob",
		SAS:   "sig=example",
	}, GetTargetCredInfoOptions{
		Context:    context.Background(),
		CpkOptions: common.CpkOptions{CpkInfo: true},
	})
	assert.ErrorContains(t, err, "failed to fetch cpk encryption key")
}

/*
 * This function tests that common.isPublic routine is works fine.
 * Two cases are considered, a blob is public or a container is public.
 */
func TestIsPublic(t *testing.T) {
	// TODO: Migrate this test to mocked UT.
	t.Skip("Public access is sometimes turned off due to organization policy. This test should ideally be migrated to a mocked UT.")

	a := assert.New(t)
	ctx, _ := context.WithTimeout(context.TODO(), 5*time.Minute)
	bsc := getBlobServiceClient()
	ctr, _ := getContainerClient(a, bsc)
	defer ctr.Delete(ctx, nil)

	publicAccess := container.PublicAccessTypeContainer

	// Create a public container
	_, err := ctr.Create(ctx, &container.CreateOptions{Access: &publicAccess})
	a.Nil(err)

	// verify that container is public
	public, err := isPublic(ctx, ctr.URL(), common.CpkOptions{})
	a.NoError(err)
	a.True(public)

	publicAccess = container.PublicAccessTypeBlob
	_, err = ctr.SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{Access: &publicAccess})
	a.Nil(err)

	// Verify that blob is public.
	bb, _ := getBlockBlobClient(a, ctr, "")
	_, err = bb.UploadBuffer(ctx, []byte("I'm a block blob."), nil)
	a.Nil(err)

	public, err = isPublic(ctx, bb.URL(), common.CpkOptions{})
	a.NoError(err)
	a.True(public)

}
