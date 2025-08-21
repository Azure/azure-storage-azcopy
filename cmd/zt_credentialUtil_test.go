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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	chk "gopkg.in/check.v1"
)

type credentialUtilSuite struct{}

var _ = chk.Suite(&credentialUtilSuite{})

func TestCheckAuthSafeForTarget(t *testing.T) {
	a := assert.New(t)
	tests := []struct {
		ct               common.CredentialType
		resourceType     common.Location
		resource         string
		extraSuffixesAAD string
		expectedOK       bool
	}{
		// these auth types deliberately don't get checked, i.e. always should be considered safe
		// invalid URLs are supposedly overridden as the resource type specified via --fromTo in this scenario
		{common.ECredentialType.Unknown(), common.ELocation.Blob(), "http://nowhere.com", "", true},
		{common.ECredentialType.Anonymous(), common.ELocation.Blob(), "http://nowhere.com", "", true},

		// these ones get checked, so these should pass:
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.windows.net", "", true},
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.chinacloudapi.cn", "", true},
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.cloudapi.de", "", true},
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.core.usgovcloudapi.net", "", true},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.windows.net", "", true},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.chinacloudapi.cn", "", true},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.cloudapi.de", "", true},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://myaccount.blob.core.core.usgovcloudapi.net", "", true},
		{common.ECredentialType.SharedKey(), common.ELocation.BlobFS(), "http://myaccount.dfs.core.windows.net", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://something.s3.eu-central-1.amazonaws.com", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://something.s3.cn-north-1.amazonaws.com.cn", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.eu-central-1.amazonaws.com", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.cn-north-1.amazonaws.com.cn", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.amazonaws.com", "", true},
		{common.ECredentialType.GoogleAppCredentials(), common.ELocation.GCP(), "http://storage.cloud.google.com", "", true},

		// These should fail (they are not storage)
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://somethingelseinazure.windows.net", "", false},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://somethingelseinazure.windows.net", "", false},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://somethingelseinaws.amazonaws.com", "", false},
		{common.ECredentialType.GoogleAppCredentials(), common.ELocation.GCP(), "http://appengine.google.com", "", false},

		// As should these (they are nothing to do with the expected URLs)
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://abc.example.com", "", false},
		{common.ECredentialType.GoogleAppCredentials(), common.ELocation.GCP(), "http://abc.example.com", "", false},
		// Test that we don't want to send an S3 access key to a blob resource type.
		{common.ECredentialType.S3AccessKey(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{common.ECredentialType.GoogleAppCredentials(), common.ELocation.Blob(), "http://abc.example.com", "", false},

		// But the same Azure one should pass if the user opts in to them (we don't support any similar override for S3)
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "*.foo.com;*.example.com", true},
		{common.ECredentialType.MDOAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "*.foo.com;*.example.com", true},
	}

	for i, t := range tests {
		err := checkAuthSafeForTarget(t.ct, t.resource, t.extraSuffixesAAD, t.resourceType)
		a.Equal(t.expectedOK, err == nil, chk.Commentf("Failed on test %d for resource %s", i, t.resource))
	}
}

func TestCheckAuthSafeForTargetIsCalledWhenGettingAuthType(t *testing.T) {
	common.AzcopyJobPlanFolder = os.TempDir()
	a := assert.New(t)
	mockGetCredTypeFromEnvVar := func() common.CredentialType {
		return common.ECredentialType.OAuthToken() // force it to OAuth, which is the case we want to test
	}

	res, err := azcopy.SplitResourceString("http://notblob.example.com", common.ELocation.Blob())
	a.NoError(err)

	// Call our core cred type getter function, in a way that will fail the safety check, and assert
	// that it really does fail.
	// This checks that our safety check is hooked into the main logic
	_, _, err = doGetCredentialTypeForLocation(context.Background(), common.ELocation.Blob(), res, true, mockGetCredTypeFromEnvVar, nil, common.CpkOptions{})
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), "If this URL is in fact an Azure service, you can enable Azure authentication to notblob.example.com."))
}

func TestCheckAuthSafeForTargetIsCalledWhenGettingAuthTypeMDOAuth(t *testing.T) {
	a := assert.New(t)
	mockGetCredTypeFromEnvVar := func() common.CredentialType {
		return common.ECredentialType.MDOAuthToken() // force it to OAuth, which is the case we want to test
	}

	res, err := azcopy.SplitResourceString("http://notblob.example.com", common.ELocation.Blob())
	a.NoError(err)

	// Call our core cred type getter function, in a way that will fail the safety check, and assert
	// that it really does fail.
	// This checks that our safety check is hooked into the main logic
	_, _, err = doGetCredentialTypeForLocation(context.Background(), common.ELocation.Blob(), res, true, mockGetCredTypeFromEnvVar, nil, common.CpkOptions{})
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), "If this URL is in fact an Azure service, you can enable Azure authentication to notblob.example.com."))
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
	a.True(isPublic(ctx, ctr.URL(), common.CpkOptions{}))

	publicAccess = container.PublicAccessTypeBlob
	_, err = ctr.SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{Access: &publicAccess})
	a.Nil(err)

	// Verify that blob is public.
	bb, _ := getBlockBlobClient(a, ctr, "")
	_, err = bb.UploadBuffer(ctx, []byte("I'm a block blob."), nil)
	a.Nil(err)

	a.True(isPublic(ctx, bb.URL(), common.CpkOptions{}))

}
