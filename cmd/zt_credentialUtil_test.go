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
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
	"strings"
)

type credentialUtilSuite struct{}

var _ = chk.Suite(&credentialUtilSuite{})

func (s *credentialUtilSuite) TestCheckAuthSafeForTarget(c *chk.C) {
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
		{common.ECredentialType.SharedKey(), common.ELocation.BlobFS(), "http://myaccount.dfs.core.windows.net", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://something.s3.eu-central-1.amazonaws.com", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://something.s3.cn-north-1.amazonaws.com.cn", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.eu-central-1.amazonaws.com", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.cn-north-1.amazonaws.com.cn", "", true},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://s3.amazonaws.com", "", true},

		// These should fail (they are not storage)
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://somethingelseinazure.windows.net", "", false},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://somethingelseinaws.amazonaws.com", "", false},

		// As should these (they are nothing to do with the expected URLs)
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "", false},
		{common.ECredentialType.S3AccessKey(), common.ELocation.S3(), "http://abc.example.com", "", false},
		// Test that we don't want to send an S3 access key to a blob resource type.
		{common.ECredentialType.S3AccessKey(), common.ELocation.Blob(), "http://abc.example.com", "", false},

		// But the same Azure one should pass if the user opts in to them (we don't support any similar override for S3)
		{common.ECredentialType.OAuthToken(), common.ELocation.Blob(), "http://abc.example.com", "*.foo.com;*.example.com", true},
	}

	for i, t := range tests {
		err := checkAuthSafeForTarget(t.ct, t.resource, t.extraSuffixesAAD, t.resourceType)
		c.Assert(err == nil, chk.Equals, t.expectedOK, chk.Commentf("Failed on test %d for resource %s", i, t.resource))
	}
}

func (s *credentialUtilSuite) TestCheckAuthSafeForTargetIsCalledWhenGettingAuthType(c *chk.C) {
	mockGetCredTypeFromEnvVar := func() common.CredentialType {
		return common.ECredentialType.OAuthToken() // force it to OAuth, which is the case we want to test
	}

	// Call our core cred type getter function, in a way that will fail the safety check, and assert
	// that it really does fail.
	// This checks that our safety check is hooked into the main logic
	_, _, err := doGetCredentialTypeForLocation(context.Background(), common.ELocation.Blob(),
		"http://notblob.example.com", "", true, mockGetCredTypeFromEnvVar)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), "If this URL is in fact an Azure service, you can enable Azure authentication to notblob.example.com."),
		chk.Equals, true)
}
