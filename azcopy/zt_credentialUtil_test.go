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

package azcopy

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"

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
		err := CheckAuthSafeForTarget(t.ct, t.resource, t.extraSuffixesAAD, t.resourceType)
		a.Equal(t.expectedOK, err == nil, "Failed on test %d for resource %s", i, t.resource)
	}
}

func TestManagedDiskCredentialPropagatesCPKError(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.CPKEncryptionKey().Name, "")
	t.Setenv(enum.EEnvironmentVariable.CPKEncryptionKeySHA256().Name, "")
	cpkOptions := common.CpkOptions{CpkInfo: true, IsSourceEncrypted: true}
	if _, err := cpkOptions.GetCPKInfo(); err == nil {
		t.Fatal("fixture must reject missing CPK keys before constructing a request")
	}
	// A cancelled context also prevents network activity if the validation regresses.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := getBlobCredInfo(common.ResourceString{
		Value: "https://md-unit-test.invalid/container/blob",
		SAS:   "sig=not-a-credential",
	}, GetTargetCredInfoOptions{
		Context:    ctx,
		CpkOptions: cpkOptions,
	})
	assert.ErrorContains(t, err, "failed to fetch cpk encryption key")
}

type namedTestToken string

func (token namedTestToken) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: string(token), ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type namedTestManager struct {
	cred.Manager
	requested []string
	login     cred.LoginNewTokenOptions
	deleted   string
}

func (manager *namedTestManager) GetCredentials(nickname string, _ context.Context) (azcore.TokenCredential, error) {
	manager.requested = append(manager.requested, nickname)
	return namedTestToken(nickname), nil
}

func (manager *namedTestManager) DoLogin(opts cred.LoginNewTokenOptions, _ context.Context) (azcore.TokenCredential, error) {
	manager.login = opts
	return namedTestToken(opts.Nickname), nil
}

func (manager *namedTestManager) DeleteCredentials(nickname string) bool {
	manager.deleted = nickname
	return true
}

func TestNamedCredentialsRemainIndependent(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.CredentialType().Name, "")
	oldForced := stashedEnvCredType
	stashedEnvCredType = ""
	t.Cleanup(func() { stashedEnvCredType = oldForced })
	manager := &namedTestManager{}
	for _, nickname := range []string{"source-tenant", "destination-tenant"} {
		info, err := GetTargetCredInfo(common.ResourceString{Value: "https://account.file.core.windows.net/share/file"},
			common.ELocation.File(), GetTargetCredInfoOptions{
				Context: context.Background(), PreferredTokenName: nickname, TokenManager: manager,
			})
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, enum.ECredentialType.OAuthToken(), info.CredentialType)
		token, err := info.TokenCredential.GetToken(context.Background(), policy.TokenRequestOptions{})
		assert.NoError(t, err)
		assert.Equal(t, nickname, token.Token)
	}
	assert.Equal(t, []string{"source-tenant", "destination-tenant"}, manager.requested)
}

func TestClientCredentialManagerOverride(t *testing.T) {
	original := GetCredentialManager
	t.Cleanup(func() { GetCredentialManager = original })
	first := &namedTestManager{}
	second := &namedTestManager{}
	GetCredentialManager = func() cred.Manager { return first }
	client := Client{}
	assert.Same(t, first, client.GetCredentialManager())
	GetCredentialManager = func() cred.Manager { return second }
	assert.Same(t, second, client.GetCredentialManager())
	injected := Client{credentialManager: first}
	assert.Same(t, first, injected.GetCredentialManager())
}

func TestNamedLoginLogoutDelegateToManager(t *testing.T) {
	manager := &namedTestManager{}
	client := Client{credentialManager: manager}
	_, err := client.Login(LoginOptions{
		LoginType: enum.EAutoLoginType.SPN(), CredentialName: "destination", PersistToken: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, "destination", manager.login.Nickname)
	assert.True(t, manager.login.SaveCredential)
	_, err = client.Logout(LogoutOptions{Nickname: "destination"})
	assert.NoError(t, err)
	assert.Equal(t, "destination", manager.deleted)
}
