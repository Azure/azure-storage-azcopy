package ste

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	authRequiredResp = `<?xml version="1.0" encoding="utf-8"?>  
<OnError>  
  <Code>InvalidAuthenticationInfo</Code>  
  <Message>placeholder</Message>  
</OnError>`
	accountPropsResp = `<?xml version="1.0" encoding="utf-8"?>
<StorageServiceProperties>
</StorageServiceProperties>`
)

type ReauthTransporter struct {
	RequireAuth bool
}

func (r *ReauthTransporter) Do(req *http.Request) (*http.Response, error) {
	if r.RequireAuth { // Format this as a retry blob error
		h := http.Header{}
		h.Add("WWW-Authenticate", "Bearer authorization_uri=https://login.microsoftonline.com/c1cacfe1-4dd7-4d62-b8c5-5b6cf62d10f9/oauth2/authorize resource_id=https://storage.azure.com")
		return &http.Response{
			StatusCode:    http.StatusUnauthorized,
			Status:        http.StatusText(http.StatusUnauthorized),
			ContentLength: int64(len(authRequiredResp)),
			Body:          io.NopCloser(strings.NewReader(authRequiredResp)),
			Request:       req,
			Header:        h,
		}, nil
	}

	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        http.StatusText(http.StatusOK),
		ContentLength: int64(len(accountPropsResp)),
		Body:          io.NopCloser(strings.NewReader(accountPropsResp)),
		Request:       req,
	}, nil
}

type ReauthTestCred struct {
	// ImmediateReauth fires off an azidentity.AuthenticationRequiredError in GetToken
	ImmediateReauth bool

	ReauthCallback func(ctx context.Context)
}

func (r *ReauthTestCred) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if r.ImmediateReauth {
		return azcore.AccessToken{}, &azidentity.AuthenticationRequiredError{}
	}

	return azcore.AccessToken{Token: "foobar", ExpiresOn: time.Now().Add(time.Hour * 24)}, nil
}

func (r *ReauthTestCred) Authenticate(ctx context.Context, opts *policy.TokenRequestOptions) (azidentity.AuthenticationRecord, error) {
	if r.ReauthCallback != nil {
		r.ReauthCallback(ctx)
	}

	r.ImmediateReauth = false

	return azidentity.AuthenticationRecord{}, nil
}

// This is not an end-to-end test. But it is an instantaneous validation of the logic.
func TestDestReauthPolicy(t *testing.T) {
	rootctx := context.WithValue(context.Background(), destReauthDebugNoPrompt, true)
	ctx, cancel := context.WithCancel(rootctx)

	reauthed := false
	cred := &ReauthTestCred{
		ReauthCallback: func(ctx context.Context) {
			reauthed = true
			assert.Equal(t, ctx.Value(destReauthDebugExecuted), true, "Expected reauth to occur via the policy")
			assert.Equal(t, ctx.Value(destReauthDebugCause), destReauthDebugCauseAuthenticationRequired, "Expected reauth to occur via the AuthenticationRequired mechanism")
			cancel()
		},
	}

	transport := &ReauthTransporter{}

	opts := NewClientOptions(
		policy.RetryOptions{},
		policy.TelemetryOptions{},
		transport,
		LogOptions{},
		nil, (*common.ScopedAuthenticator)(common.NewScopedCredential[common.AuthenticateToken](cred, common.ECredentialType.OAuthToken())),
	)

	c, err := blobservice.NewClient("https://foobar.blob.core.windows.net/", cred, &blobservice.ClientOptions{ClientOptions: opts})
	assert.NoError(t, err, "Failed to create service client")

	// Initially, fire off a request that will get slapped with an AuthenticationRequired.
	cred.ImmediateReauth = true
	_, err = c.GetProperties(ctx, nil)
	assert.Equal(t, reauthed, true, "Expected reauthentication attempt in request")

	// Reset the context
	ctx, cancel = context.WithCancel(rootctx)
	ctx = context.WithValue(ctx, destReauthDebugNoPrompt, true)
	reauthed = false

	// =========== InvalidAuthenticationInfo ============

	// Set the cred & request to require a reauth on round trip, rather than up front, triggering the alternative activation method
	cred.ImmediateReauth = false
	transport.RequireAuth = true

	// reset callback
	cred.ReauthCallback = func(ctx context.Context) {
		reauthed = true
		assert.Equal(t, ctx.Value(destReauthDebugExecuted), true, "Expected reauth to occur via the policy")
		assert.Equal(t, ctx.Value(destReauthDebugCause), destReauthDebugCauseInvalidAuthenticationInfo, "Expected reauth to occur via the InvalidAuthenticationInfo mechanism")
		cancel()
		transport.RequireAuth = false
	}

	// Initially, fire off a request that will get slapped with an InvalidAuthenticationMethod
	_, err = c.GetProperties(ctx, nil)
	assert.Equal(t, reauthed, true, "Expected reauthentication attempt in request")
}
