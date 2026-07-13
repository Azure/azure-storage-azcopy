package cred

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// NewScopedToken takes in a credInfo object and returns ScopedCredential
// if credentialType is either MDOAuth or oAuth. For anything else,
// nil is returned
func NewScopedToken(cred azcore.TokenCredential, credType enum.CredentialType) ScopedToken {
	var scope string
	if !credType.IsAzureOAuth() {
		return nil
	} else if credType == enum.ECredentialType.MDOAuthToken() {
		scope = ManagedDiskScope
	} else if credType == enum.ECredentialType.OAuthToken() {
		scope = StorageScope
	}

	if at, ok := cred.(AuthenticateToken); ok {
		return scopedAuthenticatorImpl{
			cred:         at,
			targetScopes: []string{scope},
		}
	}

	return scopedTokenImpl{
		cred:         cred,
		targetScopes: []string{scope},
	}
}

type ScopedToken interface {
	GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error)

	// "anchor" method; ensures that it's really a scoped credential (conforming to the behavior discarding the request options we expect)
	// not really intended for use.
	scopes() []string
}

type ScopedAuthenticator interface {
	ScopedToken

	Authenticate(ctx context.Context, _ *policy.TokenRequestOptions) (azidentity.AuthenticationRecord, error)
}

type scopedTokenImpl struct {
	cred         azcore.TokenCredential
	targetScopes []string
}

func (s scopedTokenImpl) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return s.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: s.targetScopes, EnableCAE: true})
}

func (s scopedTokenImpl) scopes() []string {
	return s.targetScopes
}

type scopedAuthenticatorImpl struct {
	cred         AuthenticateToken
	targetScopes []string
}

func (s scopedAuthenticatorImpl) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return s.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: s.targetScopes, EnableCAE: true})
}

func (s scopedAuthenticatorImpl) scopes() []string {
	return s.targetScopes
}

func (s scopedAuthenticatorImpl) Authenticate(ctx context.Context, _ *policy.TokenRequestOptions) (azidentity.AuthenticationRecord, error) {
	return s.cred.Authenticate(ctx, &policy.TokenRequestOptions{Scopes: s.targetScopes, EnableCAE: true})
}
