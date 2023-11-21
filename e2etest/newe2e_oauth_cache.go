package e2etest

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"sync"
	"time"
)

const (
	AzureManagementResource = "https://management.core.windows.net/.default"
	AzureStorageResource    = "https://storage.azure.com/.default"
	AzureDisksResource      = "https://disk.azure.com/.default"
)

var PrimaryOAuthCache *OAuthCache

func SetupOAuthCache(a Asserter) {
	if GlobalConfig.StaticResources() {
		return // no-op, because there's no OAuth configured
	}

	loginInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo

	cred, err := azidentity.NewClientSecretCredential(
		loginInfo.TenantID,
		loginInfo.ApplicationID,
		loginInfo.ClientSecret,
		nil, // Hopefully the defaults should be OK?
	)
	a.NoError("create credentials", err)

	PrimaryOAuthCache = NewOAuthCache(cred, GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.TenantID)
}

/*
The goal of the OAuthCache is to prevent getting rejected for an auth loop in the testing framework.
As such, we store all the AccessTokens in one place such that any portion of the application can request them
*/

type OAuthCache struct {
	tc     azcore.TokenCredential
	tenant string
	tokens map[string]*azcore.AccessToken
	mut    *sync.RWMutex
}

func NewOAuthCache(cred azcore.TokenCredential, tenant string) *OAuthCache {
	return &OAuthCache{
		tc:     cred,
		tokens: make(map[string]*azcore.AccessToken),
		mut:    &sync.RWMutex{},
	}
}

func (o *OAuthCache) GetAccessToken(scope string) (*AzCoreAccessToken, error) {
	o.mut.RLock()
	tok, ok := o.tokens[scope]
	o.mut.RUnlock()

	if !ok || time.Now().Add(time.Minute*3).After(tok.ExpiresOn) {
		o.mut.Lock()
		newTok, err := o.tc.GetToken(ctx, policy.TokenRequestOptions{
			Scopes:   []string{scope},
			TenantID: o.tenant,
		})
		if err != nil {
			return nil, fmt.Errorf("failed fetching new AccessToken: %w", err)
		}

		o.tokens[scope] = &newTok
		o.mut.Unlock()

		tok = &newTok
	}

	return &AzCoreAccessToken{tok, o, scope}, nil
}

type AccessToken interface {
	FreshToken() (string, error)
	CurrentToken() string
}

type AzCoreAccessToken struct {
	tok    *azcore.AccessToken
	parent *OAuthCache
	Scope  string // this is bad design but maybe it's right
}

// FreshToken attempts to cleanly get a token.
func (a *AzCoreAccessToken) FreshToken() (string, error) {
	if time.Now().Add(time.Minute).After(a.tok.ExpiresOn) {
		newTok, err := a.parent.GetAccessToken(a.Scope)
		if err != nil {
			return "", fmt.Errorf("failed to refresh token: %w", err)
		}

		a.tok = newTok.tok
	}

	return a.tok.Token, nil
}

func (a *AzCoreAccessToken) CurrentToken() string {
	return a.tok.Token
}
