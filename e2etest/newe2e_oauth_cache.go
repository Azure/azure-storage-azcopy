package e2etest

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const (
	AzureManagementResource = "https://management.core.windows.net/.default"
	AzureStorageResource    = "https://storage.azure.com/.default"
	AzureDisksResource      = "https://disk.azure.com/.default"
)

var PrimaryOAuthCache *OAuthCache

func SetupOAuthCache(a Asserter) {
	var (
		cred     azcore.TokenCredential
		err      error
		tenantId string

		staticOAuth = GlobalConfig.E2EAuthConfig.StaticStgAcctInfo.StaticOAuth
	)

	// We don't consider workload identity in here because it's only used in a few tests

	if GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.Environment == AzurePipeline {
		tenantId = GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.DynamicOAuth.Workload.TenantId
		cred, err = azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
			TenantID: tenantId,
		})
	} else if useSpn, tenant, appId, secret := GlobalConfig.GetSPNOptions(); useSpn {
		tenantId = tenant
		cred, err = azidentity.NewClientSecretCredential(tenant, appId, secret, nil)
	} else if staticOAuth.OAuthSource.CLIInherit {
		tenantId = staticOAuth.TenantID
		cred, err = azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{
			TenantID: tenantId,
		})
	} else if staticOAuth.OAuthSource.PSInherit {
		tenantId = staticOAuth.TenantID
		cred, err = common.NewPowershellContextCredential(&common.PowershellContextCredentialOptions{
			TenantID: tenantId,
		})
	} else {
		a.Log("OAuth Cache unconfigured.")
	}
	a.NoError("create credentials", err)
	a.AssertNow("cred cannot be nil", Not{IsNil{}}, cred)

	PrimaryOAuthCache = NewOAuthCache(cred, tenantId)
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
		tenant: tenant,
		tokens: make(map[string]*azcore.AccessToken),
		mut:    &sync.RWMutex{},
	}
}

var OAuthCacheDisabledError = errors.New("the OAuth cache is currently disabled")

func (o *OAuthCache) GetAccessToken(scope string) (*AzCoreAccessToken, error) {
	if o == nil {
		return nil, OAuthCacheDisabledError
	}

	o.mut.RLock()
	tok, ok := o.tokens[scope]
	o.mut.RUnlock()

	if !ok || time.Now().Add(time.Minute*3).After(tok.ExpiresOn) {
		o.mut.Lock()
		newTok, err := o.tc.GetToken(ctx, policy.TokenRequestOptions{
			Scopes:    []string{scope},
			TenantID:  o.tenant,
			EnableCAE: true,
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
