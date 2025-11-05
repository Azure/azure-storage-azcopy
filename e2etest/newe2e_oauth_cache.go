package e2etest

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
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

	// We want to know...
	tokens        map[uuid.UUID]*azcore.AccessToken // what tokens we have
	tokensByScope map[string][]uuid.UUID            // which tokens can access a given scope
	scopesOfToken map[uuid.UUID][]string            // what scopes a given token has access to

	mut *sync.RWMutex
}

func NewOAuthCache(cred azcore.TokenCredential, tenant string) *OAuthCache {
	return &OAuthCache{
		tc:            cred,
		tenant:        tenant,
		tokens:        make(map[uuid.UUID]*azcore.AccessToken),
		tokensByScope: make(map[string][]uuid.UUID),
		scopesOfToken: make(map[uuid.UUID][]string),
		mut:           &sync.RWMutex{},
	}
}

var OAuthCacheDisabledError = errors.New("the OAuth cache is currently disabled")

func (o *OAuthCache) GetAccessToken(scope string) (*AzCoreAccessToken, error) {
	// at this point, operates on backwards compat
	tok, err := o.GetToken(ctx, policy.TokenRequestOptions{
		Scopes:    []string{scope},
		TenantID:  o.tenant,
		EnableCAE: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed fetching new AccessToken: %w", err)
	}

	return &AzCoreAccessToken{
		tok:    &tok,
		parent: o,
		Scope:  scope,
	}, nil
}

func (o *OAuthCache) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if o == nil {
		return azcore.AccessToken{}, OAuthCacheDisabledError
	}

	// step 1: See if we can find a suitable token.
	o.mut.RLock()
	var tok *azcore.AccessToken
	var tokID uuid.UUID
	// No scope is almost certainly invalid, but we'll treat that as uuid.Nil and see if it's there
	if len(options.Scopes) == 0 {
		tok = o.tokens[uuid.Nil]
		tokID = uuid.Nil
	} else {
		// If we do *have* a matching token containing all requested scopes,
		// any scope should contain a token matching the request.
		// thus, we take the first of the list.
		for _, v := range o.tokensByScope[options.Scopes[0]] { // check all of the tokens for the first requested scope
			if ListOverlaps(options.Scopes, o.scopesOfToken[v]) { // does this token contain all requested scopes?
				tok = o.tokens[v] // earmark it
				tokID = v

				// if the range of the token is wider, update our options so that we refresh an equally scoped token.
				if len(o.scopesOfToken[v]) > len(options.Scopes) {
					options.Scopes = o.scopesOfToken[v]
				}

				break // stop looking now
			}
		}
	}
	o.mut.RUnlock()

	if tok == nil || time.Now().Add(time.Minute*3).After(tok.ExpiresOn) {
		o.mut.Lock()

		// Grab a new token
		newTok, err := o.tc.GetToken(ctx, options)
		if err != nil {
			return azcore.AccessToken{}, fmt.Errorf("failed fetching new AccessToken: %w", err)
		}

		// find a free slot for a non-zero scoped token
		if tokID == uuid.Nil && len(options.Scopes) != 0 {
			for {
				tokID = uuid.New()
				if _, ok := o.tokens[tokID]; !ok {
					break
				}
			}
		}

		// Add our token to the cache
		tok = &newTok
		o.tokens[tokID] = &newTok
		o.scopesOfToken[tokID] = options.Scopes
		for _, v := range options.Scopes {
			o.tokensByScope[v] = append(o.tokensByScope[v], tokID)
		}

		o.mut.Unlock()
	}

	return *tok, nil
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
