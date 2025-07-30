// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity/cache"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/go-autorest/autorest/date"

	// importing the cache module registers the cache implementation for the current platform
	_ "github.com/Azure/azure-sdk-for-go/sdk/azidentity/cache"
)

// ApplicationID represents 1st party ApplicationID for AzCopy.
// const ApplicationID = "a45c21f4-7066-40b4-97d8-14f4313c3caa" // 3rd party test ApplicationID for AzCopy.
const ApplicationID = "579a7132-0e58-4d80-b1e1-7a1e2d337859"

// Resource used in azure storage OAuth authentication
const Resource = "https://storage.azure.com"
const MDResource = "https://disk.azure.com/" // There must be a trailing slash-- The service checks explicitly for "https://disk.azure.com/"

const StorageScope = "https://storage.azure.com/.default"
const ManagedDiskScope = "https://disk.azure.com//.default" // There must be a trailing slash-- The service checks explicitly for "https://disk.azure.com/"

const DefaultTenantID = "common"
const DefaultActiveDirectoryEndpoint = "https://login.microsoftonline.com"

const TokenCache = "AzCopyTokenCache"

type CredCacheImplementation interface {
	HasCachedToken() (bool, error)
	LoadToken() (*OAuthTokenInfo, error)
	SaveToken(OAuthTokenInfo) error
	RemoveCachedToken() error
}

// UserOAuthTokenManager for token management.
type UserOAuthTokenManager struct {
	oauthClient *http.Client
	credCache   CredCacheImplementation

	// Stash the credential info as we delete the environment variable after reading it, and we need to get it multiple times.
	stashedInfo *OAuthTokenInfo
}

// NewUserOAuthTokenManagerInstance creates a token manager instance.
func NewUserOAuthTokenManagerInstance(credCacheOptions CredCacheOptions) *UserOAuthTokenManager {
	return &UserOAuthTokenManager{
		oauthClient: newAzcopyHTTPClient(),
		credCache:   NewCredCache(credCacheOptions),
	}
}

func newAzcopyHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: GlobalProxyLookup,
			// We use Dial instead of DialContext as DialContext has been reported to cause slower performance.
			Dial /*Context*/ : (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
				DualStack: true,
			}).Dial, /*Context*/
			MaxIdleConns:           0, // No limit
			MaxIdleConnsPerHost:    1000,
			IdleConnTimeout:        180 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     true,
			MaxResponseHeaderBytes: 0,
			// ResponseHeaderTimeout:  time.Duration{},
			// ExpectContinueTimeout:  time.Duration{},
		},
	}
}

// GetTokenInfo gets token info, it follows rule:
//  1. If there is token passed from environment variable(note this is only for testing purpose),
//     use token passed from environment variable.
//  2. Otherwise, try to get token from cache.
//
// This method either successfully return token, or return error.
func (uotm *UserOAuthTokenManager) GetTokenInfo(ctx context.Context) (*OAuthTokenInfo, error) {
	if uotm.stashedInfo != nil {
		return uotm.stashedInfo, nil
	}

	var tokenInfo *OAuthTokenInfo
	var err error
	if tokenInfo, err = uotm.getTokenInfoFromEnvVar(ctx); err == nil || !IsErrorEnvVarOAuthTokenInfoNotSet(err) {
		// Scenario-Test: unattended testing with oauthTokenInfo set through environment variable
		// Note: Whenever environment variable is set in the context, it will overwrite the cached token info.
		if err != nil { // this is the case when env var exists while get token info failed
			return nil, err
		}
	} else { // Scenario: session mode which get token from cache
		if tokenInfo, err = uotm.getCachedTokenInfo(ctx); err != nil {
			return nil, err
		}
	}

	if tokenInfo == nil || tokenInfo.IsEmpty() {
		return nil, errors.New("invalid state, cannot get valid token info")
	}

	uotm.stashedInfo = tokenInfo

	return tokenInfo, nil
}

func (uotm *UserOAuthTokenManager) validateAndPersistLogin(oAuthTokenInfo *OAuthTokenInfo) error {
	// Use default tenant ID and active directory endpoint, if nothing specified.
	if oAuthTokenInfo.Tenant == "" {
		oAuthTokenInfo.Tenant = DefaultTenantID
	}
	if oAuthTokenInfo.ActiveDirectoryEndpoint == "" {
		oAuthTokenInfo.ActiveDirectoryEndpoint = DefaultActiveDirectoryEndpoint
	}
	tc, err := oAuthTokenInfo.GetTokenCredential()
	if err != nil {
		return err
	}
	scopes := []string{StorageScope}
	_, err = tc.GetToken(context.TODO(), policy.TokenRequestOptions{Scopes: scopes, EnableCAE: true})
	if err != nil {
		return err
	}
	uotm.stashedInfo = oAuthTokenInfo

	if oAuthTokenInfo.Persist {
		err = uotm.credCache.SaveToken(*oAuthTokenInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (uotm *UserOAuthTokenManager) WorkloadIdentityLogin(persist bool) error {
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType: EAutoLoginType.Workload(),
		Persist:   persist,
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

func (uotm *UserOAuthTokenManager) AzCliLogin(tenantID string, persist bool) error {
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType: EAutoLoginType.AzCLI(),
		Tenant:    tenantID,
		Persist:   persist, // AzCLI creds do not need to be persisted, AzCLI handles persistence.
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

func (uotm *UserOAuthTokenManager) PSContextToken(tenantID string, persist bool) error {
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType: EAutoLoginType.PsCred(),
		Tenant:    tenantID,
		Persist:   persist, // Powershell creds do not need to be persisted, Powershell handles persistence.
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

// MSILogin tries to get token from MSI, persist indicates whether to cache the token on local disk.
func (uotm *UserOAuthTokenManager) MSILogin(identityInfo IdentityInfo, persist bool) error {
	if err := identityInfo.Validate(); err != nil {
		return err
	}

	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType:    EAutoLoginType.MSI(),
		IdentityInfo: identityInfo,
		Persist:      persist,
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

// SecretLogin is a UOTM shell for secretLoginNoUOTM.
func (uotm *UserOAuthTokenManager) SecretLogin(tenantID, activeDirectoryEndpoint, secret, applicationID string, persist bool) error {
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType:               EAutoLoginType.SPN(),
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		ApplicationID:           applicationID,
		SPNInfo: SPNInfo{
			Secret:   secret,
			CertPath: "",
		},
		Persist: persist,
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

// CertLogin non-interactively logs in using a specified certificate, certificate password, and activedirectory endpoint.
func (uotm *UserOAuthTokenManager) CertLogin(tenantID, activeDirectoryEndpoint, certPath, certPass, applicationID string, persist bool) error {
	absCertPath, _ := filepath.Abs(certPath)
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType:               EAutoLoginType.SPN(),
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		ApplicationID:           applicationID,
		SPNInfo: SPNInfo{
			Secret:   certPass,
			CertPath: absCertPath,
		},
		Persist: persist,
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

// CertLogin non-interactively logs in using a specified certificate, certificate password, and activedirectory endpoint.
func (uotm *UserOAuthTokenManager) SpnArgsLogin(tenantID, activeDirectoryEndpoint, certData, certPass, applicationID string, persist bool) error {
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType:               EAutoLoginType.SPN(),
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		ApplicationID:           applicationID,
		SPNInfo: SPNInfo{
			Secret:   certPass,
			CertData: certData,
			CertPath: "empty",
		},
		Persist: persist,
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

// UserLogin interactively logins in with specified tenantID and activeDirectoryEndpoint, persist indicates whether to
// cache the token on local disk.
func (uotm *UserOAuthTokenManager) UserLogin(tenantID, activeDirectoryEndpoint string, persist bool) error {
	oAuthTokenInfo := &OAuthTokenInfo{
		LoginType:               EAutoLoginType.Device(),
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		ApplicationID:           ApplicationID,
		DeviceCodeInfo:          &azidentity.AuthenticationRecord{},
		Persist:                 persist,
	}

	return uotm.validateAndPersistLogin(oAuthTokenInfo)
}

// getCachedTokenInfo get a fresh token from local disk cache.
// If access token is expired, it will refresh the token.
// If refresh token is expired, the method will fail and return failure reason.
// Fresh token is persisted if access token or refresh token is changed.
func (uotm *UserOAuthTokenManager) getCachedTokenInfo(ctx context.Context) (*OAuthTokenInfo, error) {
	hasToken, err := uotm.credCache.HasCachedToken()
	if err != nil {
		return nil, fmt.Errorf("no cached token found, please log in with azcopy's login command, %w", err)
	}
	if !hasToken {
		return nil, errors.New("no cached token found, please log in with azcopy's login command")
	}

	tokenInfo, err := uotm.credCache.LoadToken()
	if err != nil {
		return nil, fmt.Errorf("get cached token failed, %w", err)
	}

	freshToken, err := tokenInfo.Refresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("get cached token failed to ensure token fresh, please log in with azcopy's login command again, %w", err)
	}

	// Update token cache, if token is updated.
	if freshToken.AccessToken != tokenInfo.AccessToken || freshToken.RefreshToken != tokenInfo.RefreshToken {
		tokenInfo.Token = *freshToken
		if err := uotm.credCache.SaveToken(*tokenInfo); err != nil {
			return nil, err
		}
	}

	return tokenInfo, nil
}

// HasCachedToken returns if there is cached token in token manager.
func (uotm *UserOAuthTokenManager) HasCachedToken() (bool, error) {
	if uotm.stashedInfo != nil {
		return true, nil
	}

	return uotm.credCache.HasCachedToken()
}

// RemoveCachedToken delete all the cached token.
func (uotm *UserOAuthTokenManager) RemoveCachedToken() error {
	return uotm.credCache.RemoveCachedToken()
}

// ====================================================================================

// EnvVarOAuthTokenInfo passes oauth token info into AzCopy through environment variable.
// Note: this is only used for testing, and not encouraged to be used in production environments.
const EnvVarOAuthTokenInfo = "AZCOPY_OAUTH_TOKEN_INFO"

// ErrorCodeEnvVarOAuthTokenInfoNotSet defines error code when environment variable AZCOPY_OAUTH_TOKEN_INFO is not set.
const ErrorCodeEnvVarOAuthTokenInfoNotSet = "environment variable AZCOPY_OAUTH_TOKEN_INFO is not set"

var stashedEnvOAuthTokenExists = false

// EnvVarOAuthTokenInfoExists verifies if environment variable for OAuthTokenInfo is specified.
// The method returns true if the environment variable is set.
// Note: This is useful for only checking whether the env var exists, please use getTokenInfoFromEnvVar
// directly in the case getting token info is necessary.
func EnvVarOAuthTokenInfoExists() bool {
	if GetEnvironmentVariable(EEnvironmentVariable.OAuthTokenInfo()) == "" && !stashedEnvOAuthTokenExists {
		return false
	}
	stashedEnvOAuthTokenExists = true
	return true
}

// IsErrorEnvVarOAuthTokenInfoNotSet verifies if an error indicates environment variable AZCOPY_OAUTH_TOKEN_INFO is not set.
func IsErrorEnvVarOAuthTokenInfoNotSet(err error) bool {
	if err != nil && strings.Contains(err.Error(), ErrorCodeEnvVarOAuthTokenInfoNotSet) {
		return true
	}
	return false
}

// getTokenInfoFromEnvVar gets token info from environment variable.
func (uotm *UserOAuthTokenManager) getTokenInfoFromEnvVar(ctx context.Context) (*OAuthTokenInfo, error) {
	rawToken := GetEnvironmentVariable(EEnvironmentVariable.OAuthTokenInfo())
	if rawToken == "" {
		return nil, errors.New(ErrorCodeEnvVarOAuthTokenInfoNotSet)
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectedly.
	ClearEnvironmentVariable(EEnvironmentVariable.OAuthTokenInfo())

	tokenInfo, err := jsonToTokenInfo([]byte(rawToken))
	if err != nil {
		return nil, fmt.Errorf("get token from environment variable failed to unmarshal token, %w", err)
	}

	if tokenInfo.LoginType != EAutoLoginType.TokenStore() {
		refreshedToken, err := tokenInfo.Refresh(ctx)
		if err != nil {
			return nil, fmt.Errorf("get token from environment variable failed to ensure token fresh, %w", err)
		}
		tokenInfo.Token = *refreshedToken
	}

	return tokenInfo, nil
}

// ====================================================================================

// OAuthTokenInfo contains info necessary for refresh OAuth credentials.
type OAuthTokenInfo struct {
	azcore.TokenCredential `json:"-"`
	Token
	Tenant                  string        `json:"_tenant"`
	ActiveDirectoryEndpoint string        `json:"_ad_endpoint"`
	LoginType               AutoLoginType `json:"_token_refresh_source"`
	ApplicationID           string        `json:"_application_id"`
	IdentityInfo            IdentityInfo
	SPNInfo                 SPNInfo
	// Note: ClientID should be only used for internal integrations through env var with refresh token.
	// It indicates the Application ID assigned to your app when you registered it with Azure AD.
	// In this case AzCopy refresh token on behalf of caller.
	// For more details, please refer to
	// https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-protocols-oauth-code#refreshing-the-access-tokens
	ClientID       string                           `json:"_client_id"`
	DeviceCodeInfo *azidentity.AuthenticationRecord `json:"_authentication_record,omitempty"`
	Persist        bool                             `json:"_persist"`
}

// Token encapsulates the access token used to authorize Azure requests.
// https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-oauth2-client-creds-grant-flow#service-to-service-access-token-response
type Token struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`

	ExpiresIn json.Number `json:"expires_in"`
	ExpiresOn json.Number `json:"expires_on"`
	NotBefore json.Number `json:"not_before"`

	Resource string `json:"resource"`
	Type     string `json:"token_type"`
}

// IsZero returns true if the token object is zero-initialized.
func (t Token) IsZero() bool {
	return t == Token{}
}

// Expires returns the time.Time when the Token expires.
func (t Token) Expires() time.Time {
	s, err := t.ExpiresOn.Float64()
	if err != nil {
		s = -3600
	}

	expiration := date.NewUnixTimeFromSeconds(s)

	return time.Time(expiration).UTC()
}

// IsExpired returns true if the Token is expired, false otherwise.
func (t Token) IsExpired() bool {
	return !t.Expires().After(time.Now().Add(0))
}

// IdentityInfo contains info for MSI.
type IdentityInfo struct {
	ClientID string `json:"_identity_client_id"`
	ObjectID string `json:"_identity_object_id"`
	MSIResID string `json:"_identity_msi_res_id"`
}

// SPNInfo contains info for authenticating with Service Principal Names
type SPNInfo struct {
	// Secret is used for two purposes: The certificate secret, and a client secret.
	// The secret is persisted to the JSON file because AAD does not issue a refresh token.
	// Thus, the original secret is needed to refresh.
	Secret   string `json:"_spn_secret"`
	CertPath string `json:"_spn_cert_path"`
	CertData string `json:"_spn_cert_data"` // certData is used for SPN login with certData directly passed in
}

// Validate validates identity info, at most only one of clientID, objectID or MSI resource ID could be set.
func (identityInfo *IdentityInfo) Validate() error {
	v := make(map[string]bool, 3)
	if identityInfo.ClientID != "" {
		v[identityInfo.ClientID] = true
	}
	if identityInfo.ObjectID != "" {
		v[identityInfo.ObjectID] = true
	}
	if identityInfo.MSIResID != "" {
		v[identityInfo.MSIResID] = true
	}
	if len(v) > 1 {
		return errors.New("client ID, object ID and MSI resource ID are mutually exclusive")
	}
	return nil
}

// Refresh gets new token with token info.
func (credInfo *OAuthTokenInfo) Refresh(ctx context.Context) (*Token, error) {
	// TODO: I think this method is only necessary until datalake is migrated.
	// Returns cached TokenCredential or creates a new one if it hasn't been created yet.
	tc, err := credInfo.GetTokenCredential()
	if err != nil {
		return nil, err
	}
	scopes := []string{StorageScope}
	t, err := tc.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes, EnableCAE: true})
	if err != nil {
		return nil, err
	}
	return &Token{
		AccessToken: t.Token,
		ExpiresOn:   json.Number(strconv.FormatInt(int64(t.ExpiresOn.Sub(date.UnixEpoch())/time.Second), 10)),
	}, nil
}

// Single instance token store credential cache shared by entire azcopy process.
var tokenStoreCredCache CredCacheImplementation = NewCredCacheInternalIntegration(CredCacheOptions{
	KeyName:     "azcopy/aadtoken/" + strconv.Itoa(os.Getpid()),
	ServiceName: "azcopy",
	AccountName: "aadtoken/" + strconv.Itoa(os.Getpid()),
})

// IsEmpty returns if current OAuthTokenInfo is empty and doesn't contain any useful info.
func (credInfo OAuthTokenInfo) IsEmpty() bool {
	if credInfo.Tenant == "" && credInfo.ActiveDirectoryEndpoint == "" && credInfo.Token.IsZero() {
		return true
	}

	return false
}

// toJSON converts OAuthTokenInfo to json format.
func (credInfo OAuthTokenInfo) toJSON() ([]byte, error) {
	return json.Marshal(credInfo)
}

func getAuthorityURL(activeDirectoryEndpoint string) (*url.URL, error) {
	u, err := url.Parse(activeDirectoryEndpoint)
	if err != nil {
		return nil, err
	}
	return u, nil
}

const minimumTokenValidDuration = time.Minute * 5

type TokenStoreCredential struct {
	token     *azcore.AccessToken
	lock      sync.RWMutex
	credCache CredCacheImplementation
}

// globalTokenStoreCredential is created to make sure that all
// service clients share same cred object. This is required so that
// we do not make repeated GetToken calls.
// This is a temporary fix for issue where we would request a
// new token from Stg Exp even while they've not yet populated the
// tokenstore.
//
// This is okay because we use same credential on both source and
// destination. If we move to a case where the credentials are
// different, this should be removed.
//
// We should move to a method where the token is always read  from
// tokenstore, and azcopy is invoked after tokenstore is populated.
var globalTokenStoreCredential *TokenStoreCredential
var globalTsc sync.Once

func (tsc *TokenStoreCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	// if the token we have has not expired, return the same.
	tsc.lock.RLock()
	if rem := time.Until(tsc.token.ExpiresOn); rem > minimumTokenValidDuration {
		tsc.lock.RUnlock() // return path, so we must release the read lock here as well.
		return *tsc.token, nil
	}
	tsc.lock.RUnlock()

	tsc.lock.Lock()
	defer tsc.lock.Unlock()

	hasToken, err := tsc.credCache.HasCachedToken()
	if err != nil || !hasToken {
		AzcopyCurrentJobLogger.Log(LogDebug, fmt.Sprintf("no token found %v", err))
		return azcore.AccessToken{}, fmt.Errorf("no cached token found in Token Store Mode(SE), %w", err)
	}

	tokenInfo, err := tsc.credCache.LoadToken()
	if err != nil {
		AzcopyCurrentJobLogger.Log(LogDebug, fmt.Sprintf("get token failed %s", err.Error()))
		return azcore.AccessToken{}, fmt.Errorf("get cached token failed in Token Store Mode(SE), %w", err)
	}

	tsc.token = &azcore.AccessToken{
		Token:     tokenInfo.AccessToken,
		ExpiresOn: tokenInfo.Expires(),
	}

	return *tsc.token, nil

}

// GetTokenStoreCredential gets token from token store. (Credential Manager in Windows, keyring in Linux and keychain in MacOS.)
// Note: This approach should only be used in internal integrations.
func GetTokenStoreCredential(accessToken string, expiresOn time.Time) azcore.TokenCredential {
	globalTsc.Do(func() {
		globalTokenStoreCredential = &TokenStoreCredential{
			token: &azcore.AccessToken{
				Token:     accessToken,
				ExpiresOn: expiresOn,
			},
			credCache: tokenStoreCredCache,
		}
	})
	return globalTokenStoreCredential
}

func (credInfo *OAuthTokenInfo) GetTokenStoreCredential() (azcore.TokenCredential, error) {
	credInfo.TokenCredential = GetTokenStoreCredential(credInfo.AccessToken, credInfo.Expires())
	return credInfo.TokenCredential, nil
}

func (credInfo *OAuthTokenInfo) GetManagedIdentityCredential() (azcore.TokenCredential, error) {
	var id azidentity.ManagedIDKind
	if credInfo.IdentityInfo.ClientID != "" {
		id = azidentity.ClientID(credInfo.IdentityInfo.ClientID)
	} else if credInfo.IdentityInfo.MSIResID != "" {
		id = azidentity.ResourceID(credInfo.IdentityInfo.MSIResID)
	} else if credInfo.IdentityInfo.ObjectID != "" {
		return nil, fmt.Errorf("object ID is deprecated and no longer supported for managed identity. Please use client ID or resource ID instead")
	}

	tc, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: newAzcopyHTTPClient(),
		},
		ID: id,
	})
	if err != nil {
		return nil, err
	}
	credInfo.TokenCredential = tc
	return tc, nil
}

func (credInfo *OAuthTokenInfo) GetClientCertificateCredential() (azcore.TokenCredential, error) {
	authorityHost, err := getAuthorityURL(credInfo.ActiveDirectoryEndpoint)
	if err != nil {
		return nil, err
	}
	var certData []byte
	if credInfo.SPNInfo.CertData != "" {
		certData = []byte(credInfo.SPNInfo.CertData)
		if err != nil {
			return nil, err
		}
	} else {
		certData, err = os.ReadFile(credInfo.SPNInfo.CertPath)
		if err != nil {
			return nil, err
		}
	}
	certs, key, err := azidentity.ParseCertificates(certData, []byte(credInfo.SPNInfo.Secret))
	if err != nil {
		return nil, err
	}
	tc, err := azidentity.NewClientCertificateCredential(credInfo.Tenant, credInfo.ApplicationID, certs, key, &azidentity.ClientCertificateCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
			Transport: newAzcopyHTTPClient(),
		},
		SendCertificateChain: true,
	})
	if err != nil {
		return nil, err
	}
	credInfo.TokenCredential = tc
	return tc, nil
}

func (credInfo *OAuthTokenInfo) GetClientSecretCredential() (azcore.TokenCredential, error) {
	authorityHost, err := getAuthorityURL(credInfo.ActiveDirectoryEndpoint)
	if err != nil {
		return nil, err
	}
	tc, err := azidentity.NewClientSecretCredential(credInfo.Tenant, credInfo.ApplicationID, credInfo.SPNInfo.Secret, &azidentity.ClientSecretCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
			Transport: newAzcopyHTTPClient(),
		},
	})
	if err != nil {
		return nil, err
	}
	credInfo.TokenCredential = tc
	return tc, nil
}

func (credInfo *OAuthTokenInfo) GetAzCliCredential() (azcore.TokenCredential, error) {
	if credInfo.Tenant == DefaultTenantID {
		credInfo.Tenant = ""
	}

	tc, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{TenantID: credInfo.Tenant})
	if err != nil {
		return nil, err
	}
	credInfo.TokenCredential = tc
	return tc, nil
}

func (credInfo *OAuthTokenInfo) GetPSContextCredential() (azcore.TokenCredential, error) {
	if credInfo.Tenant == DefaultTenantID {
		credInfo.Tenant = ""
	}

	tc, err := NewPowershellContextCredential(&PowershellContextCredentialOptions{TenantID: credInfo.Tenant})
	if err != nil {
		return nil, err
	}
	credInfo.TokenCredential = tc
	return tc, nil
}

func (credInfo *OAuthTokenInfo) GetWorkloadIdentityCredential() (azcore.TokenCredential, error) {
	tc, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: newAzcopyHTTPClient(),
		},
	})
	if err != nil {
		return nil, err
	}
	credInfo.TokenCredential = tc
	return tc, nil
}

func (credInfo *OAuthTokenInfo) GetDeviceCodeCredential() (azcore.TokenCredential, error) {
	authorityHost, err := getAuthorityURL(credInfo.ActiveDirectoryEndpoint)
	if err != nil {
		return nil, err
	}
	var persistentCache azidentity.Cache
	if credInfo.Persist {
		persistentCache, err = cache.New(&cache.Options{
			Name: TokenCache,
		})
		if err != nil {
			return nil, err
		}
	}
	// Read the record
	record := IffNotNil(credInfo.DeviceCodeInfo, azidentity.AuthenticationRecord{})
	tc, err := azidentity.NewDeviceCodeCredential(&azidentity.DeviceCodeCredentialOptions{
		TenantID:                       credInfo.Tenant,
		ClientID:                       ApplicationID,
		DisableAutomaticAuthentication: true,
		Cache:                          persistentCache,
		AuthenticationRecord:           record,
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
			Transport: newAzcopyHTTPClient(),
		},
		UserPrompt: func(ctx context.Context, message azidentity.DeviceCodeMessage) error {
			lcm.Info(fmt.Sprintf("Authentication is required. To sign in, open the webpage %s and enter the code %s to authenticate.",
				Iff(message.VerificationURL != "", message.VerificationURL, "https://aka.ms/devicelogin"), message.UserCode))
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	if record == (azidentity.AuthenticationRecord{}) {
		// No stored record; call Authenticate to acquire one
		record, err = tc.Authenticate(context.TODO(), &policy.TokenRequestOptions{
			EnableCAE: true,
			Scopes:    []string{StorageScope},
		})
		if err != nil {
			return nil, err
		}
		if credInfo.Tenant == DefaultTenantID {
			fmt.Println("INFO: Logging in under the \"Common\" tenant. This will log the account in under its home tenant.")
			fmt.Println("INFO: If you plan to use AzCopy with a B2B account (where the account's home tenant is separate from the tenant of the target storage account), please sign in under the target tenant with --tenant-id")
		}
		// Store the record
		credInfo.DeviceCodeInfo = &record
	}

	credInfo.TokenCredential = tc
	return tc, nil
}

type AuthenticateToken interface {
	azcore.TokenCredential
	Authenticate(ctx context.Context, opts *policy.TokenRequestOptions) (azidentity.AuthenticationRecord, error)
}

func (credInfo *OAuthTokenInfo) GetTokenCredential() (azcore.TokenCredential, error) {
	// Token Credential is cached.
	if credInfo.TokenCredential != nil {
		return credInfo.TokenCredential, nil
	}

	if credInfo.LoginType == EAutoLoginType.TokenStore() {
		return credInfo.GetTokenStoreCredential()
	}

	switch credInfo.LoginType {
	case EAutoLoginType.MSI():
		return credInfo.GetManagedIdentityCredential()
	case EAutoLoginType.SPN():
		if credInfo.SPNInfo.CertPath != "" {
			return credInfo.GetClientCertificateCredential()
		} else {
			return credInfo.GetClientSecretCredential()
		}
	case EAutoLoginType.AzCLI():
		return credInfo.GetAzCliCredential()
	case EAutoLoginType.PsCred():
		return credInfo.GetPSContextCredential()
	case EAutoLoginType.Workload():
		return credInfo.GetWorkloadIdentityCredential()
	case EAutoLoginType.Device():
		return credInfo.GetDeviceCodeCredential()
	default:
		return nil, fmt.Errorf("invalid auto-login type specified: %s", credInfo.LoginType)
	}
}

// jsonToTokenInfo converts bytes to OAuthTokenInfo
func jsonToTokenInfo(b []byte) (*OAuthTokenInfo, error) {
	var OAuthTokenInfo OAuthTokenInfo
	if err := json.Unmarshal(b, &OAuthTokenInfo); err != nil {
		return nil, err
	}
	if OAuthTokenInfo.LoginType == EAutoLoginType.TokenStore() {
		_, _ = OAuthTokenInfo.GetTokenStoreCredential()
	}
	return &OAuthTokenInfo, nil
}

// ====================================================================================

// TestOAuthInjection controls variables for OAuth testing injections
type TestOAuthInjection struct {
	DoTokenRefreshInjection bool
	TokenRefreshDuration    time.Duration
}

// GlobalTestOAuthInjection is the global setting for OAuth testing injection control
var GlobalTestOAuthInjection = TestOAuthInjection{
	DoTokenRefreshInjection: false,
	TokenRefreshDuration:    time.Second * 10,
}

// TODO: Add pipeline policy for token refreshing validating.
