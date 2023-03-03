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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ApplicationID represents 1st party ApplicationID for AzCopy.
// const ApplicationID = "a45c21f4-7066-40b4-97d8-14f4313c3caa" // 3rd party test ApplicationID for AzCopy.
const ApplicationID = "579a7132-0e58-4d80-b1e1-7a1e2d337859"

// Resource used in azure storage OAuth authentication
const Resource = "https://storage.azure.com/.default"
const MDResource = "https://disk.azure.com/" // There must be a trailing slash-- The service checks explicitly for "https://disk.azure.com/"
const DefaultTenantID = "common"
const DefaultActiveDirectoryEndpoint = "https://login.microsoftonline.com"

// UserOAuthTokenManager for token management.
type UserOAuthTokenManager struct {
	credCache *CredCache

	// Stash the credential info as we delete the environment variable after reading it, and we need to get it multiple times.
	stashedInfo *OAuthTokenInfo
}

// NewUserOAuthTokenManagerInstance creates a token manager instance.
func NewUserOAuthTokenManagerInstance(credCacheOptions CredCacheOptions) *UserOAuthTokenManager {
	return &UserOAuthTokenManager{
		credCache: NewCredCache(credCacheOptions),
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
			}).Dial,                   /*Context*/
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

// MSILogin tries to get token from MSI, persist indicates whether to cache the token on local disk.
func (uotm *UserOAuthTokenManager) MSILogin(ctx context.Context, identityInfo IdentityInfo, persist bool) (*OAuthTokenInfo, error) {
	if err := identityInfo.Validate(); err != nil {
		return nil, err
	}

	oAuthTokenInfo := &OAuthTokenInfo{
		Identity:     true,
		IdentityInfo: identityInfo,
	}
	token, err := oAuthTokenInfo.GetNewTokenFromMSI(ctx)
	if err != nil {
		return nil, err
	}
	oAuthTokenInfo.AccessToken = *token
	uotm.stashedInfo = oAuthTokenInfo

	if persist {
		err = uotm.credCache.SaveToken(*oAuthTokenInfo)
		if err != nil {
			return nil, err
		}
	}

	return oAuthTokenInfo, nil
}

func getAuthorityURL(tenantID, activeDirectoryEndpoint string) (*url.URL, error) {
	u, err := url.Parse(activeDirectoryEndpoint)
	if err != nil {
		return nil, err
	}
	return u.Parse(tenantID)
}

// secretLoginNoUOTM non-interactively logs in with a client secret.
func secretLoginNoUOTM(tenantID, activeDirectoryEndpoint, secret, applicationID, resource string) (*OAuthTokenInfo, error) {
	if tenantID == "" {
		tenantID = DefaultTenantID
	}

	if activeDirectoryEndpoint == "" {
		activeDirectoryEndpoint = DefaultActiveDirectoryEndpoint
	}

	if applicationID == "" {
		return nil, fmt.Errorf("please supply your OWN application ID")
	}

	authorityHost, err := getAuthorityURL(tenantID, activeDirectoryEndpoint)
	if err != nil {
		return nil, err
	}

	spn, err := azidentity.NewClientSecretCredential(tenantID, applicationID, secret, &azidentity.ClientSecretCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
			Transport: newAzcopyHTTPClient(),
		},
	})
	if err != nil {
		return nil, err
	}

	scopes := []string{resource}

	accessToken, err := spn.GetToken(context.TODO(), policy.TokenRequestOptions{Scopes: scopes})
	if err != nil {
		return nil, err
	}
	oAuthTokenInfo := OAuthTokenInfo{
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		ApplicationID:           applicationID,
		Resource:                resource,
		ServicePrincipalName:    true,
		SPNInfo: SPNInfo{
			Secret:   secret,
			CertPath: "",
		},
		AccessToken: accessToken,
	}

	return &oAuthTokenInfo, nil
}

// SecretLogin is a UOTM shell for secretLoginNoUOTM.
func (uotm *UserOAuthTokenManager) SecretLogin(tenantID, activeDirectoryEndpoint, secret, applicationID string, persist bool) (*OAuthTokenInfo, error) {
	oAuthTokenInfo, err := secretLoginNoUOTM(tenantID, activeDirectoryEndpoint, secret, applicationID, Resource)

	if err != nil {
		return nil, err
	}

	uotm.stashedInfo = oAuthTokenInfo
	if persist {
		err = uotm.credCache.SaveToken(*oAuthTokenInfo)
		if err != nil {
			return nil, err
		}
	}

	return oAuthTokenInfo, nil
}

// GetNewTokenFromSecret is a refresh shell for secretLoginNoUOTM
func (credInfo *OAuthTokenInfo) GetNewTokenFromSecret(ctx context.Context) (*azcore.AccessToken, error) {
	targetResource := Resource
	if credInfo.Resource != "" && credInfo.Resource != targetResource {
		targetResource = credInfo.Resource
	}

	tokeninfo, err := secretLoginNoUOTM(credInfo.Tenant, credInfo.ActiveDirectoryEndpoint, credInfo.SPNInfo.Secret, credInfo.ApplicationID, targetResource)

	if err != nil {
		return nil, err
	} else {
		return &tokeninfo.AccessToken, nil
	}
}

func certLoginNoUOTM(tenantID, activeDirectoryEndpoint, certPath, certPass, applicationID, resource string) (*OAuthTokenInfo, error) {
	if tenantID == "" {
		tenantID = DefaultTenantID
	}

	if activeDirectoryEndpoint == "" {
		activeDirectoryEndpoint = DefaultActiveDirectoryEndpoint
	}

	if applicationID == "" {
		return nil, fmt.Errorf("please supply your OWN application ID")
	}

	authorityHost, err := getAuthorityURL(tenantID, activeDirectoryEndpoint)
	if err != nil {
		return nil, err
	}

	certData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	certs, key, err := azidentity.ParseCertificates(certData, []byte(certPass))

	spt, err := azidentity.NewClientCertificateCredential(tenantID, applicationID, certs, key, &azidentity.ClientCertificateCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
			Transport: newAzcopyHTTPClient(),
		},
	})
	if err != nil {
		return nil, err
	}

	scopes := []string{resource}

	accessToken, err := spt.GetToken(context.TODO(), policy.TokenRequestOptions{Scopes: scopes})
	if err != nil {
		return nil, err
	}

	cpfq, _ := filepath.Abs(certPath)
	oAuthTokenInfo := OAuthTokenInfo{
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		ApplicationID:           applicationID,
		Resource:                resource,
		ServicePrincipalName:    true,
		SPNInfo: SPNInfo{
			Secret:   certPass,
			CertPath: cpfq,
		},
		AccessToken: accessToken,
	}

	return &oAuthTokenInfo, nil
}

// CertLogin non-interactively logs in using a specified certificate, certificate password, and activedirectory endpoint.
func (uotm *UserOAuthTokenManager) CertLogin(tenantID, activeDirectoryEndpoint, certPath, certPass, applicationID string, persist bool) (*OAuthTokenInfo, error) {
	// TODO: Global default cert flag for true non interactive login?
	// (Also could be useful if the user has multiple certificates they want to switch between in the same file.)
	oAuthTokenInfo, err := certLoginNoUOTM(tenantID, activeDirectoryEndpoint, certPath, certPass, applicationID, Resource)
	uotm.stashedInfo = oAuthTokenInfo

	if persist && err == nil {
		err = uotm.credCache.SaveToken(*oAuthTokenInfo)
		if err != nil {
			return nil, err
		}
	}

	return oAuthTokenInfo, err
}

// GetNewTokenFromCert refreshes a token manually from a certificate.
func (credInfo *OAuthTokenInfo) GetNewTokenFromCert(ctx context.Context) (*azcore.AccessToken, error) {
	targetResource := Resource
	if credInfo.Resource != "" && credInfo.Resource != targetResource {
		targetResource = credInfo.Resource
	}

	tokeninfo, err := certLoginNoUOTM(credInfo.Tenant, credInfo.ActiveDirectoryEndpoint, credInfo.SPNInfo.CertPath, credInfo.SPNInfo.Secret, credInfo.ApplicationID, targetResource)

	if err != nil {
		return nil, err
	} else {
		return &tokeninfo.AccessToken, nil
	}
}

// UserLogin interactively logins in with specified tenantID and activeDirectoryEndpoint, persist indicates whether to
// cache the token on local disk.
func (uotm *UserOAuthTokenManager) UserLogin(tenantID, activeDirectoryEndpoint string, persist bool) (*OAuthTokenInfo, error) {
	// Use default tenant ID and active directory endpoint, if nothing specified.
	if tenantID == "" {
		tenantID = DefaultTenantID
	}
	if activeDirectoryEndpoint == "" {
		activeDirectoryEndpoint = DefaultActiveDirectoryEndpoint
	}

	authorityHost, err := getAuthorityURL(tenantID, activeDirectoryEndpoint)
	if err != nil {
		return nil, err
	}

	dcc, err := azidentity.NewDeviceCodeCredential(&azidentity.DeviceCodeCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
			Transport: newAzcopyHTTPClient(),
		},
		TenantID: tenantID,
		ClientID: ApplicationID,
		UserPrompt: func(ctx context.Context, message azidentity.DeviceCodeMessage) error {
			fmt.Println(message.Message + "\n")
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to login with tenantID %q, Azure directory endpoint %q, %v",
			tenantID, activeDirectoryEndpoint, err)
	}

	if tenantID == "" || tenantID == "common" {
		fmt.Println("INFO: Logging in under the \"Common\" tenant. This will log the account in under its home tenant.")
		fmt.Println("INFO: If you plan to use AzCopy with a B2B account (where the account's home tenant is separate from the tenant of the target storage account), please sign in under the target tenant with --tenant-id")
	}

	scopes := []string{Resource}
	// TODO: Go SDK has new method which supports context, currently ctrl-C can stop the login in console interactively.
	accessToken, err := dcc.GetToken(context.TODO(), policy.TokenRequestOptions{Scopes: scopes})
	if err != nil {
		return nil, fmt.Errorf("failed to login with tenantID %q, Azure directory endpoint %q, %v",
			tenantID, activeDirectoryEndpoint, err)
	}

	oAuthTokenInfo := OAuthTokenInfo{
		AccessToken:             accessToken,
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
	}
	uotm.stashedInfo = &oAuthTokenInfo

	// to dump for diagnostic purposes:
	// buf, _ := json.Marshal(oAuthTokenInfo)
	// panic("don't check me in. Buf is " + string(buf))

	if persist {
		err = uotm.credCache.SaveToken(oAuthTokenInfo)
		if err != nil {
			return nil, err
		}
	}

	return &oAuthTokenInfo, nil
}

// getCachedTokenInfo get a fresh token from local disk cache.
// If access token is expired, it will refresh the token.
// If refresh token is expired, the method will fail and return failure reason.
// Fresh token is persisted if access token or refresh token is changed.
func (uotm *UserOAuthTokenManager) getCachedTokenInfo(ctx context.Context) (*OAuthTokenInfo, error) {
	hasToken, err := uotm.credCache.HasCachedToken()
	if err != nil {
		return nil, fmt.Errorf("no cached token found, please log in with azcopy's login command, %v", err)
	}
	if !hasToken {
		return nil, errors.New("no cached token found, please log in with azcopy's login command")
	}

	tokenInfo, err := uotm.credCache.LoadToken()
	if err != nil {
		return nil, fmt.Errorf("get cached token failed, %v", err)
	}

	freshToken, err := tokenInfo.Refresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("get cached token failed to ensure token fresh, please log in with azcopy's login command again, %v", err)
	}

	// Update token cache, if token is updated.
	if freshToken.Token != tokenInfo.Token || freshToken.ExpiresOn != tokenInfo.ExpiresOn {
		tokenInfo.AccessToken = *freshToken
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
	if lcm.GetEnvironmentVariable(EEnvironmentVariable.OAuthTokenInfo()) == "" && !stashedEnvOAuthTokenExists {
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
	rawToken := lcm.GetEnvironmentVariable(EEnvironmentVariable.OAuthTokenInfo())
	if rawToken == "" {
		return nil, errors.New(ErrorCodeEnvVarOAuthTokenInfoNotSet)
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectedly.
	lcm.ClearEnvironmentVariable(EEnvironmentVariable.OAuthTokenInfo())

	tokenInfo, err := jsonToTokenInfo([]byte(rawToken))
	if err != nil {
		return nil, fmt.Errorf("get token from environment variable failed to unmarshal token, %v", err)
	}

	if tokenInfo.TokenRefreshSource != TokenRefreshSourceTokenStore {
		refreshedToken, err := tokenInfo.Refresh(ctx)
		if err != nil {
			return nil, fmt.Errorf("get token from environment variable failed to ensure token fresh, %v", err)
		}
		tokenInfo.AccessToken = *refreshedToken
	}

	return tokenInfo, nil
}

// ====================================================================================

// TokenRefreshSourceTokenStore indicates enabling azcopy oauth integration through tokenstore.
// Note: This should be only used for internal integrations.
const TokenRefreshSourceTokenStore = "tokenstore"

// OAuthTokenInfo contains info necessary for refresh OAuth credentials.
type OAuthTokenInfo struct {
	azcore.AccessToken
	Tenant                  string `json:"_tenant"`
	ActiveDirectoryEndpoint string `json:"_ad_endpoint"`
	TokenRefreshSource      string `json:"_token_refresh_source"`
	ApplicationID           string `json:"_application_id"`
	Identity                bool   `json:"_identity"`
	IdentityInfo            IdentityInfo
	ServicePrincipalName    bool   `json:"_spn"`
	Resource                string `json:"_resource"`
	SPNInfo                 SPNInfo
	// Note: ClientID should be only used for internal integrations through env var with refresh token.
	// It indicates the Application ID assigned to your app when you registered it with Azure AD.
	// In this case AzCopy refresh token on behalf of caller.
	// For more details, please refer to
	// https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-protocols-oauth-code#refreshing-the-access-tokens
	ClientID string `json:"_client_id"`
}

// Expires returns the time.Time when the Token expires.
func (t OAuthTokenInfo) Expires() time.Time {
	return t.ExpiresOn.UTC()
}

// IsExpired returns true if the Token is expired, false otherwise.
func (t OAuthTokenInfo) IsExpired() bool {
	return !t.Expires().After(time.Now())
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
func (credInfo *OAuthTokenInfo) Refresh(ctx context.Context) (*azcore.AccessToken, error) {
	// StorageExplorer Integration
	if credInfo.TokenRefreshSource == TokenRefreshSourceTokenStore {
		return credInfo.GetNewTokenFromTokenStore(ctx)
	}

	if credInfo.Identity {
		return credInfo.GetNewTokenFromMSI(ctx)
	}

	if credInfo.ServicePrincipalName {
		if credInfo.SPNInfo.CertPath != "" {
			return credInfo.GetNewTokenFromCert(ctx)
		} else {
			return credInfo.GetNewTokenFromSecret(ctx)
		}
	}

	// Token Credential Cache (Device Code login)
	return credInfo.RefreshTokenWithUserCredential(ctx)
}

// Single instance token store credential cache shared by entire azcopy process.
var tokenStoreCredCache = NewCredCacheInternalIntegration(CredCacheOptions{
	KeyName:     "azcopy/aadtoken/" + strconv.Itoa(os.Getpid()),
	ServiceName: "azcopy",
	AccountName: "aadtoken/" + strconv.Itoa(os.Getpid()),
})

// GetNewTokenFromTokenStore gets token from token store. (Credential Manager in Windows, keyring in Linux and keychain in MacOS.)
// Note: This approach should only be used in internal integrations.
func (credInfo *OAuthTokenInfo) GetNewTokenFromTokenStore(ctx context.Context) (*azcore.AccessToken, error) {
	hasToken, err := tokenStoreCredCache.HasCachedToken()
	if err != nil || !hasToken {
		return nil, fmt.Errorf("no cached token found in Token Store Mode(SE), %v", err)
	}

	tokenInfo, err := tokenStoreCredCache.LoadToken()
	if err != nil {
		return nil, fmt.Errorf("get cached token failed in Token Store Mode(SE), %v", err)
	}
	return &(tokenInfo.AccessToken), nil
}

// GetNewTokenFromMSI gets token from Azure Instance Metadata Service identity endpoint. It first checks if the VM is registered with Azure Arc. Failing that case, it checks if it is an Azure VM.
// For details, please refer to https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
// Note: The msiTokenHTTPClient timeout is has been reduced from 30 sec to 10 sec as IMDS endpoint is local to the machine.
// Without this change, if some router is configured to not return "ICMP unreachable" then it will take 30 secs to timeout and increase the response time.
// We are additionally checking Arc first, and then Azure VM because Arc endpoint is local so as to further reduce the response time of the Azure VM IMDS endpoint.
func (credInfo *OAuthTokenInfo) GetNewTokenFromMSI(ctx context.Context) (*azcore.AccessToken, error) {
	targetResource := Resource
	if credInfo.Resource != "" && credInfo.Resource != targetResource {
		targetResource = credInfo.Resource
	}

	mi, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		return nil, err
	}

	scopes := []string{targetResource}

	accessToken, err := mi.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
	if err != nil {
		return nil, err
	}

	oAuthTokenInfo := OAuthTokenInfo{
		AccessToken: accessToken,
	}

	return &oAuthTokenInfo.AccessToken, nil
}

// RefreshTokenWithUserCredential gets new token with user credential through refresh.
func (credInfo *OAuthTokenInfo) RefreshTokenWithUserCredential(ctx context.Context) (*azcore.AccessToken, error) {
	// TODO (gapra) : Add logic to refresh the device code credential.
	return &credInfo.AccessToken, nil
}

// IsEmpty returns if current OAuthTokenInfo is empty and doesn't contain any useful info.
func (credInfo OAuthTokenInfo) IsEmpty() bool {
	if credInfo.Tenant == "" && credInfo.ActiveDirectoryEndpoint == "" && credInfo.AccessToken.Token == "" && credInfo.AccessToken.ExpiresOn.IsZero() && !credInfo.Identity {
		return true
	}

	return false
}

// toJSON converts OAuthTokenInfo to json format.
func (credInfo OAuthTokenInfo) toJSON() ([]byte, error) {
	return json.Marshal(credInfo)
}

// jsonToTokenInfo converts bytes to OAuthTokenInfo
func jsonToTokenInfo(b []byte) (*OAuthTokenInfo, error) {
	var OAuthTokenInfo OAuthTokenInfo
	if err := json.Unmarshal(b, &OAuthTokenInfo); err != nil {
		return nil, err
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
