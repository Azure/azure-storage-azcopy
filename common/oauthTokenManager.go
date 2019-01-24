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
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/go-autorest/autorest/adal"
)

// ApplicationID represents 1st party ApplicationID for AzCopy.
//const ApplicationID = "a45c21f4-7066-40b4-97d8-14f4313c3caa" // 3rd party test ApplicationID for AzCopy.
const ApplicationID = "579a7132-0e58-4d80-b1e1-7a1e2d337859"

// Resource used in azure storage OAuth authentication
const Resource = "https://storage.azure.com"
const DefaultTenantID = "common"
const DefaultActiveDirectoryEndpoint = "https://login.microsoftonline.com"
const IMDSAPIVersion = "2018-02-01"
const MSIEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token"

var DefaultTokenExpiryWithinThreshold = time.Minute * 10

// UserOAuthTokenManager for token management.
type UserOAuthTokenManager struct {
	oauthClient *http.Client
	credCache   *CredCache
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
			Proxy: http.ProxyFromEnvironment,
			// We use Dial instead of DialContext as DialContext has been reported to cause slower performance.
			Dial /*Context*/ : (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).Dial, /*Context*/
			MaxIdleConns:           0, // No limit
			MaxIdleConnsPerHost:    1000,
			IdleConnTimeout:        180 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     false,
			MaxResponseHeaderBytes: 0,
			//ResponseHeaderTimeout:  time.Duration{},
			//ExpectContinueTimeout:  time.Duration{},
		},
	}
}

// GetTokenInfo gets token info, it follows rule:
// 1. If there is token passed from environment variable(note this is only for testing purpose),
//    use token passed from environment variable.
// 2. Otherwise, try to get token from cache.
// This method either successfully return token, or return error.
func (uotm *UserOAuthTokenManager) GetTokenInfo(ctx context.Context) (*OAuthTokenInfo, error) {
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
	oAuthTokenInfo.Token = *token

	if persist {
		err = uotm.credCache.SaveToken(*oAuthTokenInfo)
		if err != nil {
			return nil, err
		}
	}

	return oAuthTokenInfo, nil
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

	// Init OAuth config
	oauthConfig, err := adal.NewOAuthConfig(activeDirectoryEndpoint, tenantID)
	if err != nil {
		return nil, err
	}

	// Acquire the device code
	deviceCode, err := adal.InitiateDeviceAuth(
		uotm.oauthClient,
		*oauthConfig,
		ApplicationID,
		Resource)
	if err != nil {
		return nil, fmt.Errorf("failed to login with tenantID %q, Azure directory endpoint %q, %v",
			tenantID, activeDirectoryEndpoint, err)
	}

	// Display the authentication message
	fmt.Println(*deviceCode.Message)

	// Wait here until the user is authenticated
	// TODO: check if adal Go SDK has new method which supports context, currently ctrl-C can stop the login in console interactively.
	token, err := adal.WaitForUserCompletion(uotm.oauthClient, deviceCode)
	if err != nil {
		return nil, fmt.Errorf("failed to login with tenantID %q, Azure directory endpoint %q, %v",
			tenantID, activeDirectoryEndpoint, err)
	}

	oAuthTokenInfo := OAuthTokenInfo{
		Token:                   *token,
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
	}

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
// Fresh token is persisted if acces token or refresh token is changed.
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
	return uotm.credCache.HasCachedToken()
}

// RemoveCachedToken delete all the cached token.
func (uotm *UserOAuthTokenManager) RemoveCachedToken() error {
	return uotm.credCache.RemoveCachedToken()
}

//====================================================================================

// EnvVarOAuthTokenInfo passes oauth token info into AzCopy through environment variable.
// Note: this is only used for testing, and not encouraged to be used in production environments.
const EnvVarOAuthTokenInfo = "AZCOPY_OAUTH_TOKEN_INFO"

// ErrorCodeEnvVarOAuthTokenInfoNotSet defines error code when environment variable AZCOPY_OAUTH_TOKEN_INFO is not set.
const ErrorCodeEnvVarOAuthTokenInfoNotSet = "environment variable AZCOPY_OAUTH_TOKEN_INFO is not set"

// EnvVarOAuthTokenInfoExists verifies if environment variable for OAuthTokenInfo is specified.
// The method returns true if the environment variable is set.
// Note: This is useful for only checking whether the env var exists, please use getTokenInfoFromEnvVar
// directly in the case getting token info is necessary.
func EnvVarOAuthTokenInfoExists() bool {
	if os.Getenv(EnvVarOAuthTokenInfo) == "" {
		return false
	}
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
	rawToken := os.Getenv(EnvVarOAuthTokenInfo)
	if rawToken == "" {
		return nil, errors.New(ErrorCodeEnvVarOAuthTokenInfoNotSet)
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectly.
	os.Setenv(EnvVarOAuthTokenInfo, "")

	tokenInfo, err := jsonToTokenInfo([]byte(rawToken))
	if err != nil {
		return nil, fmt.Errorf("get token from environment variable failed to unmarshal token, %v", err)
	}

	if tokenInfo.TokenRefreshSource != TokenRefreshSourceTokenStore {
		refreshedToken, err := tokenInfo.Refresh(ctx)
		if err != nil {
			return nil, fmt.Errorf("get token from environment variable failed to ensure token fresh, %v", err)
		}
		tokenInfo.Token = *refreshedToken
	}

	return tokenInfo, nil
}

//====================================================================================

// TokenRefreshSourceTokenStore indicates enabling azcopy oauth integration through tokenstore.
// Note: This should be only used for internal integrations.
const TokenRefreshSourceTokenStore = "tokenstore"

// OAuthTokenInfo contains info necessary for refresh OAuth credentials.
type OAuthTokenInfo struct {
	adal.Token
	Tenant                  string `json:"_tenant"`
	ActiveDirectoryEndpoint string `json:"_ad_endpoint"`
	TokenRefreshSource      string `json:"_token_refresh_source"`
	Identity                bool   `json:"_identity"`
	IdentityInfo            IdentityInfo
	// Note: ClientID should be only used for internal integrations through env var with refresh token.
	// It indicates the Application ID assigned to your app when you registered it with Azure AD.
	// In this case AzCopy refresh token on behalf of caller.
	// For more details, please refer to
	// https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-protocols-oauth-code#refreshing-the-access-tokens
	ClientID string `json:"_client_id"`
}

// IdentityInfo contains info for MSI.
type IdentityInfo struct {
	ClientID string `json:"_identity_client_id"`
	ObjectID string `json:"_identity_object_id"`
	MSIResID string `json:"_identity_msi_res_id"`
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
func (credInfo *OAuthTokenInfo) Refresh(ctx context.Context) (*adal.Token, error) {
	if credInfo.TokenRefreshSource == TokenRefreshSourceTokenStore {
		return credInfo.GetNewTokenFromTokenStore(ctx)
	}

	if credInfo.Identity {
		return credInfo.GetNewTokenFromMSI(ctx)
	}

	return credInfo.RefreshTokenWithUserCredential(ctx)
}

var msiTokenHTTPClient = newAzcopyHTTPClient()

// Single instance token store credential cache shared by entire azcopy process.
var tokenStoreCredCache = NewCredCacheInternalIntegration(CredCacheOptions{
	KeyName:     "azcopy/aadtoken/" + strconv.Itoa(os.Getpid()),
	ServiceName: "azcopy",
	AccountName: "aadtoken/" + strconv.Itoa(os.Getpid()),
})

// GetNewTokenFromTokenStore gets token from token store. (Credential Manager in Windows, keyring in Linux and keychain in MacOS.)
// Note: This approach should only be used in internal integrations.
func (credInfo *OAuthTokenInfo) GetNewTokenFromTokenStore(ctx context.Context) (*adal.Token, error) {
	hasToken, err := tokenStoreCredCache.HasCachedToken()
	if err != nil || !hasToken {
		return nil, fmt.Errorf("no cached token found in Token Store Mode(SE), %v", err)
	}

	tokenInfo, err := tokenStoreCredCache.LoadToken()
	if err != nil {
		return nil, fmt.Errorf("get cached token failed in Token Store Mode(SE), %v", err)
	}

	return &(tokenInfo.Token), nil
}

// GetNewTokenFromMSI gets token from Azure Instance Metadata Service identity endpoint.
// For details, please refer to https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
func (credInfo *OAuthTokenInfo) GetNewTokenFromMSI(ctx context.Context) (*adal.Token, error) {
	// Prepare request to get token from Azure Instance Metadata Service identity endpoint.
	req, err := http.NewRequest("GET", MSIEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request, %v", err)
	}
	params := req.URL.Query()
	params.Set("resource", Resource)
	params.Set("api-version", IMDSAPIVersion)
	if credInfo.IdentityInfo.ClientID != "" {
		params.Set("client_id", credInfo.IdentityInfo.ClientID)
	}
	if credInfo.IdentityInfo.ObjectID != "" {
		params.Set("object_id", credInfo.IdentityInfo.ObjectID)
	}
	if credInfo.IdentityInfo.MSIResID != "" {
		params.Set("msi_res_id", credInfo.IdentityInfo.MSIResID)
	}
	req.URL.RawQuery = params.Encode()
	req.Header.Set("Metadata", "true")
	// Set context.
	req.WithContext(ctx)

	// Send request
	resp, err := msiTokenHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("please check whether MSI is enabled on this PC, to enable MSI please refer to https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/qs-configure-portal-windows-vm#enable-system-assigned-identity-on-an-existing-vm. (Error details: %v)", err)
	}
	defer func() { // resp and Body should not be nil
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	// Check if the status code indicates success
	// The request returns 200 currently, add 201 and 202 as well for possible extension.
	if !(HTTPResponseExtension{Response: resp}).IsSuccessStatusCode(http.StatusOK, http.StatusCreated, http.StatusAccepted) {
		return nil, fmt.Errorf("failed to get token from msi, status code: %v", resp.StatusCode)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	result := &adal.Token{}
	if len(b) > 0 {
		b = ByteSliceExtension{ByteSlice: b}.RemoveBOM()
		if err := json.Unmarshal(b, result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body, %v", err)
		}
	} else {
		return nil, errors.New("failed to get token from msi")
	}

	return result, nil
}

// RefreshTokenWithUserCredential gets new token with user credential through refresh.
func (credInfo *OAuthTokenInfo) RefreshTokenWithUserCredential(ctx context.Context) (*adal.Token, error) {
	oauthConfig, err := adal.NewOAuthConfig(credInfo.ActiveDirectoryEndpoint, credInfo.Tenant)
	if err != nil {
		return nil, err
	}

	// ClientID in credInfo is optional which is used for internal integration only.
	// Use AzCopy's 1st party applicationID for refresh by default.
	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		IffString(credInfo.ClientID != "", credInfo.ClientID, ApplicationID),
		Resource,
		credInfo.Token)
	if err != nil {
		return nil, err
	}

	if err := spt.RefreshWithContext(ctx); err != nil {
		return nil, err
	}

	newToken := spt.Token()
	return &newToken, nil
}

// IsEmpty returns if current OAuthTokenInfo is empty and doesn't contain any useful info.
func (credInfo OAuthTokenInfo) IsEmpty() bool {
	if credInfo.Tenant == "" && credInfo.ActiveDirectoryEndpoint == "" && credInfo.Token.IsZero() && !credInfo.Identity {
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

//====================================================================================

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
