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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Azure/go-autorest/autorest/adal"
)

// ApplicationID represents 1st party ApplicationID for AzCopy.
//const ApplicationID = "a45c21f4-7066-40b4-97d8-14f4313c3caa" // 3rd party test ApplicationID for AzCopy.
const ApplicationID = "579a7132-0e58-4d80-b1e1-7a1e2d337859"

// Resource used in azure storage OAuth authentication
const Resource = "https://storage.azure.com"
const DefaultTenantID = "microsoft.com"
const DefaultActiveDirectoryEndpoint = "https://login.microsoftonline.com"

var DefaultTokenExpiryWithinThreshold = time.Minute * 10

// UserOAuthTokenManager for token management.
type UserOAuthTokenManager struct {
	oauthClient *http.Client
	credCache   *CredCache
}

// NewUserOAuthTokenManagerInstance creates a token manager instance.
func NewUserOAuthTokenManagerInstance(userTokenCachePath string) *UserOAuthTokenManager {
	return &UserOAuthTokenManager{
		oauthClient: &http.Client{},
		credCache:   NewCredCache(userTokenCachePath),
	}
}

// GetTokenInfo gets token info, it follows rule:
// 1. If there is token passed from environment variable(note this is only for testing purpose),
//    use token passed from environment variable.
// 2. Otherwise, try to get token from cache.
// This method either successfully return token, or return error.
func (uotm *UserOAuthTokenManager) GetTokenInfo() (*OAuthTokenInfo, error) {
	var tokenInfo *OAuthTokenInfo
	var err error
	if tokenInfo, err = uotm.GetTokenInfoFromEnvVar(); err == nil || !IsErrorEnvVarOAuthTokenInfoNotSet(err) {
		// Scenario-Test: unattended testing with oauthTokenInfo set through environment variable
		// Note: Whenever environment variable is set in the context, it will overwrite the cached token info.
		if err != nil { // this is the case when env var exists while get token info failed
			return nil, err
		}
	} else { // Scenario: session mode which get token from cache
		if tokenInfo, err = uotm.GetCachedTokenInfo(); err != nil {
			return nil, err
		}
	}

	if tokenInfo == nil || tokenInfo.IsEmpty() {
		return nil, errors.New("invalid state, cannot get valid token info")
	}

	return tokenInfo, nil
}

// LoginWithDefaultADEndpoint interactively logins in with specified tenantID, persist indicates whether to
// cache the token on local disk.
func (uotm *UserOAuthTokenManager) LoginWithDefaultADEndpoint(tenantID string, persist bool) (*OAuthTokenInfo, error) {
	return uotm.LoginWithADEndpoint(tenantID, DefaultActiveDirectoryEndpoint, persist)
}

// LoginWithADEndpoint interactively logins in with specified tenantID and activeDirectoryEndpoint, persist indicates whether to
// cache the token on local disk.
func (uotm *UserOAuthTokenManager) LoginWithADEndpoint(tenantID, activeDirectoryEndpoint string, persist bool) (*OAuthTokenInfo, error) {
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
		return nil, fmt.Errorf("Failed to login with tenantID %q, Azure directory endpoint %q, due to error: %v",
			tenantID, activeDirectoryEndpoint, err)
	}

	// Display the authentication message
	fmt.Println(*deviceCode.Message)

	// Wait here until the user is authenticated
	// TODO: check if this can complete
	token, err := adal.WaitForUserCompletion(uotm.oauthClient, deviceCode)
	if err != nil {
		return nil, fmt.Errorf("Failed to login with tenantID %q, Azure directory endpoint %q, due to error: %v",
			tenantID, activeDirectoryEndpoint, err)
	}

	oAuthTokenInfo := OAuthTokenInfo{
		Token:                   *token,
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
	}

	if persist {
		// TODO: consider to retry the save token process for multi-instance case.
		err = uotm.credCache.SaveToken(oAuthTokenInfo)
		if err != nil {
			return nil, fmt.Errorf("Failed to login during persisting token to local, due to error: %v", err)
		}
	}

	return &oAuthTokenInfo, nil
}

// GetCachedTokenInfo get a fresh token from local disk cache.
// If access token is expired, it will refresh the token.
// If refresh token is expired, the method will fail and return failure reason.
// Fresh token is persisted if acces token or refresh token is changed.
func (uotm *UserOAuthTokenManager) GetCachedTokenInfo() (*OAuthTokenInfo, error) {
	hasToken, err := uotm.credCache.HasCachedToken()
	if err != nil {
		return nil, fmt.Errorf("No cached token found, due to error: %s", err)
	}
	if !hasToken {
		return nil, errors.New("No cached token found, please use login command first before getToken")
	}

	tokenInfo, err := uotm.credCache.LoadToken()
	if err != nil {
		return nil, fmt.Errorf("Get cached token failed due to error: %v", err.Error())
	}

	oauthConfig, err := adal.NewOAuthConfig(tokenInfo.ActiveDirectoryEndpoint, tokenInfo.Tenant)
	if err != nil {
		return nil, err
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		ApplicationID,
		Resource,
		tokenInfo.Token)
	if err != nil {
		return nil, fmt.Errorf("Get cached token failed due to error: %v", err.Error())
	}

	// Ensure at least 10 minutes fresh time.
	spt.SetRefreshWithin(DefaultTokenExpiryWithinThreshold)
	spt.SetAutoRefresh(true)
	err = spt.EnsureFresh() // EnsureFresh only refresh token when access token's fresh duration is less than threshold set in RefreshWithin.
	if err != nil {
		return nil, fmt.Errorf("Get cached token failed to ensure token fresh due to error: %v", err.Error())
	}

	freshToken := spt.Token()

	// Update token cache, if token is updated.
	if freshToken.AccessToken != tokenInfo.AccessToken || freshToken.RefreshToken != tokenInfo.RefreshToken {
		tokenInfoToPersist := OAuthTokenInfo{
			Token:                   freshToken,
			Tenant:                  tokenInfo.Tenant,
			ActiveDirectoryEndpoint: tokenInfo.ActiveDirectoryEndpoint,
		}
		if err := uotm.credCache.SaveToken(tokenInfoToPersist); err != nil {
			return nil, err
		}
		return &tokenInfoToPersist, nil
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

// EnvVarOAuthTokenInfo passes oauth token info into AzCopy through environment variable.
// Note: this is only used for testing, and not encouraged to be used in production environments.
const EnvVarOAuthTokenInfo = "AZCOPY_OAUTH_TOKEN_INFO"

// ErrorCodeEnvVarOAuthTokenInfoNotSet defines error code when environment variable AZCOPY_OAUTH_TOKEN_INFO is not set.
const ErrorCodeEnvVarOAuthTokenInfoNotSet = "environment variable AZCOPY_OAUTH_TOKEN_INFO is not set"

// EnvVarOAuthTokenInfoExists verifies if environment variable for OAuthTokenInfo is specified.
// The method returns true if the environment variable is set.
// Note: This is useful for only checking whether the env var exists, please use GetTokenInfoFromEnvVar
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

// GetTokenInfoFromEnvVar gets token info from environment variable.
func (uotm *UserOAuthTokenManager) GetTokenInfoFromEnvVar() (*OAuthTokenInfo, error) {
	rawToken := os.Getenv(EnvVarOAuthTokenInfo)
	if rawToken == "" {
		return nil, errors.New(ErrorCodeEnvVarOAuthTokenInfoNotSet)
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectly.
	os.Setenv(EnvVarOAuthTokenInfo, "")

	tokenInfo, err := JSONToTokenInfo([]byte(rawToken))
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token, %v", err)
	}

	oauthConfig, err := adal.NewOAuthConfig(tokenInfo.ActiveDirectoryEndpoint, tokenInfo.Tenant)
	if err != nil {
		return nil, err
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		ApplicationID,
		Resource,
		tokenInfo.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to get token from env var, %v", err)
	}

	// Ensure at least 10 minutes fresh time.
	spt.SetRefreshWithin(DefaultTokenExpiryWithinThreshold)
	spt.SetAutoRefresh(true)
	err = spt.EnsureFresh() // EnsureFresh only refresh token when access token's fresh duration is less than threshold set in RefreshWithin.
	if err != nil {
		return nil, fmt.Errorf("failed to ensure token fresh during get token from env var, %v", err)
	}

	return &OAuthTokenInfo{
		Token:                   spt.Token(),
		Tenant:                  tokenInfo.Tenant,
		ActiveDirectoryEndpoint: tokenInfo.ActiveDirectoryEndpoint,
	}, nil
}

//====================================================================================

// OAuthTokenInfo contains info necessary for refresh OAuth credentials.
type OAuthTokenInfo struct {
	adal.Token
	Tenant                  string `json:"_tenant"`
	ActiveDirectoryEndpoint string `json:"_ad_endpoint"`
}

// IsEmpty returns if current OAuthTokenInfo is empty and doesn't contain any useful info.
func (credInfo OAuthTokenInfo) IsEmpty() bool {
	if credInfo.Tenant == "" && credInfo.ActiveDirectoryEndpoint == "" && credInfo.Token.IsZero() {
		return true
	}

	return false
}

// ToJSON converts OAuthTokenInfo to json format.
func (credInfo OAuthTokenInfo) ToJSON() ([]byte, error) {
	return json.Marshal(credInfo)
}

// JSONToTokenInfo converts bytes to OAuthTokenInfo
func JSONToTokenInfo(b []byte) (*OAuthTokenInfo, error) {
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
