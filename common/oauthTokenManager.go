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
	"time"

	"github.com/Azure/go-autorest/autorest/adal"
)

// ApplicationID represents 3rd party ApplicationID for AzCopy.
const ApplicationID = "a45c21f4-7066-40b4-97d8-14f4313c3caa"

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
		return nil, fmt.Errorf("Failed to login due to error: %s", err.Error())
	}

	// Display the authentication message
	fmt.Println(*deviceCode.Message)

	// Wait here until the user is authenticated
	token, err := adal.WaitForUserCompletion(uotm.oauthClient, deviceCode)
	if err != nil {
		return nil, fmt.Errorf("Failed to login due to error: %s", err.Error())
	}

	oAuthTokenInfo := OAuthTokenInfo{
		Token:                   *token,
		Tenant:                  tenantID,
		ActiveDirectoryEndpoint: activeDirectoryEndpoint,
	}

	if persist {
		err = uotm.credCache.SaveToken(oAuthTokenInfo)
		if err != nil {
			return nil, fmt.Errorf("Failed to login during persisting token to local, due to error: %s", err.Error())
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
		return nil, fmt.Errorf("Get cached token failed to due to error: %v", err.Error())
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
