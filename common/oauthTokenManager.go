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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/Azure/go-autorest/autorest/adal"
)

// ApplicationID for azcopy-v2
// TODO: azcopy-v2 need register a new first party application, currently use powershell's application ID for testing
const ApplicationID = "1950a258-227b-4e31-a9cf-717495945fc2"

// Resource for azure storage
const Resource = "https://storage.azure.com"
const DefaultTenantID = "microsoft.com"
const DefaultActiveDirectoryEndpoint = "https://login.microsoftonline.com"

var DefaultTokenExpiryWithinThreshold = time.Minute * 10

const defaultTokenFileName = "AccessToken.json"

// UserOAuthTokenManager for token manager
// TODO: add testing for non-microsoft.com tenantID
type UserOAuthTokenManager struct {
	oauthClient        *http.Client
	userTokenCachePath string
}

// NewUserOAuthTokenManagerInstance creates a token manager instance.
// TODO: userTokenCachePath can be optimized to cache manager
func NewUserOAuthTokenManagerInstance(userTokenCachePath string) *UserOAuthTokenManager {
	return &UserOAuthTokenManager{
		oauthClient:        &http.Client{},
		userTokenCachePath: userTokenCachePath,
	}
}

// LoginWithDefaultADEndpoint interactively logins in with specified tenantID.
func (uotm *UserOAuthTokenManager) LoginWithDefaultADEndpoint(tenantID string, persist bool) (*OAuthTokenInfo, error) {
	return uotm.LoginWithADEndpoint(tenantID, DefaultActiveDirectoryEndpoint, persist)
}

// LoginWithADEndpoint interactively logins in with specified tenantID and activeDirectoryEndpoint.
func (uotm *UserOAuthTokenManager) LoginWithADEndpoint(tenantID, activeDirectoryEndpoint string, persist bool) (*OAuthTokenInfo, error) {
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
		err = uotm.saveTokenInfo(oAuthTokenInfo)
		if err != nil {
			return nil, fmt.Errorf("Failed to login during persisting token to local, due to error: %s", err.Error())
		}
	}

	return &oAuthTokenInfo, nil
}

// GetCachedTokenInfo get a fresh token from cached token, if access token expired, it will refresh the token.
// if refresh token expired, the method will fail and ask user for new login action.
// Fresh token is persisted if acces token or refresh token is changed.
func (uotm *UserOAuthTokenManager) GetCachedTokenInfo() (*OAuthTokenInfo, error) {
	if !uotm.HasCachedToken() {
		return nil, fmt.Errorf("getToken failed: No cached token found, please use login command first before getToken")
	}

	tokenInfo, err := uotm.loadTokenInfo()
	if err != nil {
		return nil, fmt.Errorf("getToken failed due to error: %v", err.Error())
	}
	activeDirectoryEndpoint := tokenInfo.ActiveDirectoryEndpoint
	tenantID := tokenInfo.Tenant

	oauthConfig, err := adal.NewOAuthConfig(activeDirectoryEndpoint, tenantID)
	if err != nil {
		return nil, err
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		ApplicationID,
		Resource,
		tokenInfo.Token)
	if err != nil {
		return nil, fmt.Errorf("getToken failed to get token due to error: %v", err)
	}

	// Ensure at least 10 minutes fresh time.
	spt.SetRefreshWithin(DefaultTokenExpiryWithinThreshold)
	spt.SetAutoRefresh(true)
	err = spt.EnsureFresh() // EnsureFresh only refresh token when access token's fresh duration is less than threshold set in RefreshWithin.
	if err != nil {
		return nil, fmt.Errorf("getToken failed to ensure token fresh due to error: %v", err.Error())
	}

	freshToken := spt.Token()

	// Update token cache, if token is updated.
	if freshToken.AccessToken != tokenInfo.AccessToken || freshToken.RefreshToken != tokenInfo.RefreshToken {
		tokenInfoToPersist := OAuthTokenInfo{
			Token:                   freshToken,
			Tenant:                  tenantID,
			ActiveDirectoryEndpoint: activeDirectoryEndpoint,
		}
		if err := uotm.saveTokenInfo(tokenInfoToPersist); err != nil {
			return nil, err
		}
		return &tokenInfoToPersist, nil
	}

	return tokenInfo, nil
}

// HasCachedToken returns if there is cached token in token manager.
func (uotm *UserOAuthTokenManager) HasCachedToken() bool {
	if _, err := os.Stat(uotm.tokenFilePath()); err == nil {
		return true
	}
	return false
}

// RemoveCachedToken delete all the cached token.
func (uotm *UserOAuthTokenManager) RemoveCachedToken() error {
	tokenFilePath := uotm.tokenFilePath()

	if _, err := os.Stat(tokenFilePath); err == nil {
		// Cached token file existed
		err = os.Remove(tokenFilePath)
		if err != nil { // remove failed
			return fmt.Errorf("failed to remove cached token file with path: %s, due to error: %v", tokenFilePath, err)
		}

		// remove succeeded
	} else {
		if !os.IsNotExist(err) { // Failed to stat cached token file
			return fmt.Errorf("fail to stat cached token file with path: %s, due to error: %v", tokenFilePath, err)
		}

		//token doesn't exist
		fmt.Println("no cached token found for current user.")
	}

	return nil
}

func (uotm *UserOAuthTokenManager) tokenFilePath() string {
	return path.Join(uotm.userTokenCachePath, "/", defaultTokenFileName)
}

func (uotm *UserOAuthTokenManager) loadTokenInfo() (*OAuthTokenInfo, error) {
	token, err := uotm.loadTokenInfoInternal(uotm.tokenFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to load token from cache: %v", err)
	}

	return token, nil
}

// LoadToken restores a Token object from a file located at 'path'.
func (uotm *UserOAuthTokenManager) loadTokenInfoInternal(path string) (*OAuthTokenInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file (%s) while loading token: %v", path, err)
	}
	defer file.Close()

	var token OAuthTokenInfo

	dec := json.NewDecoder(file)
	if err = dec.Decode(&token); err != nil {
		return nil, fmt.Errorf("failed to decode contents of file (%s) into Token representation: %v", path, err)
	}
	return &token, nil
}

func (uotm *UserOAuthTokenManager) saveTokenInfo(token OAuthTokenInfo) error {
	err := uotm.saveTokenInfoInternal(uotm.tokenFilePath(), 0600, token) // Save token with read/write permissions for the owner of the file.
	if err != nil {
		return fmt.Errorf("failed to save token to cache: %v", err)
	}
	return nil
}

// saveTokenInternal persists an oauth token at the given location on disk.
// It moves the new file into place so it can safely be used to replace an existing file
// that maybe accessed by multiple processes.
// get from adal and optimzied to involve more token info.
func (uotm *UserOAuthTokenManager) saveTokenInfoInternal(path string, mode os.FileMode, token OAuthTokenInfo) error {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory (%s) to store token in: %v", dir, err)
	}

	newFile, err := ioutil.TempFile(dir, "token")
	if err != nil {
		return fmt.Errorf("failed to create the temp file to write the token: %v", err)
	}
	tempPath := newFile.Name()

	if err := json.NewEncoder(newFile).Encode(token); err != nil {
		return fmt.Errorf("failed to encode token to file (%s) while saving token: %v", tempPath, err)
	}
	if err := newFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %s: %v", tempPath, err)
	}

	// Atomic replace to avoid multi-writer file corruptions
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("failed to move temporary token to desired output location. src=%s dst=%s: %v", tempPath, path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("failed to chmod the token file %s: %v", path, err)
	}
	return nil
}

// func (uotm *UserOAuthTokenManager) encrypt(token adal.Token) (string, error) {
// 	panic("not implemented")
// }
// func (uotm *UserOAuthTokenManager) decrypt(string) (adal.Token, error) {
// 	panic("not implemented")
// }

// ====================================================================================

// OAuthTokenInfo contains info necessary for azcopy to get/refresh OAuth credentials.
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
