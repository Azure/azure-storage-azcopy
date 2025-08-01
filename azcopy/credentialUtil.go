// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"sync"
)

const (
	oauthLoginSessionCacheKeyName     = "AzCopyOAuthTokenCache"
	oauthLoginSessionCacheServiceName = "AzCopyV10"
	oauthLoginSessionCacheAccountName = "AzCopyOAuthTokenCache"
)

// TODO: (gapra) Should we make currentUserOAuthTokenManager a variable in the client?

// only one UserOAuthTokenManager should exist in azcopy process for current user.
// (a given AzcopyJobPlanFolder is mapped to current user)
var oauthTokenManagerOnce sync.Once
var currentUserOAuthTokenManager *common.UserOAuthTokenManager

// GetUserOAuthTokenManagerInstance gets or creates OAuthTokenManager for current user.
// Note: Currently, only support to have TokenManager for one user mapping to one tenantID.
func GetUserOAuthTokenManagerInstance() *common.UserOAuthTokenManager {
	oauthTokenManagerOnce.Do(func() {
		if common.AzcopyJobPlanFolder == "" {
			panic("invalid state, AzcopyJobPlanFolder should not be an empty string")
		}
		cacheName := common.GetEnvironmentVariable(common.EEnvironmentVariable.LoginCacheName())

		currentUserOAuthTokenManager = common.NewUserOAuthTokenManagerInstance(common.CredCacheOptions{
			DPAPIFilePath: common.AzcopyJobPlanFolder,
			KeyName:       common.Iff(cacheName != "", cacheName, oauthLoginSessionCacheKeyName),
			ServiceName:   oauthLoginSessionCacheServiceName,
			AccountName:   common.Iff(cacheName != "", cacheName, oauthLoginSessionCacheAccountName),
		})
	})

	return currentUserOAuthTokenManager
}
