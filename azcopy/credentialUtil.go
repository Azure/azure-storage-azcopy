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
