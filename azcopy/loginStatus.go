package azcopy

import (
	"context"
	"errors"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type GetLoginStatusOptions struct {
}

type LoginStatus struct {
	Valid       bool
	TenantID    string
	AADEndpoint string
	LoginType   common.AutoLoginType
}

func (c Client) GetLoginStatus(_ GetLoginStatusOptions) (LoginStatus, error) {
	uotm := GetUserOAuthTokenManagerInstance()

	// Get current token info and refresh it with GetTokenInfo()
	ctx := context.Background()
	tokenInfo, err := uotm.GetTokenInfo(ctx)
	if err != nil || tokenInfo.IsExpired() {
		return LoginStatus{Valid: false}, errors.New("you are currently not logged in. please login using 'azcopy login'")
	}
	return LoginStatus{
		Valid:       true,
		TenantID:    tokenInfo.Tenant,
		AADEndpoint: tokenInfo.ActiveDirectoryEndpoint,
		LoginType:   tokenInfo.LoginType,
	}, nil
}
