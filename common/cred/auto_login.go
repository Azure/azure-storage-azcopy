package cred

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

func NewAutoLoginKeyring() Keyring {
	return NewMemKeyring(prepareAutoLogins())
}

func prepareAutoLogins() map[string]token {
	loginTypeString, ok := enum.EEnvironmentVariable.AutoLoginType().Lookup()
	if !ok {
		return map[string]token{}
	}

	loginType, _ := enum.EAutoLoginType.Parse(loginTypeString)

	autoToken := newLoginToken(LoginTokenOptions{
		TenantID:           enum.EEnvironmentVariable.TenantID().Get(),
		AADEndpoint:        enum.EEnvironmentVariable.AADEndpoint().Get(),
		LoginType:          loginType,
		IdentityClientID:   enum.EEnvironmentVariable.ManagedIdentityClientID().Get(),
		IdentityObjectID:   enum.EEnvironmentVariable.ManagedIdentityObjectID().Get(),
		IdentityResourceID: enum.EEnvironmentVariable.ManagedIdentityResourceString().Get(),
		ApplicationID:      enum.EEnvironmentVariable.ApplicationID().Get(),
		CertificateData:    enum.EEnvironmentVariable.CertificatePath().Get(),
		ClientSecret:       enum.EEnvironmentVariable.ClientSecret().Get(),
		Nickname:           DefaultNickname,
		SaveCredential:     false,
	})

	if loginType == enum.EAutoLoginType.Device() || loginType == enum.EAutoLoginType.Interactive() {
		if userLogin, ok := autoToken.tokenImpl.(*tokenInfoUserLogin); ok {
			cred, err := autoToken.tokenImpl.getTokenCredential(autoToken.TokenHeader)
			if err == nil {
				if authToken, ok := cred.(AuthenticateToken); ok {
					record, err := authToken.Authenticate(context.TODO(), &policy.TokenRequestOptions{
						EnableCAE: true,
						Scopes:    DefaultAuthenticateScopes,
					})
					if err == nil {
						userLogin.AuthRecord = &record
						autoToken.tokenImpl = userLogin
					}
				}
			}
		}
	}

	return map[string]token{
		DefaultNickname: autoToken,
	}
}
