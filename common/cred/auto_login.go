package cred

import (
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

func NewAutoLoginKeyring() Keyring {
	return NewMemKeyring(prepareAutoLogins())
}

func prepareAutoLogins() map[string]Token {
	loginTypeString, ok := enum.EEnvironmentVariable.AutoLoginType().Lookup()
	if !ok {
		return map[string]Token{}
	}

	loginType, _ := enum.EAutoLoginType.Parse(loginTypeString)

	autoToken := NewToken(NewTokenOptions{
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

	// We no longer need to concern ourselves with "readying" our user login; as that will happen the first time get token is called

	return map[string]Token{
		DefaultNickname: autoToken,
	}
}
