package cred

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

type SPNTokenOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   enum.AutoLoginType

	ApplicationID   string
	CertificateData string
	ClientSecret    string
}

func (o SPNTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType,
		},
		tokenImpl: tokenInfoSPN{
			ApplicationID: o.ApplicationID,
			Cert:          o.CertificateData,
			Secret:        o.ClientSecret,
		},
	}
}

type MSITokenOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   enum.AutoLoginType

	IdentityClientID   string
	IdentityObjectID   string
	IdentityResourceID string
}

func (o MSITokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType,
		},
		tokenImpl: tokenInfoManagedIdentity{
			ClientID: o.IdentityClientID,
			ObjectID: o.IdentityObjectID,
			MSIResID: o.IdentityResourceID,
		},
	}
}

type UserLoginTokenOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   enum.AutoLoginType

	ApplicationID   string
	InteractionType enum.InteractiveLoginType
}

func (o UserLoginTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType,
		},
		tokenImpl: &tokenInfoUserLogin{
			ApplicationID:   o.ApplicationID,
			InteractionType: o.InteractionType,
			AuthRecord:      &azidentity.AuthenticationRecord{},
		},
	}
}

type AzureCLITokenOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   enum.AutoLoginType
}

func (o AzureCLITokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType,
		},
		tokenImpl: tokenInfoCLI{},
	}
}

type PSCredTokenOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   enum.AutoLoginType
}

func (o PSCredTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType,
		},
		tokenImpl: tokenInfoPSCred{},
	}
}

type WorkloadTokenOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   enum.AutoLoginType
}

func (o WorkloadTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType,
		},
		tokenImpl: tokenInfoWorkload{},
	}
}
