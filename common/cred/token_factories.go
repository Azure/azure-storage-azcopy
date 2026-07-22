package cred

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

type NewSPNTokenOptions struct {
	TenantID    string
	AADEndpoint string

	ApplicationID   string
	CertificateData string
	ClientSecret    string
}

func (o NewSPNTokenOptions) LoginType() enum.AutoLoginType { return enum.EAutoLoginType.SPN() }

func (o NewSPNTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType(),
		},
		tokenImpl: tokenInfoSPN{
			ApplicationID: o.ApplicationID,
			Cert:          o.CertificateData,
			Secret:        o.ClientSecret,
		},
	}
}

type NewMSITokenOptions struct {
	TenantID    string
	AADEndpoint string

	IdentityClientID   string
	IdentityObjectID   string
	IdentityResourceID string
}

func (o NewMSITokenOptions) LoginType() enum.AutoLoginType { return enum.EAutoLoginType.MSI() }

func (o NewMSITokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType(),
		},
		tokenImpl: tokenInfoManagedIdentity{
			ClientID: o.IdentityClientID,
			ObjectID: o.IdentityObjectID,
			MSIResID: o.IdentityResourceID,
		},
	}
}

type NewUserLoginTokenOptions struct {
	TenantID    string
	AADEndpoint string

	ApplicationID   string
	InteractionType enum.InteractiveLoginType
}

func (o NewUserLoginTokenOptions) LoginType() enum.AutoLoginType {
	if o.InteractionType == enum.EInteractiveLoginType.Device() {
		return enum.EAutoLoginType.Device()
	}
	return enum.EAutoLoginType.Interactive()
}

func (o NewUserLoginTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType(),
		},
		tokenImpl: &tokenInfoUserLogin{
			ApplicationID:   o.ApplicationID,
			InteractionType: o.InteractionType,
			AuthRecord:      &azidentity.AuthenticationRecord{},
		},
	}
}

type NewAzureCLITokenOptions struct {
	TenantID    string
	AADEndpoint string
}

func (o NewAzureCLITokenOptions) LoginType() enum.AutoLoginType { return enum.EAutoLoginType.AzCLI() }

func (o NewAzureCLITokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType(),
		},
		tokenImpl: tokenInfoCLI{},
	}
}

type NewPSCredTokenOptions struct {
	TenantID    string
	AADEndpoint string
}

func (o NewPSCredTokenOptions) LoginType() enum.AutoLoginType { return enum.EAutoLoginType.PsCred() }

func (o NewPSCredTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType(),
		},
		tokenImpl: tokenInfoPSCred{},
	}
}

type NewWorkloadTokenOptions struct {
	TenantID    string
	AADEndpoint string
}

func (o NewWorkloadTokenOptions) LoginType() enum.AutoLoginType { return enum.EAutoLoginType.Workload() }

func (o NewWorkloadTokenOptions) NewToken() Token {
	return &token{
		TokenHeader: TokenHeader{
			Tenant:                  ternary.Iff(o.TenantID != "", o.TenantID, DefaultTenantID),
			ActiveDirectoryEndpoint: ternary.Iff(o.AADEndpoint != "", o.AADEndpoint, DefaultActiveDirectoryEndpoint),
			LoginType:               o.LoginType(),
		},
		tokenImpl: tokenInfoWorkload{},
	}
}
