package client

import "github.com/Azure/azure-storage-azcopy/v10/common"

type LoginOptions struct {
	TenantID                string
	ActiveDirectoryEndpoint string
	LoginType               common.AutoLoginType

	IdentityClientID   string
	IdentityResourceID string

	ApplicationID   string
	CertificatePath string
}

func (cc Client) Login(options LoginOptions) error {
	return nil
}

type LoginStatusOptions struct {
	TenantID                bool
	ActiveDirectoryEndpoint bool
}

func (cc Client) LoginStatus(options LoginStatusOptions) error {
	return nil
}

type LogoutOptions struct {
}

func (cc Client) Logout(options LogoutOptions) error {
	return nil
}
