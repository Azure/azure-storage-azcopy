package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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
	err := cc.initialize(common.JobID{}, false)
	if err != nil {
		return err
	}
	return cmd.RunLogin(cmd.LoginOptions{
		TenantID:           options.TenantID,
		AADEndpoint:        options.ActiveDirectoryEndpoint,
		LoginType:          options.LoginType,
		IdentityClientID:   options.IdentityClientID,
		IdentityResourceID: options.IdentityResourceID,
		ApplicationID:      options.ApplicationID,
		CertificatePath:    options.CertificatePath,
	})
}

type LoginStatusOptions struct {
}
type LoginStatus cmd.LoginStatus

func (cc Client) LoginStatus(_ LoginStatusOptions) (LoginStatus, error) {
	err := cc.initialize(common.JobID{}, false)
	if err != nil {
		return LoginStatus{Valid: false}, err
	}

	ls, err := cmd.RunLoginStatus(cmd.LoginStatusOptions{})

	return LoginStatus(ls), err
}

type LogoutOptions struct {
}

func (cc Client) Logout(_ LogoutOptions) error {
	err := cc.initialize(common.JobID{}, false)
	if err != nil {
		return err
	}
	return cmd.RunLogout(cmd.LogoutOptions{})
}
