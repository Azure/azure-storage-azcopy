package azcopyclient

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type ClientOptions cmd.RootOptions

type Client struct {
	ClientOptions
}

// Initialize sets up AzCopy logger, ste and performs the version check
// TODO: this will be made internal and called by the respective commands
func (cc Client) Initialize(resumeJobID common.JobID, isBench bool) error {
	cmd.SetRootOptions(cmd.RootOptions(cc.ClientOptions))
	return cmd.Initialize(resumeJobID, isBench)
}

type LoginOptions struct {
	TenantId    string
	AadEndpoint string
	LoginType   common.AutoLoginType

	IdentityClientId   string
	IdentityResourceId string

	ApplicationId   string
	CertificatePath string
}

func setLoginArgs(options LoginOptions) cmd.LoginCmdArgs {
	return cmd.LoginCmdArgs{
		TenantID:           options.TenantId,
		AadEndpoint:        options.AadEndpoint,
		LoginType:          options.LoginType.String(),
		IdentityClientID:   options.IdentityClientId,
		IdentityResourceID: options.IdentityResourceId,
		ApplicationID:      options.ApplicationId,
		CertificatePath:    options.CertificatePath,
	}
}

func (cc Client) Login(options LoginOptions) error {
	err := cc.Initialize(common.JobID{}, false)
	if err != nil {
		return err
	}
	cmd.RunLogin(setLoginArgs(options))
	return nil
}
