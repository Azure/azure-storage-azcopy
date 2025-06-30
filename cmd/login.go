// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"errors"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

type LoginOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   common.AutoLoginType

	IdentityClientID   string
	IdentityResourceID string

	ApplicationID   string
	CertificatePath string

	certificatePassword string
	clientSecret        string
	persistToken        bool

	identityObjectID string
}

var loginCmdArg = rawLoginArgs{tenantID: common.DefaultTenantID}

var lgCmd = &cobra.Command{
	Use:        "login",
	SuggestFor: []string{"login"},
	Short:      loginCmdShortDescription,
	Long:       loginCmdLongDescription,
	Example:    loginCmdExample,
	Args: func(cmd *cobra.Command, args []string) error {
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Login type consolidation to allow backward compatibility.
		// Commenting the warning message till we decide on when to deprecate these options
		// if lca.servicePrincipal || lca.identity {
		// 	glcm.Warn("The flags --service-principal and --identity will be deprecated in a future release. Please use --login-type=SPN or --login-type=MSI instead.")
		// }
		if (loginCmdArg.servicePrincipal || loginCmdArg.identity) && cmd.Flag("login-type").Changed {
			return errors.New("you cannot set --service-principal or --identity in conjunction with --login-type. please use --login-type only")
		} else if loginCmdArg.servicePrincipal && loginCmdArg.identity {
			// This isn't necessary, but stands as a sanity check. It will never be hit.
			return errors.New("you can only login using one authentication method at a time")
		} else if loginCmdArg.servicePrincipal {
			loginCmdArg.loginType = common.EAutoLoginType.SPN().String()
		} else if loginCmdArg.identity {
			loginCmdArg.loginType = common.EAutoLoginType.MSI().String()
		}
		// Any required variables for login type will be validated by the Azure Identity SDK.
		loginCmdArg.loginType = strings.ToLower(loginCmdArg.loginType)

		options, err := loginCmdArg.toOptions()
		if err != nil {
			return err
		}
		err = RunLogin(options)
		if err != nil {
			// the errors from adal contains \r\n in the body, get rid of them to make the error easier to look at
			prettyErr := strings.Replace(err.Error(), `\r\n`, "\n", -1)
			prettyErr += "\n\nNOTE: If your credential was created in the last 5 minutes, please wait a few minutes and try again."
			glcm.Error("Failed to perform login command: \n" + prettyErr + getErrorCodeUrl(err))
		}
		return nil
	},
}

func RunLogin(args LoginOptions) error {
	args.certificatePassword = common.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePassword())
	args.clientSecret = common.GetEnvironmentVariable(common.EEnvironmentVariable.ClientSecret())
	args.persistToken = true

	if args.certificatePassword != "" || args.clientSecret != "" {
		glcm.Info(environmentVariableNotice)
	}

	return args.process()

}

func init() {

	rootCmd.AddCommand(lgCmd)

	lgCmd.PersistentFlags().StringVar(&loginCmdArg.tenantID, "tenant-id", "", "The Azure Active Directory tenant ID to use for OAuth device interactive login.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.aadEndpoint, "aad-endpoint", "", "The Azure Active Directory endpoint to use. The default ("+common.DefaultActiveDirectoryEndpoint+") is correct for the public Azure cloud. Set this parameter when authenticating in a national cloud. Not needed for Managed Service Identity")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.loginType, "login-type", common.EAutoLoginType.Device().String(), "Default value is "+common.EAutoLoginType.Device().String()+". Specify the credential type to access Azure Resource, available values are "+strings.Join(common.ValidAutoLoginTypes(), ", ")+".")

	// Managed Identity flags
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityClientID, "identity-client-id", "", "Client ID of user-assigned identity.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityResourceID, "identity-resource-id", "", "Resource ID of user-assigned identity.")
	// SPN flags
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.applicationID, "application-id", "", "Application ID of user-assigned identity. Required for service principal auth.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.certPath, "certificate-path", "", "Path to certificate for SPN authentication. Required for certificate-based service principal auth.")

	// Deprecate these flags in favor of a new login type flag
	lgCmd.PersistentFlags().BoolVar(&loginCmdArg.identity, "identity", false, "Deprecated. Please use --login-type=MSI. Log in using virtual machine's identity, also known as managed service identity (MSI).")
	_ = lgCmd.PersistentFlags().MarkHidden("identity")
	lgCmd.PersistentFlags().BoolVar(&loginCmdArg.servicePrincipal, "service-principal", false, "Deprecated. Please use --login-type=SPN. Log in via Service Principal Name (SPN) by using a certificate or a secret. The client secret or certificate password must be placed in the appropriate environment variable. Type AzCopy env to see names and descriptions of environment variables.")
	_ = lgCmd.PersistentFlags().MarkHidden("service-principal")

	// Deprecate the identity-object-id flag
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityObjectID, "identity-object-id", "", "Object ID of user-assigned identity. This parameter is deprecated. Please use client id or resource id")
	_ = lgCmd.PersistentFlags().MarkHidden("identity-object-id") // Object ID of user-assigned identity.

}

type rawLoginArgs struct {
	// OAuth login arguments
	tenantID    string
	aadEndpoint string

	identity         bool // Whether to use MSI.
	servicePrincipal bool

	loginType string

	// Info of VM's user assigned identity, client or object ids of the service identity are required if
	// your VM has multiple user-assigned managed identities.
	// https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-go
	identityClientID   string
	identityObjectID   string
	identityResourceID string

	//Required to sign in with a SPN (Service Principal Name)
	applicationID string
	certPath      string
}

func (args rawLoginArgs) toOptions() (LoginOptions, error) {
	var loginType common.AutoLoginType
	err := loginType.Parse(loginCmdArg.loginType)
	if err != nil {
		return LoginOptions{}, err
	}
	return LoginOptions{
		TenantID:           args.tenantID,
		AADEndpoint:        args.aadEndpoint,
		LoginType:          loginType,
		IdentityClientID:   args.identityClientID,
		identityObjectID:   args.identityObjectID,
		IdentityResourceID: args.identityResourceID,
		ApplicationID:      args.applicationID,
		CertificatePath:    args.certPath,
	}, nil
}

func (options LoginOptions) process() error {
	uotm := GetUserOAuthTokenManagerInstance()
	// Persist the token to cache, if login fulfilled successfully.

	switch options.LoginType {
	case common.EAutoLoginType.SPN():
		if options.CertificatePath != "" {
			if err := uotm.CertLogin(options.TenantID, options.AADEndpoint, options.CertificatePath, options.certificatePassword, options.ApplicationID, options.persistToken); err != nil {
				return err
			}
			glcm.Info("SPN Auth via cert succeeded.")
		} else {
			if err := uotm.SecretLogin(options.TenantID, options.AADEndpoint, options.clientSecret, options.ApplicationID, options.persistToken); err != nil {
				return err
			}
			glcm.Info("SPN Auth via secret succeeded.")
		}
	case common.EAutoLoginType.MSI():
		if err := uotm.MSILogin(common.IdentityInfo{
			ClientID: options.IdentityClientID,
			ObjectID: options.identityObjectID,
			MSIResID: options.IdentityResourceID,
		}, options.persistToken); err != nil {
			return err
		}
		// For MSI login, info success message to user.
		glcm.Info("Login with identity succeeded.")
	case common.EAutoLoginType.AzCLI():
		if err := uotm.AzCliLogin(options.TenantID, options.persistToken); err != nil {
			return err
		}
		glcm.Info("Login with AzCliCreds succeeded")
	case common.EAutoLoginType.PsCred():
		if err := uotm.PSContextToken(options.TenantID, options.persistToken); err != nil {
			return err
		}
		glcm.Info("Login with Powershell context succeeded")
	case common.EAutoLoginType.Workload():
		if err := uotm.WorkloadIdentityLogin(options.persistToken); err != nil {
			return err
		}
		glcm.Info("Login with Workload Identity succeeded")
	default:
		if err := uotm.UserLogin(options.TenantID, options.AADEndpoint, options.persistToken); err != nil {
			return err
		}
		// User fulfills login in browser, and there would be message in browser indicating whether login fulfilled successfully.
		glcm.Info("Login succeeded.")
	}

	return nil
}
