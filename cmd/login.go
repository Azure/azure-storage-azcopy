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

var loginCmdArg = loginCmdArgs{tenantID: common.DefaultTenantID}

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
		loginCmdArg.certPass = common.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePassword())
		loginCmdArg.clientSecret = common.GetEnvironmentVariable(common.EEnvironmentVariable.ClientSecret())
		loginCmdArg.persistToken = true

		if loginCmdArg.certPass != "" || loginCmdArg.clientSecret != "" {
			glcm.Info(environmentVariableNotice)
		}

		loginCmdArg.loginType = strings.ToLower(loginCmdArg.loginType)

		err := loginCmdArg.process()
		if err != nil {
			// the errors from adal contains \r\n in the body, get rid of them to make the error easier to look at
			prettyErr := strings.Replace(err.Error(), `\r\n`, "\n", -1)
			prettyErr += "\n\nNOTE: If your credential was created in the last 5 minutes, please wait a few minutes and try again."
			glcm.Error("Failed to perform login command: \n" + prettyErr + getErrorCodeUrl(err))
		}
		return nil
	},
}

func init() {

	rootCmd.AddCommand(lgCmd)

	lgCmd.PersistentFlags().StringVar(&loginCmdArg.tenantID, "tenant-id", "", "The Azure Active Directory tenant ID to use for OAuth device interactive login.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.aadEndpoint, "aad-endpoint", "", "The Azure Active Directory endpoint to use. The default ("+common.DefaultActiveDirectoryEndpoint+") is correct for the public Azure cloud. Set this parameter when authenticating in a national cloud. Not needed for Managed Service Identity")

	lgCmd.PersistentFlags().BoolVar(&loginCmdArg.identity, "identity", false, "Deprecated. Please use --login-type=MSI. Log in using virtual machine's identity, also known as managed service identity (MSI).")
	lgCmd.PersistentFlags().BoolVar(&loginCmdArg.servicePrincipal, "service-principal", false, "Deprecated. Please use --login-type=SPN. Log in via Service Principal Name (SPN) by using a certificate or a secret. The client secret or certificate password must be placed in the appropriate environment variable. Type AzCopy env to see names and descriptions of environment variables.")
	// Deprecate these flags in favor of a new login type flag
	_ = lgCmd.PersistentFlags().MarkHidden("identity")
	_ = lgCmd.PersistentFlags().MarkHidden("service-principal")

	lgCmd.PersistentFlags().StringVar(&loginCmdArg.loginType, "login-type", common.EAutoLoginType.Device().String(), "Default value is "+common.EAutoLoginType.Device().String()+". Specify the credential type to access Azure Resource, available values are "+strings.Join(common.ValidAutoLoginTypes(), ", ")+".")

	// Managed Identity flags
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityClientID, "identity-client-id", "", "Client ID of user-assigned identity.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityResourceID, "identity-resource-id", "", "Resource ID of user-assigned identity.")
	// SPN flags
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.applicationID, "application-id", "", "Application ID of user-assigned identity. Required for service principal auth.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.certPath, "certificate-path", "", "Path to certificate for SPN authentication. Required for certificate-based service principal auth.")

	// Deprecate the identity-object-id flag
	_ = lgCmd.PersistentFlags().MarkHidden("identity-object-id") // Object ID of user-assigned identity.
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityObjectID, "identity-object-id", "", "Object ID of user-assigned identity. This parameter is deprecated. Please use client id or resource id")

}

type loginCmdArgs struct {
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
	certPass      string
	clientSecret  string
	persistToken  bool
}

func (lca loginCmdArgs) process() error {
	// Login type consolidation to allow backward compatibility.
	// Commenting the warning message till we decide on when to deprecate these options
	// if lca.servicePrincipal || lca.identity {
	// 	glcm.Warn("The flags --service-principal and --identity will be deprecated in a future release. Please use --login-type=SPN or --login-type=MSI instead.")
	// }
	if lca.servicePrincipal {
		lca.loginType = common.EAutoLoginType.SPN().String()
	} else if lca.identity {
		lca.loginType = common.EAutoLoginType.MSI().String()
	} else if lca.servicePrincipal && lca.identity {
		// This isn't necessary, but stands as a sanity check. It will never be hit.
		return errors.New("you can only log in with one type of auth at once")
	}
	// Any required variables for login type will be validated by the Azure Identity SDK.
	lca.loginType = strings.ToLower(lca.loginType)

	uotm := GetUserOAuthTokenManagerInstance()
	// Persist the token to cache, if login fulfilled successfully.

	switch lca.loginType {
	case common.EAutoLoginType.SPN().String():
		if lca.certPath != "" {
			if err := uotm.CertLogin(lca.tenantID, lca.aadEndpoint, lca.certPath, lca.certPass, lca.applicationID, lca.persistToken); err != nil {
				return err
			}
			glcm.Info("SPN Auth via cert succeeded.")
		} else {
			if err := uotm.SecretLogin(lca.tenantID, lca.aadEndpoint, lca.clientSecret, lca.applicationID, lca.persistToken); err != nil {
				return err
			}
			glcm.Info("SPN Auth via secret succeeded.")
		}
	case common.EAutoLoginType.MSI().String():
		if err := uotm.MSILogin(common.IdentityInfo{
			ClientID: lca.identityClientID,
			ObjectID: lca.identityObjectID,
			MSIResID: lca.identityResourceID,
		}, lca.persistToken); err != nil {
			return err
		}
		// For MSI login, info success message to user.
		glcm.Info("Login with identity succeeded.")
	case common.EAutoLoginType.AzCLI().String():
		if err := uotm.AzCliLogin(lca.tenantID, lca.persistToken); err != nil {
			return err
		}
		glcm.Info("Login with AzCliCreds succeeded")
	case common.EAutoLoginType.PsCred().String():
		if err := uotm.PSContextToken(lca.tenantID, lca.persistToken); err != nil {
			return err
		}
		glcm.Info("Login with Powershell context succeeded")
	case common.EAutoLoginType.Workload().String():
		if err := uotm.WorkloadIdentityLogin(lca.persistToken); err != nil {
			return err
		}
		glcm.Info("Login with Workload Identity succeeded")
	default:
		if err := uotm.UserLogin(lca.tenantID, lca.aadEndpoint, lca.persistToken); err != nil {
			return err
		}
		// User fulfills login in browser, and there would be message in browser indicating whether login fulfilled successfully.
		glcm.Info("Login succeeded.")
	}

	return nil
}
