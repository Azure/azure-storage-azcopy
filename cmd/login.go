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

// loginWithSPNArgs is a type that holds the arguments for login with SPN so we don't need to use environment variables. It also uses certData rather than certPath
type SPNArgs struct {
	certData      string
	applicationId string
	certPass      string
	tenantId      string
	aadEndpoint   string
}

var loginWithSPNArgs SPNArgs = SPNArgs{}

func setSPNArgs(certData string, certPass string, applicationId string, tenantId string, aadEndpoint string) {
	loginWithSPNArgs.certData = certData
	loginWithSPNArgs.applicationId = applicationId
	loginWithSPNArgs.certPass = certPass
	loginWithSPNArgs.tenantId = tenantId
	loginWithSPNArgs.aadEndpoint = aadEndpoint
}

// certData consts
const (
	certDataArg      = "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"
	applicationIdArg = "926b0989-653d-4904-8d52-971ccae528e5"
	tenantIdArg      = "72f988bf-86f1-41af-91ab-2d7cd011db47"
	aadEndpointArg   = "https://login.microsoftonline.com"
)

func (lca loginCmdArgs) process() error {
	// Login type consolidation to allow backward compatibility.
	// Commenting the warning message till we decide on when to deprecate these options
	// if lca.servicePrincipal || lca.identity {
	// 	glcm.Warn("The flags --service-principal and --identity will be deprecated in a future release. Please use --login-type=SPN or --login-type=MSI instead.")
	// }
	setSPNArgs(certDataArg, "", applicationIdArg, tenantIdArg, aadEndpointArg)
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
		} else if lca.certPath == "" && loginWithSPNArgs.certData != "" {
			if err := uotm.SpnArgsLogin(loginWithSPNArgs.tenantId, loginWithSPNArgs.aadEndpoint, loginWithSPNArgs.certData, loginWithSPNArgs.certPass, loginWithSPNArgs.applicationId, lca.persistToken); err != nil {
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
		if err := uotm.AzCliLogin(lca.tenantID); err != nil {
			return err
		}
		glcm.Info("Login with AzCliCreds succeeded")
	case common.EAutoLoginType.PsCred().String():
		if err := uotm.PSContextToken(lca.tenantID); err != nil {
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
