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
		loginCmdArg.certPass = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePassword())
		loginCmdArg.clientSecret = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ClientSecret())
		loginCmdArg.persistToken = true

		if loginCmdArg.certPass != "" || loginCmdArg.clientSecret != "" {
			glcm.Info(environmentVariableNotice)
		}

		err := loginCmdArg.process()
		if err != nil {
			// the errors from adal contains \r\n in the body, get rid of them to make the error easier to look at
			prettyErr := strings.Replace(err.Error(), `\r\n`, "\n", -1)
			prettyErr += "\n\nNOTE: If your credential was created in the last 5 minutes, please wait a few minutes and try again."
			glcm.Error("Failed to perform login command: \n" + prettyErr)
		}
		return nil
	},
}

func init() {

	rootCmd.AddCommand(lgCmd)

	lgCmd.PersistentFlags().StringVar(&loginCmdArg.tenantID, "tenant-id", "", "The Azure Active Directory tenant ID to use for OAuth device interactive login.")
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.aadEndpoint, "aad-endpoint", "", "The Azure Active Directory endpoint to use. The default ("+common.DefaultActiveDirectoryEndpoint+") is correct for the public Azure cloud. Set this parameter when authenticating in a national cloud. Not needed for Managed Service Identity")
	// Use identity which aligns to Azure powershell and CLI.
	lgCmd.PersistentFlags().BoolVar(&loginCmdArg.identity, "identity", false, "Log in using virtual machine's identity, also known as managed service identity (MSI).")
	// Use SPN certificate to log in.
	lgCmd.PersistentFlags().BoolVar(&loginCmdArg.servicePrincipal, "service-principal", false, "Log in via Service Principal Name (SPN) by using a certificate or a secret. The client secret or certificate password must be placed in the appropriate environment variable. Type AzCopy env to see names and descriptions of environment variables.")
	// Client ID of user-assigned identity.
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityClientID, "identity-client-id", "", "Client ID of user-assigned identity.")
	// Resource ID of user-assigned identity.
	lgCmd.PersistentFlags().StringVar(&loginCmdArg.identityResourceID, "identity-resource-id", "", "Resource ID of user-assigned identity.")

	//login with SPN
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
	azCliCred        bool
	psCred           bool

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

func (lca loginCmdArgs) validate() error {
	// Only support one kind of oauth login at same time.
	switch {
	case lca.identity:
		if lca.servicePrincipal {
			return errors.New("you can only log in with one type of auth at once")
		}

		// Consider only command-line parameters as env vars are a hassle to change and it's not like we'll use them here.
		if lca.tenantID != "" || lca.applicationID != "" || lca.certPath != "" {
			return errors.New("tenant ID/application ID/cert path/client secret cannot be used with identity")
		}
	case lca.servicePrincipal:
		if lca.identity {
			return errors.New("you can only log in with one type of auth at once")
		}

		if lca.identityClientID != "" || lca.identityObjectID != "" || lca.identityResourceID != "" {
			return errors.New("identity client/object/resource ID are exclusive to managed service identity auth and are not compatible with service principal auth")
		}

		if lca.applicationID == "" || (lca.clientSecret == "" && lca.certPath == "") {
			return errors.New("service principal auth requires an application ID, and client secret/certificate")
		}
	default: // OAuth login.
		// This isn't necessary, but stands as a sanity check. It will never be hit.
		if lca.servicePrincipal || lca.identity {
			return errors.New("you can only log in with one type of auth at once")
		}

		// Consider only command-line parameters as env vars are a hassle to change and it's not like we'll use them here.
		if lca.applicationID != "" || lca.certPath != "" {
			return errors.New("application ID and certificate paths are exclusive to service principal auth and are not compatible with OAuth")
		}

		if lca.identityClientID != "" || lca.identityObjectID != "" || lca.identityResourceID != "" {
			return errors.New("identity client/object/resource IDs are exclusive to managed service identity auth and are not compatible with OAuth")
		}
	}

	return nil
}

func (lca loginCmdArgs) process() error {
	// Validate login parameters.
	if err := lca.validate(); err != nil {
		return err
	}

	uotm := GetUserOAuthTokenManagerInstance()
	// Persist the token to cache, if login fulfilled successfully.

	switch {
	case lca.servicePrincipal:

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
	case lca.identity:
		if err := uotm.MSILogin(common.IdentityInfo{
			ClientID: lca.identityClientID,
			ObjectID: lca.identityObjectID,
			MSIResID: lca.identityResourceID,
		}, lca.persistToken); err != nil {
			return err
		}
		// For MSI login, info success message to user.
		glcm.Info("Login with identity succeeded.")
	case lca.azCliCred:
		if err := uotm.AzCliLogin(lca.tenantID); err != nil {
			return err
		}
		glcm.Info("Login with AzCliCreds succeeded")
	case lca.psCred:
		if err := uotm.PSContextToken(lca.tenantID); err != nil {
			return err
		}
		glcm.Info("Login with Powershell context succeeded")
	default:
		if err := uotm.UserLogin(lca.tenantID, lca.aadEndpoint, lca.persistToken); err != nil {
			return err
		}
		// User fulfills login in browser, and there would be message in browser indicating whether login fulfilled successfully.
		glcm.Info("Login succeeded.")
	}

	return nil
}
