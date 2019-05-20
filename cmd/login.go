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
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

func init() {
	loginCmdArgs := loginCmdArgs{tenantID: common.DefaultTenantID}

	// lgCmd represents the login command
	lgCmd := &cobra.Command{
		Use:        "login",
		SuggestFor: []string{"login"},
		Short:      loginCmdShortDescription,
		Long:       loginCmdLongDescription,
		Example:    loginCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := loginCmdArgs.process()
			if err != nil {
				return fmt.Errorf("failed to perform login command, %v", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(lgCmd)

	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.tenantID, "tenant-id", "", "the Azure active directory tenant id to use for OAuth device interactive login")
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.aadEndpoint, "aad-endpoint", "", "the Azure active directory endpoint to use for OAuth user interactive login")
	// Use identity which aligns to Azure powershell and CLI.
	lgCmd.PersistentFlags().BoolVar(&loginCmdArgs.identity, "identity", false, "log in using virtual machine's identity, also known as managed service identity (MSI)")
	// Client ID of user-assigned identity.
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.identityClientID, "identity-client-id", "", "client ID of user-assigned identity")
	// Object ID of user-assigned identity.
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.identityObjectID, "identity-object-id", "", "object ID of user-assigned identity")
	// Resource ID of user-assigned identity.
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.identityResourceID, "identity-resource-id", "", "resource ID of user-assigned identity")

	// hide flags
	// temporaily hide aad-endpoint and support Production environment only.
	lgCmd.PersistentFlags().MarkHidden("aad-endpoint")
}

type loginCmdArgs struct {
	// OAuth login arguments
	tenantID    string
	aadEndpoint string

	identity bool // Whether to use MSI.

	// Info of VM's user assigned identity, client or object ids of the service identity are required if
	// your VM has multiple user-assigned managed identities.
	// https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-go
	identityClientID   string
	identityObjectID   string
	identityResourceID string
}

func (lca loginCmdArgs) validate() error {
	// Only support one kind of oauth login at same time.
	if lca.identity && lca.tenantID != "" {
		return errors.New("tenant ID cannot be used with identity")
	}

	if !lca.identity && (lca.identityClientID != "" || lca.identityObjectID != "" || lca.identityResourceID != "") {
		return errors.New("identity client/object/resource ID is only valid when using identity")
	}

	return nil
}

func (lca loginCmdArgs) process() error {
	// Validate login parameters.
	if err := lca.validate(); err != nil {
		return err
	}

	if lca.tenantID == "" || lca.tenantID == "common" {
		glcm.Info("Logging in under the \"Common\" tenant. This will log the account in under its home tenant.")
		glcm.Info("If you plan to use AzCopy with a B2B account (where the account's home tenant is separate from the tenant of the target storage account), please sign in under the target tenant with --tenant-id\n")
	}

	uotm := GetUserOAuthTokenManagerInstance()
	// Persist the token to cache, if login fulfilled successfully.
	if lca.identity {
		if _, err := uotm.MSILogin(context.TODO(), common.IdentityInfo{
			ClientID: lca.identityClientID,
			ObjectID: lca.identityObjectID,
			MSIResID: lca.identityResourceID,
		}, true); err != nil {
			return err
		}
		// For MSI login, info success message to user.
		glcm.Info("Login with identity succeeded.")
	} else {
		if _, err := uotm.UserLogin(lca.tenantID, lca.aadEndpoint, true); err != nil {
			return err
		}
		// User fulfills login in browser, and there would be message in browser indicating whether login fulfilled successfully.
		glcm.Info("Login succeeded.")
	}

	return nil
}
