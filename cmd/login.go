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
		Aliases:    []string{"login"},
		SuggestFor: []string{"lgin"},
		Short:      "login(lgin) launch oauth MSI login or device login for current user.",
		Long:       `login(lgin) launch oauth MSI login or device login for current user.`,
		Example: `Login with specified tenant ID:
- azcopy login --tenant-id "[TenantID]"

Login with default tenant ID (microsoft.com):
- azcopy login

Login with managed identity (MSI):
- azcopy login --identity
`,
		Args: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := loginCmdArgs.process()
			if err != nil {
				return fmt.Errorf("failed to perform login command due to error %s", err.Error())
			}
			return nil
		},
		// hide oauth feature temporarily
		Hidden: true,
	}

	rootCmd.AddCommand(lgCmd)

	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.tenantID, "tenant-id", "", "tenant id to use for OAuth device interactive login")
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.aadEndpoint, "aad-endpoint", "", "Azure active directory endpoint to use for OAuth user interactive login")
	// Use identity which aligns to Azure powershell and CLI.
	lgCmd.PersistentFlags().BoolVar(&loginCmdArgs.identity, "identity", false, "use virtual machine's identity, formerly known as managed service identity (MSI)")
	// Temporarily use u which aligns to Azure CLI.
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.identityID, "u", "", "managed service identity ID")

	// hide flags
	// temporaily hide aad-endpoint and support Production environment only.
	lgCmd.PersistentFlags().MarkHidden("aad-endpoint")
	lgCmd.PersistentFlags().MarkHidden("u")
}

type loginCmdArgs struct {
	// OAuth login arguments
	tenantID    string
	aadEndpoint string

	identity   bool   // Whether to use MSI.
	identityID string // VM's user assigned identity, client or object ids of the service identity.
}

func (lca loginCmdArgs) validate() error {
	// Only support one kind of oauth login at same time.
	if lca.identity && lca.tenantID != "" {
		return errors.New("tenant ID cannot be used with identity")
	}

	if !lca.identity && lca.identityID != "" {
		return errors.New("u is only valid when using identity")
	}

	return nil
}

func (lca loginCmdArgs) process() error {
	// Validate login parameters.
	if err := lca.validate(); err != nil {
		return err
	}

	uotm := GetUserOAuthTokenManagerInstance()
	// Persist the token to cache, if login fulfilled succesfully.
	if lca.identity {
		if _, err := uotm.MSILogin(context.TODO(), lca.identityID, true); err != nil {
			return err
		}
	} else {
		if _, err := uotm.UserLogin(lca.tenantID, lca.aadEndpoint, true); err != nil {
			return err
		}
	}

	return nil
}
