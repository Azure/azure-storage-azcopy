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
		Short:      "login(lgin) launch oauth device login for current user.",
		Long: `login(lgin) launch oauth device login for current user. The most common cases are:
  - launch oauth device login for current user.
  - launch oauth device login for current user, with specified tenant id.`,
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

	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.tenantID, "tenant-id", common.DefaultTenantID, "Tenant id to use for OAuth device interactive login")
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.aadEndpoint, "aad-endpoint", common.DefaultActiveDirectoryEndpoint, "Azure active directory endpoint to use for OAuth user interactive login.")
}

type loginCmdArgs struct {
	// OAuth login arguments
	tenantID    string
	aadEndpoint string
}

func (lca loginCmdArgs) process() error {
	userOAuthTokenManager := GetUserOAuthTokenManagerInstance()
	_, err := userOAuthTokenManager.LoginWithADEndpoint(lca.tenantID, lca.aadEndpoint, true) // persist token = true
	if err != nil {
		return fmt.Errorf(
			"login failed with tenantID '%s', using public Azure directory endpoint 'https://login.microsoftonline.com', due to error: %s",
			lca.tenantID,
			err.Error())
	}

	return nil
}
