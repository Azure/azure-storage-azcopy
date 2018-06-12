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

	// lsCmd represents the ls command
	lgCmd := &cobra.Command{
		Use:        "login",
		Aliases:    []string{"login"},
		SuggestFor: []string{"lgin"}, //TODO why does message appear twice on the console
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
				return fmt.Errorf("failed to perform login command due to error %s", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(lgCmd)

	// TODO: add cloud name, i.e. support other cloud names, e.g.: fairfax, mooncake, blackforest and etc.
	lgCmd.PersistentFlags().StringVar(&loginCmdArgs.tenantID, "tenant-id", common.DefaultTenantID, "Filter: tenant id to use for OAuth device login")
}

type loginCmdArgs struct {
	// OAuth login arguments
	tenantID string
	//cloudName string
}

func (lca loginCmdArgs) process() error {
	userOAuthTokenManager := GetUserOAuthTokenManagerInstance()
	_, err := userOAuthTokenManager.LoginWithDefaultADEndpoint(lca.tenantID, true)
	if err != nil {
		return fmt.Errorf(
			"fatal: login failed with tenantID '%s', using public Azure directory endpoint 'https://login.microsoftonline.com', due to error: %s",
			lca.tenantID,
			err.Error())
	}

	return nil
}
