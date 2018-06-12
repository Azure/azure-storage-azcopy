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

	"github.com/spf13/cobra"
)

func init() {
	logoutCmdArgs := logoutCmdArgs{}

	// lsCmd represents the ls command
	logoutCmd := &cobra.Command{
		Use:        "logout",
		Aliases:    []string{"logout"},
		SuggestFor: []string{"lgout"}, //TODO why does message appear twice on the console
		Short:      "logout(lgout) launch logout for current user.",
		Long: `logout(lg) launch logout for current user.. The most common cases are:
  - launch oauth device logout for current user, all cached token for current user will be deleted.`,
		Args: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := logoutCmdArgs.process()
			if err != nil {
				return fmt.Errorf("failed to perform logout command due to error %s", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(logoutCmd)

	// TODO: p2 functions, add tenant and cloud name, i.e. support soverign clouds
	//lgCmd.PersistentFlags().StringVar(&logoutCmdArgs.tenantID, "tenantid", "microsoft.com", "Filter: tenant id to use for OAuth device logout")
}

type logoutCmdArgs struct {
	// OAuth logout arguments
	//tenantID string
}

func (lca logoutCmdArgs) process() error {
	userOAuthTokenManager := GetUserOAuthTokenManagerInstance()
	err := userOAuthTokenManager.RemoveCachedToken()
	if err != nil {
		return fmt.Errorf(
			"fatal: logout failed due to error: %s",
			err.Error())
	}

	return nil
}
