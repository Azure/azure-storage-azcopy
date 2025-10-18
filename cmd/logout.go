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
	logoutCmdArgs := LogoutOptions{}

	// logoutCmd represents the logout command
	logoutCmd := &cobra.Command{
		Use:        "logout",
		SuggestFor: []string{"logout"},
		Short:      logoutCmdShortDescription,
		Long:       logoutCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunLogout(logoutCmdArgs)
		},
	}

	rootCmd.AddCommand(logoutCmd)
}

func RunLogout(options LogoutOptions) error {
	return options.process()
}

type LogoutOptions struct{}

func (options LogoutOptions) process() error {
	uotm := Client.GetUserOAuthTokenManagerInstance()
	if err := uotm.RemoveCachedToken(); err != nil {
		return fmt.Errorf("failed to perform logout command, %v", err)
	}

	// For MSI login, info success message to user.
	glcm.Info("Logout succeeded.")

	return nil
}
