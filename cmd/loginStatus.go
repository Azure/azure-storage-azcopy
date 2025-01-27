// Copyright © Microsoft <wastore@microsoft.com>
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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

type LoginStatusOptions struct {
	TenantId    bool
	AadEndpoint bool
}

func (options LoginStatusOptions) process() error {
	// getting current token info and refreshing it with GetTokenInfo()
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	uotm := GetUserOAuthTokenManagerInstance()
	tokenInfo, err := uotm.GetTokenInfo(ctx)

	if err == nil && !tokenInfo.IsExpired() {
		glcm.Info("You have successfully refreshed your token. Your login session is still active")

		if options.TenantId {
			glcm.Info(fmt.Sprintf("Tenant ID: %v", tokenInfo.Tenant))
		}

		if options.AadEndpoint {
			glcm.Info(fmt.Sprintf("Active directory endpoint: %v", tokenInfo.ActiveDirectoryEndpoint))
		}

		glcm.Exit(nil, common.EExitCode.Success())
	}

	glcm.Info("You are currently not logged in. Please login using 'azcopy login'")
	glcm.Exit(nil, common.EExitCode.Error())
	return nil
}

var commandLineInput = LoginStatusOptions{}

func RunLoginStatus(options LoginStatusOptions) error {
	return options.process()
}

func init() {
	lgStatus := &cobra.Command{
		Use:   "status",
		Short: loginStatusShortDescription,
		Long:  loginStatusLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return fmt.Errorf("login status does not require any argument")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunLoginStatus(commandLineInput)
		},
	}

	lgCmd.AddCommand(lgStatus)
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.TenantId, "tenant", false, "Prints the Azure Active Directory tenant ID that is currently being used in session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.AadEndpoint, "endpoint", false, "Prints the Azure Active Directory endpoint that is being used in the current session.")
}
