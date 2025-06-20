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
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

type LoginStatusOutput struct {
	Valid       bool    `json:"valid"`
	TenantID    *string `json:"tenantID,omitempty"`
	AADEndpoint *string `json:"AADEndpoint,omitempty"`
	AuthMethod  *string `json:"authMethod,omitempty"`
}

func init() {
	type loginStatus struct {
		tenantID bool
		endpoint bool
		method   bool
	}
	commandLineInput := loginStatus{}

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
		Run: func(cmd *cobra.Command, args []string) {
			// getting current token info and refreshing it with GetTokenInfo()
			ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
			uotm := GetUserOAuthTokenManagerInstance()
			tokenInfo, err := uotm.GetTokenInfo(ctx)

			var Info = LoginStatusOutput{
				Valid: err == nil && !tokenInfo.IsExpired(),
			}

			logText := func(format string, a ...any) {
				if OutputFormat == common.EOutputFormat.None() || OutputFormat == common.EOutputFormat.Text() {
					glcm.Info(fmt.Sprintf(format, a...))
				}
			}

			if Info.Valid {
				logText("You have successfully refreshed your token. Your login session is still active")

				if commandLineInput.tenantID {
					logText("Tenant ID: %v", tokenInfo.Tenant)
					Info.TenantID = &tokenInfo.Tenant
				}

				if commandLineInput.endpoint {
					logText(fmt.Sprintf("Active directory endpoint: %v", tokenInfo.ActiveDirectoryEndpoint))
					Info.AADEndpoint = &tokenInfo.ActiveDirectoryEndpoint
				}

				if commandLineInput.method {
					logText(fmt.Sprintf("Authorized using %s", tokenInfo.LoginType))
					method := tokenInfo.LoginType.String()
					Info.AuthMethod = &method
				}
			} else {
				logText("You are currently not logged in. Please login using 'azcopy login'")
			}

			if OutputFormat == common.EOutputFormat.Json() {
				glcm.Output(
					func(_ common.OutputFormat) string {
						buf, err := json.Marshal(Info)
						if err != nil {
							panic(err)
						}

						return string(buf)
					}, common.EOutputMessageType.LoginStatusInfo())
			}

			glcm.Exit(nil, common.Iff(Info.Valid, common.EExitCode.Success(), common.EExitCode.Error()))
		},
	}

	lgCmd.AddCommand(lgStatus)
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.tenantID, "tenant", false, "Prints the Microsoft Entra tenant ID that is currently being used in session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.endpoint, "endpoint", false, "Prints the Microsoft Entra endpoint that is being used in the current session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.method, "method", false, "Prints the authorization method used in the current session.")
}
