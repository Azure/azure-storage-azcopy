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
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/spf13/cobra"
)

type LoginStatusOptions struct {
	TenantID    bool
	AADEndpoint bool
	Method      bool
}

type LoginStatus struct {
	Valid       bool
	TenantID    string
	AADEndpoint string
	AuthMethod  enum.AutoLoginType
}

func (options LoginStatusOptions) process() (LoginStatus, error) {
	manager := GetCredentialManager()
	creds, err := manager.ListCredentials()
	if err != nil {
		return LoginStatus{}, err
	}

	if len(creds) == 0 {
		return LoginStatus{}, nil
	}

	primary := creds[0]
	status := LoginStatus{
		Valid:       true,
		TenantID:    primary.Tenant,
		AADEndpoint: primary.ActiveDirectoryEndpoint,
		AuthMethod:  primary.LoginType,
	}

	if OutputFormat == common.EOutputFormat.None() || OutputFormat == common.EOutputFormat.Text() {
		for _, c := range creds {
			nickname := c.Nickname
			if nickname == "" {
				nickname = c.Tenant
			} else if nickname == "*" {
				c.Tenant += "; default token"
			}
			glcm.Info(fmt.Sprintf("Credential: %s (Tenant: %s)", nickname, c.Tenant))
		}
	}

	return status, nil
}

func RunLoginStatus(options LoginStatusOptions) (LoginStatus, error) {
	return options.process()
}

type LoginStatusOutput struct {
	Valid       bool    `json:"valid"`
	TenantID    *string `json:"tenantID,omitempty"`
	AADEndpoint *string `json:"AADEndpoint,omitempty"`
	AuthMethod  *string `json:"authMethod,omitempty"`
}

func init() {
	commandLineInput := LoginStatusOptions{}

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
			logText := func(format string, a ...any) {
				if OutputFormat == common.EOutputFormat.None() || OutputFormat == common.EOutputFormat.Text() {
					glcm.Info(fmt.Sprintf(format, a...))
				}
			}
			status, _ := RunLoginStatus(commandLineInput)
			var Info = LoginStatusOutput{
				Valid: status.Valid,
			}
			if Info.Valid {
				logText("You have successfully refreshed your token. Your login session is still active")

				if commandLineInput.TenantID {
					logText("Tenant ID: %v", status.TenantID)
					Info.TenantID = &status.TenantID
				}

				if commandLineInput.AADEndpoint {
					logText(fmt.Sprintf("Active directory endpoint: %v", status.AADEndpoint))
					Info.AADEndpoint = &status.AADEndpoint
				}

				if commandLineInput.Method {
					logText(fmt.Sprintf("Authorized using %s", status.AuthMethod))
					method := status.AuthMethod.String()
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

			glcm.Exit(nil, ternary.Iff(Info.Valid, common.EExitCode.Success(), common.EExitCode.Error()))
		},
	}

	lgCmd.AddCommand(lgStatus)
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.TenantID, "tenant", false, "Prints the Microsoft Entra tenant ID that is currently being used in session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.AADEndpoint, "endpoint", false, "Prints the Microsoft Entra endpoint that is being used in the current session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.Method, "method", false, "Prints the authorization method used in the current session.")
}
