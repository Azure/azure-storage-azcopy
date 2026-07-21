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
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/spf13/cobra"
)

type LoginStatusOptions struct {
	TenantID          bool
	AADEndpoint       bool
	Method            bool
	NicknameSpecified bool
}

type LoginStatus struct {
	Identities map[string]IdentityStatus
}

type IdentityStatus struct {
	Valid       bool   `json:"valid"`
	Error       error  `json:"error,omitempty"`
	TenantID    string `json:"tenantID"`
	AADEndpoint string `json:"AADEndpoint"`
	AuthMethod  string `json:"authMethod"`
}

func (options LoginStatusOptions) process() LoginStatus {
	manager := GetCredentialManager()

	var creds []cred.TokenHeader

	if options.NicknameSpecified {
		nickname := TargetCredentialName
		header, ok := manager.ProbeToken(nickname)
		if !ok {
			return LoginStatus{Identities: map[string]IdentityStatus{
				nickname: {Valid: false, Error: errors.New("identity not found")},
			}}
		}

		creds = []cred.TokenHeader{header}
	} else {
		var err error
		creds, err = manager.ListCredentials()
		if err != nil {
			return LoginStatus{}
		}
	}

	if len(creds) == 0 {
		return LoginStatus{}
	}

	status := LoginStatus{
		make(map[string]IdentityStatus),
	}

	for _, c := range creds {
		result := IdentityStatus{
			TenantID:    c.Tenant,
			AADEndpoint: c.ActiveDirectoryEndpoint,
			AuthMethod:  c.LoginType.String(),
		}

		if targetCred, err := manager.GetCredentials(c.Nickname, nil); err != nil {
			result.Error = err
		} else {
			_, err = cred.NewScopedToken(targetCred, enum.ECredentialType.OAuthToken()).GetToken(context.Background(), policy.TokenRequestOptions{})
			if err != nil {
				result.Error = err
			} else {
				result.Valid = true
			}
		}

		status.Identities[c.Nickname] = result
	}

	return status
}

func RunLoginStatus(options LoginStatusOptions) LoginStatus {
	return options.process()
}

func init() {
	commandLineInput := LoginStatusOptions{}

	lgStatus := &cobra.Command{
		Use:   "status",
		Short: loginStatusShortDescription,
		Long:  loginStatusLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			commandLineInput.NicknameSpecified = cmd.PersistentFlags().Changed("nickname")
			status := RunLoginStatus(commandLineInput)

			glcm.Output(func(format common.OutputFormat) string {
				if format == common.EOutputFormat.Json() {
					buf, _ := json.Marshal(status)
					return string(buf)
				} else {
					var output strings.Builder

					for nickname, ident := range status.Identities {
						output.WriteString(fmt.Sprintf("identity `%s`: ", nickname))
						output.WriteString(ternary.Iff(ident.Valid, "refreshed successfully", "failed refresh (re-login required)"))
						output.WriteString(fmt.Sprintf("; tenant ID: `%s` / AD endpoint: `%s` / Auth method: `%s`", ident.TenantID, ident.AADEndpoint, ident.AuthMethod))

						if !ident.Valid {
							output.WriteString(fmt.Sprintf("\nrefresh error: %s", ident.Error.Error()))
						}
						output.WriteString("\n\n")
					}

					return output.String()
				}
			}, common.EOutputMessageType.LoginStatusInfo())

			for _, v := range status.Identities {
				if !v.Valid {
					glcm.Exit(nil, common.EExitCode.Error())
				}
			}

			glcm.Exit(nil, common.EExitCode.Success())
		},
	}

	AddTargetCredFlags(lgStatus, "nickname")

	lgCmd.AddCommand(lgStatus)
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.TenantID, "tenant", false, "Prints the Microsoft Entra tenant ID that is currently being used in session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.AADEndpoint, "endpoint", false, "Prints the Microsoft Entra endpoint that is being used in the current session.")
	lgStatus.PersistentFlags().BoolVar(&commandLineInput.Method, "method", false, "Prints the authorization method used in the current session.")
}
