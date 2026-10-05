// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

type LoginOptions struct {
	TenantID            string
	AADEndpoint         string
	LoginType           enum.AutoLoginType
	IdentityClientID    string
	IdentityResourceID  string
	IdentityObjectID    string
	ApplicationID       string
	CertificatePath     string
	CertificatePassword string
	ClientSecret        string
	PersistToken        bool
	CredentialName      string
}

type LoginResponse struct{}

func (c Client) Login(opts LoginOptions) (LoginResponse, error) {
	tokenOpts := cred.NewLoginNewTokenOptions(opts.LoginType)
	tokenOpts.TenantID = opts.TenantID
	tokenOpts.AADEndpoint = opts.AADEndpoint
	tokenOpts.IdentityClientID = opts.IdentityClientID
	tokenOpts.IdentityObjectID = opts.IdentityObjectID
	tokenOpts.IdentityResourceID = opts.IdentityResourceID
	tokenOpts.ApplicationID = opts.ApplicationID
	tokenOpts.CertificateData = opts.CertificatePath
	tokenOpts.ClientSecret = ternary.Iff(opts.ClientSecret != "", opts.ClientSecret, opts.CertificatePassword)
	tokenOpts.SaveCredential = opts.PersistToken
	tokenOpts.Nickname = opts.CredentialName
	_, err := c.GetCredentialManager().DoLogin(tokenOpts, context.Background())
	return LoginResponse{}, err
}

type GetLoginStatusOptions struct {
	NicknameSpecified bool
	Nickname          string
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

func (c Client) GetLoginStatus(opts GetLoginStatusOptions) (LoginStatus, error) {
	manager := c.GetCredentialManager()
	var headers []cred.TokenHeader
	if opts.NicknameSpecified {
		header, ok := manager.ProbeToken(opts.Nickname)
		if !ok {
			return LoginStatus{Identities: map[string]IdentityStatus{
				opts.Nickname: {Valid: false, Error: errors.New("identity not found")},
			}}, nil
		}
		headers = []cred.TokenHeader{header}
	} else {
		var err error
		headers, err = manager.ListCredentials()
		if err != nil {
			return LoginStatus{}, err
		}
	}
	if len(headers) == 0 {
		return LoginStatus{}, nil
	}
	status := LoginStatus{Identities: make(map[string]IdentityStatus)}
	for _, header := range headers {
		result := IdentityStatus{
			TenantID:    header.Tenant,
			AADEndpoint: header.ActiveDirectoryEndpoint,
			AuthMethod:  header.LoginType.String(),
		}
		token, err := manager.GetCredentials(header.Nickname, context.Background())
		if err == nil {
			_, err = cred.NewScopedToken(token, enum.ECredentialType.OAuthToken()).GetToken(context.Background(), policy.TokenRequestOptions{})
		}
		result.Error = err
		result.Valid = err == nil
		status.Identities[header.Nickname] = result
	}
	return status, nil
}

type LogoutOptions struct {
	Nickname string
}

type LogoutResponse struct {
}

func (c Client) Logout(opts LogoutOptions) (LogoutResponse, error) {
	if !c.GetCredentialManager().DeleteCredentials(opts.Nickname) {
		return LogoutResponse{}, fmt.Errorf("no cached token found for %q", opts.Nickname)
	}
	return LogoutResponse{}, nil
}
