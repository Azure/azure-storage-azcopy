// Copyright © 2024 Microsoft <wastore@microsoft.com>
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

package cred

import (
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

const (
	DefaultTenantID                = "common"
	DefaultActiveDirectoryEndpoint = "https://login.microsoftonline.com"
)

type LoginNewTokenOptions struct {
	TenantID    string
	AADEndpoint string
	loginType   enum.AutoLoginType

	IdentityClientID   string
	IdentityObjectID   string
	IdentityResourceID string

	ApplicationID   string
	CertificateData string
	ClientSecret    string

	SaveCredential bool
}

func NewLoginNewTokenOptions(loginType enum.AutoLoginType) LoginNewTokenOptions {
	return LoginNewTokenOptions{loginType: loginType}
}

func (opts LoginNewTokenOptions) LoginType() enum.AutoLoginType { return opts.loginType }

func (opts LoginNewTokenOptions) NewToken() Token {
	switch opts.loginType {
	case enum.EAutoLoginType.SPN():
		return NewSPNTokenOptions{
			TenantID:        opts.TenantID,
			AADEndpoint:     opts.AADEndpoint,
			ApplicationID:   opts.ApplicationID,
			CertificateData: opts.CertificateData,
			ClientSecret:    opts.ClientSecret,
		}.NewToken()
	case enum.EAutoLoginType.MSI():
		return NewMSITokenOptions{
			TenantID:           opts.TenantID,
			AADEndpoint:        opts.AADEndpoint,
			IdentityClientID:   opts.IdentityClientID,
			IdentityObjectID:   opts.IdentityObjectID,
			IdentityResourceID: opts.IdentityResourceID,
		}.NewToken()
	case enum.EAutoLoginType.Device():
		return NewUserLoginTokenOptions{
			TenantID:        opts.TenantID,
			AADEndpoint:     opts.AADEndpoint,
			ApplicationID:   opts.ApplicationID,
			InteractionType: enum.EInteractiveLoginType.Device(),
		}.NewToken()
	case enum.EAutoLoginType.Interactive():
		return NewUserLoginTokenOptions{
			TenantID:        opts.TenantID,
			AADEndpoint:     opts.AADEndpoint,
			ApplicationID:   opts.ApplicationID,
			InteractionType: enum.EInteractiveLoginType.Browser(),
		}.NewToken()
	case enum.EAutoLoginType.AzCLI():
		return NewAzureCLITokenOptions{
			TenantID:    opts.TenantID,
			AADEndpoint: opts.AADEndpoint,
		}.NewToken()
	case enum.EAutoLoginType.PsCred():
		return NewPSCredTokenOptions{
			TenantID:    opts.TenantID,
			AADEndpoint: opts.AADEndpoint,
		}.NewToken()
	case enum.EAutoLoginType.Workload():
		return NewWorkloadTokenOptions{
			TenantID:    opts.TenantID,
			AADEndpoint: opts.AADEndpoint,
		}.NewToken()
	default:
		// TokenStore and NoRefresh: construct empty token with header, fall back to reflection-based creation
		return &token{
			TokenHeader: TokenHeader{
				Tenant:                  opts.TenantID,
				ActiveDirectoryEndpoint: opts.AADEndpoint,
				LoginType:               opts.loginType,
			},
			tokenImpl: newTokenImpl(opts.loginType),
		}
	}
}
