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

import "github.com/Azure/azure-storage-azcopy/v10/common"

type LoginOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   common.AutoLoginType

	// Managed Identity Options
	IdentityClientID   string
	IdentityResourceID string
	IdentityObjectID   string

	// Service Principal Options
	ApplicationID       string
	ClientSecret        string
	CertificatePath     string
	CertificatePassword string

	PersistToken bool // Whether to persist the token in the credential cache
}

type LoginResponse struct {
}

func (c *Client) Login(opts LoginOptions) (LoginResponse, error) {
	resp := LoginResponse{}
	uotm := GetUserOAuthTokenManagerInstance()

	// Persist the token to cache, if login fulfilled successfully.
	switch opts.LoginType {
	case common.EAutoLoginType.SPN():
		if opts.CertificatePath != "" {
			return resp, uotm.CertLogin(opts.TenantID, opts.AADEndpoint, opts.CertificatePath, opts.CertificatePassword, opts.ApplicationID, opts.PersistToken)
		} else {
			return resp, uotm.SecretLogin(opts.TenantID, opts.AADEndpoint, opts.ClientSecret, opts.ApplicationID, opts.PersistToken)
		}
	case common.EAutoLoginType.MSI():
		return resp, uotm.MSILogin(common.IdentityInfo{
			ClientID: opts.IdentityClientID,
			ObjectID: opts.IdentityObjectID,
			MSIResID: opts.IdentityResourceID,
		}, opts.PersistToken)
	case common.EAutoLoginType.AzCLI():
		return resp, uotm.AzCliLogin(opts.TenantID, opts.PersistToken)
	case common.EAutoLoginType.PsCred():
		return resp, uotm.PSContextToken(opts.TenantID, opts.PersistToken)
	case common.EAutoLoginType.Workload():
		return resp, uotm.WorkloadIdentityLogin(opts.PersistToken)
	default:
		return resp, uotm.UserLogin(opts.TenantID, opts.AADEndpoint, opts.PersistToken)
		// User fulfills login in browser, and there would be message in browser indicating whether login fulfilled successfully.
	}
}
