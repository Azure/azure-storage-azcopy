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

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type GetLoginStatusOptions struct {
}

type LoginStatus struct {
	Valid       bool
	TenantID    string
	AADEndpoint string
	LoginType   common.AutoLoginType
}

func (c Client) GetLoginStatus(_ GetLoginStatusOptions) (LoginStatus, error) {
	uotm := c.GetUserOAuthTokenManagerInstance()

	// Get current token info and refresh it with GetTokenInfo()
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	tokenInfo, err := uotm.GetTokenInfo(ctx)
	if err != nil || tokenInfo.IsExpired() {
		return LoginStatus{Valid: false}, errors.New("you are currently not logged in. please login using 'azcopy login'")
	}
	return LoginStatus{
		Valid:       true,
		TenantID:    tokenInfo.Tenant,
		AADEndpoint: tokenInfo.ActiveDirectoryEndpoint,
		LoginType:   tokenInfo.LoginType,
	}, nil
}
