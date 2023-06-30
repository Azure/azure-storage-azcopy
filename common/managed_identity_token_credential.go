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

package common

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"time"
)


func NewManagedIdentityTokenCredential(msiCredential *azidentity.ManagedIdentityCredential) azcore.TokenCredential {
	tCred := &msiTokenCredential{}
	tCred.msiCredential = msiCredential
	return tCred
}

// tokenCredential is a pipeline.Factory is the credential's policy factory.
type msiTokenCredential struct {
	msiCredential *azidentity.ManagedIdentityCredential
	token azcore.AccessToken
}

func (f *msiTokenCredential) GetToken(ctx context.Context, tro policy.TokenRequestOptions) (azcore.AccessToken, error) {
	waitDuration := f.token.ExpiresOn.Sub(time.Now().UTC()) / 2
	if !f.token.ExpiresOn.After(time.Now().Add(waitDuration)) {
		token, err := f.msiCredential.GetToken(ctx, tro)
		if err != nil {
			return token, err
		}
		f.token = token
	}
	return f.token, nil
}