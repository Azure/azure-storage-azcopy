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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

type CredentialInfo struct {
	CredentialType    enum.CredentialType
	TokenCredential   azcore.TokenCredential
	S3CredentialInfo  S3CredentialInfo
	GCPCredentialInfo GCPCredentialInfo
}

func (c CredentialInfo) WithType(credentialType enum.CredentialType) CredentialInfo {
	// c is a clone, so this is OK
	c.CredentialType = credentialType
	return c
}
