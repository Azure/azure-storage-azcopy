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
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
)

func GetDatalakeSharedKeyCredential() (*azdatalake.SharedKeyCredential, error) {
	name := GetEnvironmentVariable(EEnvironmentVariable.AccountName())
	key := GetEnvironmentVariable(EEnvironmentVariable.AccountKey())
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if name == "" || key == "" {
		return nil, fmt.Errorf("ACCOUNT_NAME and ACCOUNT_KEY environment variables must be set before creating the SharedKey credential")
	}
	return azdatalake.NewSharedKeyCredential(name, key)
}

func GetBlobSharedKeyCredential() (*blob.SharedKeyCredential, error) {
	name := GetEnvironmentVariable(EEnvironmentVariable.AccountName())
	key := GetEnvironmentVariable(EEnvironmentVariable.AccountKey())
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if name == "" || key == "" {
		return nil, fmt.Errorf("ACCOUNT_NAME and ACCOUNT_KEY environment variables must be set before creating the SharedKey credential")
	}
	return blob.NewSharedKeyCredential(name, key)
}
