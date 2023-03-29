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
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// newClientCallbacks is a Generic Type to allow client creation error handling to live in a single place (createClient)
// T = Client type
// U = SharedKeyCredential type
// Note : Could also make azcore.ClientOptions generic here if one day different storage service clients have additional options. This would also make the callback definitions easier.
type newClientCallbacks[T, U any] struct {
	TokenCredential        func(string, azcore.TokenCredential, azcore.ClientOptions) (*T, error)
	NewSharedKeyCredential func(string, string) (*U, error)
	SharedKeyCredential    func(string, *U, azcore.ClientOptions) (*T, error)
	NoCredential           func(string, azcore.ClientOptions) (*T, error)
}

// createClient is a generic method to allow client creation error handling to live in a single place
func createClient[T, U any](callbacks newClientCallbacks[T, U], u string, credInfo CredentialInfo, credOpOptions CredentialOpOptions, options azcore.ClientOptions) (*T, error) {
	switch credInfo.CredentialType {
	case ECredentialType.OAuthToken():
		if credInfo.OAuthTokenInfo.IsEmpty() {
			credOpOptions.panicError(errors.New("invalid state, cannot get valid OAuth token information"))
		}
		tc, err := credInfo.OAuthTokenInfo.GetTokenCredential()
		if err != nil {
			credOpOptions.panicError(fmt.Errorf("unable to get token credential due to reason (%s)", err.Error()))
		}
		return callbacks.TokenCredential(u, tc, options)
	case ECredentialType.SharedKey():
		// Get the Account Name and Key variables from environment
		name := lcm.GetEnvironmentVariable(EEnvironmentVariable.AccountName())
		key := lcm.GetEnvironmentVariable(EEnvironmentVariable.AccountKey())
		// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
		if name == "" || key == "" {
			credOpOptions.panicError(errors.New("ACCOUNT_NAME and ACCOUNT_KEY environment variables must be set before creating the blob SharedKey credential"))
		} // create the shared key credentials
		sharedKey, err := callbacks.NewSharedKeyCredential(name, key)
		if err != nil {
			credOpOptions.panicError(errors.New("failed to create the SharedKey credential"))
		}
		return callbacks.SharedKeyCredential(u, sharedKey, options)
	case ECredentialType.Anonymous():
		return callbacks.NoCredential(u, options)
	default:
		credOpOptions.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
		return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
	}
}

///////////////////////////////////////////////// BLOB FUNCTIONS /////////////////////////////////////////////////

// CreateBlobServiceClient creates a blob service client with credentials specified by credInfo
func CreateBlobServiceClient(u string, credInfo CredentialInfo, credOpOptions CredentialOpOptions, options azcore.ClientOptions) (*blobservice.Client, error) {
	callbacks := newClientCallbacks[blobservice.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*blobservice.Client, error) {
			return blobservice.NewClient(u, tc, &blobservice.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: blob.NewSharedKeyCredential,
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*blobservice.Client, error) {
			return blobservice.NewClientWithSharedKeyCredential(u, sharedKey, &blobservice.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*blobservice.Client, error) {
			return blobservice.NewClientWithNoCredential(u, &blobservice.ClientOptions{ClientOptions: options})
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

// TODO : Can this be isolated to the blob_traverser logic
func CreateBlobClientFromServiceClient(blobURLParts blob.URLParts, client *blobservice.Client) (*blob.Client, error) {
	containerClient := client.NewContainerClient(blobURLParts.ContainerName)
	blobClient := containerClient.NewBlobClient(blobURLParts.BlobName)
	if blobURLParts.Snapshot != "" {
		return blobClient.WithSnapshot(blobURLParts.Snapshot)
	}
	if blobURLParts.VersionID != "" {
		return blobClient.WithVersionID(blobURLParts.VersionID)
	}
	return blobClient, nil
}
