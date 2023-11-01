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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalakedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	datalakeservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	sharedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
)

var glcm = GetLifecycleMgr()

// newClientCallbacks is a Generic Type to allow client creation error handling to live in a single place (createClient)
// T = Client type
// U = SharedKeyCredential type
// Note : Could also make azcore.ClientOptions generic here if one day different storage service clients have additional options. This would also make the callback definitions easier.
type newClientCallbacks[T, U any] struct {
	NewSharedKeyCredential func(string, string) (*U, error)
	SharedKeyCredential    func(string, *U, azcore.ClientOptions) (*T, error)
	TokenCredential        func(string, azcore.TokenCredential, azcore.ClientOptions) (*T, error)
	NoCredential           func(string, azcore.ClientOptions) (*T, error)
}

// createClient is a generic method to allow client creation error handling to live in a single place
func createClient[T, U any](callbacks newClientCallbacks[T, U], u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) (client *T) {
	var err error
	if credOpOptions == nil {
		credOpOptions = &CredentialOpOptions{
			LogError: glcm.Info,
		}
	}
	switch credInfo.CredentialType {
	case ECredentialType.OAuthToken():
		if credInfo.OAuthTokenInfo.IsEmpty() {
			err = errors.New("invalid state, cannot get valid OAuth token information")
			break
		}
		var tc azcore.TokenCredential
		tc, err = credInfo.OAuthTokenInfo.GetTokenCredential()
		if err != nil {
			err = fmt.Errorf("unable to get token credential due to reason (%s)", err.Error())
			break
		}
		client, err = callbacks.TokenCredential(u, tc, options)
	case ECredentialType.Anonymous():
		client, err = callbacks.NoCredential(u, options)
	case ECredentialType.SharedKey():
		name := lcm.GetEnvironmentVariable(EEnvironmentVariable.AccountName())
		key := lcm.GetEnvironmentVariable(EEnvironmentVariable.AccountKey())
		// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
		if name == "" || key == "" {
			err = fmt.Errorf("ACCOUNT_NAME and ACCOUNT_KEY environment variables must be set before creating the SharedKey credential")
			break
		}
		var sharedKey *U
		sharedKey, err = callbacks.NewSharedKeyCredential(name, key)
		if err != nil {
			err = fmt.Errorf("unable to get shared key credential due to reason (%s)", err.Error())
			break
		}
		client, err = callbacks.SharedKeyCredential(u, sharedKey, options)
	default:
		err = fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
	}
	if err != nil {
		credOpOptions.panicError(err)
	}
	return client
}

///////////////////////////////////////////////// BLOB FUNCTIONS /////////////////////////////////////////////////

// CreateBlobServiceClient creates a blob service client with credentials specified by credInfo
func CreateBlobServiceClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *blobservice.Client {
	callbacks := newClientCallbacks[blobservice.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*blobservice.Client, error) {
			return blobservice.NewClient(u, tc, &blobservice.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*blobservice.Client, error) {
			return blobservice.NewClientWithNoCredential(u, &blobservice.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*blobservice.Client, error) {
			return blobservice.NewClientWithSharedKeyCredential(u, sharedKey, &blobservice.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*blob.SharedKeyCredential, error) {
			return blob.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateContainerClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *container.Client {
	callbacks := newClientCallbacks[container.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*container.Client, error) {
			return container.NewClient(u, tc, &container.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*container.Client, error) {
			return container.NewClientWithNoCredential(u, &container.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*container.Client, error) {
			return container.NewClientWithSharedKeyCredential(u, sharedKey, &container.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*blob.SharedKeyCredential, error) {
			return blob.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateBlobClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *blob.Client {
	callbacks := newClientCallbacks[blob.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*blob.Client, error) {
			return blob.NewClient(u, tc, &blob.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*blob.Client, error) {
			return blob.NewClientWithNoCredential(u, &blob.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*blob.Client, error) {
			return blob.NewClientWithSharedKeyCredential(u, sharedKey, &blob.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*blob.SharedKeyCredential, error) {
			return blob.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateAppendBlobClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *appendblob.Client {
	callbacks := newClientCallbacks[appendblob.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*appendblob.Client, error) {
			return appendblob.NewClient(u, tc, &appendblob.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*appendblob.Client, error) {
			return appendblob.NewClientWithNoCredential(u, &appendblob.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*appendblob.Client, error) {
			return appendblob.NewClientWithSharedKeyCredential(u, sharedKey, &appendblob.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*blob.SharedKeyCredential, error) {
			return blob.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateBlockBlobClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *blockblob.Client {
	callbacks := newClientCallbacks[blockblob.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*blockblob.Client, error) {
			return blockblob.NewClient(u, tc, &blockblob.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*blockblob.Client, error) {
			return blockblob.NewClientWithNoCredential(u, &blockblob.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*blockblob.Client, error) {
			return blockblob.NewClientWithSharedKeyCredential(u, sharedKey, &blockblob.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*blob.SharedKeyCredential, error) {
			return blob.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreatePageBlobClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *pageblob.Client {
	callbacks := newClientCallbacks[pageblob.Client, blob.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*pageblob.Client, error) {
			return pageblob.NewClient(u, tc, &pageblob.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*pageblob.Client, error) {
			return pageblob.NewClientWithNoCredential(u, &pageblob.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *blob.SharedKeyCredential, options azcore.ClientOptions) (*pageblob.Client, error) {
			return pageblob.NewClientWithSharedKeyCredential(u, sharedKey, &pageblob.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*blob.SharedKeyCredential, error) {
			return blob.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

///////////////////////////////////////////////// FILE FUNCTIONS /////////////////////////////////////////////////

func CreateFileServiceClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions, trailingDot *TrailingDotOption, from *Location) *fileservice.Client {
	allowTrailingDot := trailingDot != nil && *trailingDot == trailingDot.Enable()
	allowSourceTrailingDot := allowTrailingDot && from != nil && *from == ELocation.File()
	callbacks := newClientCallbacks[fileservice.Client, sharefile.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*fileservice.Client, error) {
			return fileservice.NewClient(u, tc, &fileservice.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot), FileRequestIntent: to.Ptr(fileservice.ShareTokenIntentBackup)})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*fileservice.Client, error) {
			return fileservice.NewClientWithNoCredential(u, &fileservice.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot)})
		},
		SharedKeyCredential: func(u string, sharedKey *fileservice.SharedKeyCredential, options azcore.ClientOptions) (*fileservice.Client, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*fileservice.SharedKeyCredential, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateShareClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions, trailingDot *TrailingDotOption, from *Location) *share.Client {
	allowTrailingDot := trailingDot != nil && *trailingDot == trailingDot.Enable()
	allowSourceTrailingDot := allowTrailingDot && from != nil && *from == ELocation.File()
	callbacks := newClientCallbacks[share.Client, sharefile.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*share.Client, error) {
			return share.NewClient(u, tc, &share.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot), FileRequestIntent: to.Ptr(share.TokenIntentBackup)})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*share.Client, error) {
			return share.NewClientWithNoCredential(u, &share.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot)})
		},
		SharedKeyCredential: func(u string, sharedKey *share.SharedKeyCredential, options azcore.ClientOptions) (*share.Client, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*share.SharedKeyCredential, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateShareFileClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions, trailingDot *TrailingDotOption, from *Location) *sharefile.Client {
	allowTrailingDot := trailingDot != nil && *trailingDot == trailingDot.Enable()
	allowSourceTrailingDot := allowTrailingDot && from != nil && *from == ELocation.File()
	callbacks := newClientCallbacks[sharefile.Client, sharefile.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*sharefile.Client, error) {
			return sharefile.NewClient(u, tc, &sharefile.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot), FileRequestIntent: to.Ptr(sharefile.ShareTokenIntentBackup)})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*sharefile.Client, error) {
			return sharefile.NewClientWithNoCredential(u, &sharefile.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot)})
		},
		SharedKeyCredential: func(u string, sharedKey *sharefile.SharedKeyCredential, options azcore.ClientOptions) (*sharefile.Client, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*sharefile.SharedKeyCredential, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateShareDirectoryClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions, trailingDot *TrailingDotOption, from *Location) *sharedirectory.Client {
	allowTrailingDot := trailingDot != nil && *trailingDot == trailingDot.Enable()
	allowSourceTrailingDot := allowTrailingDot && from != nil && *from == ELocation.File()
	callbacks := newClientCallbacks[sharedirectory.Client, sharedirectory.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*sharedirectory.Client, error) {
			return sharedirectory.NewClient(u, tc, &sharedirectory.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot), FileRequestIntent: to.Ptr(sharedirectory.ShareTokenIntentBackup)})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*sharedirectory.Client, error) {
			return sharedirectory.NewClientWithNoCredential(u, &sharedirectory.ClientOptions{ClientOptions: options, AllowTrailingDot: to.Ptr(allowTrailingDot), AllowSourceTrailingDot: to.Ptr(allowSourceTrailingDot)})
		},
		SharedKeyCredential: func(u string, sharedKey *sharedirectory.SharedKeyCredential, options azcore.ClientOptions) (*sharedirectory.Client, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*sharedirectory.SharedKeyCredential, error) {
			return nil, fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

///////////////////////////////////////////////// DATALAKE FUNCTIONS /////////////////////////////////////////////////

func CreateDatalakeServiceClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *datalakeservice.Client {
	callbacks := newClientCallbacks[datalakeservice.Client, azdatalake.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*datalakeservice.Client, error) {
			return datalakeservice.NewClient(u, tc, &datalakeservice.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*datalakeservice.Client, error) {
			return datalakeservice.NewClientWithNoCredential(u, &datalakeservice.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *azdatalake.SharedKeyCredential, options azcore.ClientOptions) (*datalakeservice.Client, error) {
			return datalakeservice.NewClientWithSharedKeyCredential(u, sharedKey, &datalakeservice.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*azdatalake.SharedKeyCredential, error) {
			return azdatalake.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateFilesystemClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *filesystem.Client {
	callbacks := newClientCallbacks[filesystem.Client, azdatalake.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*filesystem.Client, error) {
			return filesystem.NewClient(u, tc, &filesystem.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*filesystem.Client, error) {
			return filesystem.NewClientWithNoCredential(u, &filesystem.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *azdatalake.SharedKeyCredential, options azcore.ClientOptions) (*filesystem.Client, error) {
			return filesystem.NewClientWithSharedKeyCredential(u, sharedKey, &filesystem.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*azdatalake.SharedKeyCredential, error) {
			return azdatalake.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateDatalakeFileClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *datalakefile.Client {
	callbacks := newClientCallbacks[datalakefile.Client, azdatalake.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*datalakefile.Client, error) {
			return datalakefile.NewClient(u, tc, &datalakefile.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*datalakefile.Client, error) {
			return datalakefile.NewClientWithNoCredential(u, &datalakefile.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *azdatalake.SharedKeyCredential, options azcore.ClientOptions) (*datalakefile.Client, error) {
			return datalakefile.NewClientWithSharedKeyCredential(u, sharedKey, &datalakefile.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*azdatalake.SharedKeyCredential, error) {
			return azdatalake.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}

func CreateDatalakeDirectoryClient(u string, credInfo CredentialInfo, credOpOptions *CredentialOpOptions, options azcore.ClientOptions) *datalakedirectory.Client {
	callbacks := newClientCallbacks[datalakedirectory.Client, azdatalake.SharedKeyCredential]{
		TokenCredential: func(u string, tc azcore.TokenCredential, options azcore.ClientOptions) (*datalakedirectory.Client, error) {
			return datalakedirectory.NewClient(u, tc, &datalakedirectory.ClientOptions{ClientOptions: options})
		},
		NoCredential: func(u string, options azcore.ClientOptions) (*datalakedirectory.Client, error) {
			return datalakedirectory.NewClientWithNoCredential(u, &datalakedirectory.ClientOptions{ClientOptions: options})
		},
		SharedKeyCredential: func(u string, sharedKey *azdatalake.SharedKeyCredential, options azcore.ClientOptions) (*datalakedirectory.Client, error) {
			return datalakedirectory.NewClientWithSharedKeyCredential(u, sharedKey, &datalakedirectory.ClientOptions{ClientOptions: options})
		},
		NewSharedKeyCredential: func(accountName string, accountKey string) (*azdatalake.SharedKeyCredential, error) {
			return azdatalake.NewSharedKeyCredential(accountName, accountKey)
		},
	}

	return createClient(callbacks, u, credInfo, credOpOptions, options)
}
