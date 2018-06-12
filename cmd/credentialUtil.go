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

package cmd

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
)

var once sync.Once

// only one UserOAuthTokenManager should exists in one azcopy-v2 process for current user.
// (given appAppPathFolder is mapped to current user)
var currentUserOAuthTokenManager *common.UserOAuthTokenManager

// GetUserOAuthTokenManagerInstance get or create OAuthTokenManager for current user.
// Note: Currently, only support to have TokenManager for one user mapping to one tenantID.
func GetUserOAuthTokenManagerInstance() *common.UserOAuthTokenManager {
	once.Do(func() {
		if azcopyAppPathFolder == "" {
			panic("invalid state, azcopyAppPathFolder should be initialized by root")
		}
		currentUserOAuthTokenManager = common.NewUserOAuthTokenManagerInstance(azcopyAppPathFolder)
	})

	return currentUserOAuthTokenManager
}

// ==============================================================================================
// Credential type methods
// ==============================================================================================

func getBlobCredentialType(ctx context.Context, blobResourceURL string, isSource bool) (common.CredentialType, error) {
	resourceURL, err := url.Parse(blobResourceURL)
	if err != nil {
		return common.ECredentialType.Unknown(), fmt.Errorf("illegal blobResourceURL, provided blobResourceURL is not in URL format")
	}

	sas := azblob.NewBlobURLParts(*resourceURL).SAS

	// If SAS existed, return anonymous credential type.
	if isSASExisted := sas.Signature() != ""; isSASExisted {
		return common.ECredentialType.Anonymous(), nil
	}

	uotm := GetUserOAuthTokenManagerInstance()

	if !uotm.HasCachedToken() {
		return common.ECredentialType.Anonymous(), nil
	} else {
		// If has cached token, and no SAS token provided, it could be a public blob resource.
		p := azblob.NewPipeline(
			azblob.NewAnonymousCredential(),
			azblob.PipelineOptions{
				Retry: azblob.RetryOptions{
					Policy:        azblob.RetryPolicyExponential,
					MaxTries:      ste.UploadMaxTries,
					TryTimeout:    ste.UploadTryTimeout,
					RetryDelay:    ste.UploadRetryDelay,
					MaxRetryDelay: ste.UploadMaxRetryDelay,
				},
			})

		isContainer := copyHandlerUtil{}.urlIsContainerOrShare(resourceURL)
		isPublicResource := false

		if isContainer {
			containerURL := azblob.NewContainerURL(*resourceURL, p)
			if _, err := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{}); err == nil {
				isPublicResource = true
			}
		} else {
			blobURL := azblob.NewBlobURL(*resourceURL, p)
			if _, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}); err == nil {
				isPublicResource = true
			}
		}

		if isPublicResource {
			return common.ECredentialType.Anonymous(), nil
		} else {
			return common.ECredentialType.OAuthToken(), nil
		}
	}
}

// ==============================================================================================
// pipeline factory methods
// ==============================================================================================

func createBlobPipeline(credentialType common.CredentialType) (pipeline.Pipeline, error) {
	switch credentialType {
	case common.ECredentialType.Anonymous():
		return azblob.NewPipeline(
			azblob.NewAnonymousCredential(),
			azblob.PipelineOptions{
				Retry: azblob.RetryOptions{
					Policy:        azblob.RetryPolicyExponential,
					MaxTries:      ste.UploadMaxTries,
					TryTimeout:    ste.UploadTryTimeout,
					RetryDelay:    ste.UploadRetryDelay,
					MaxRetryDelay: ste.UploadMaxRetryDelay,
				},
			}), nil
	case common.ECredentialType.OAuthToken():
		uotm := GetUserOAuthTokenManagerInstance()
		if freshToken, err := uotm.GetTokenInfoWithDefaultSettings(); err != nil {
			return nil, err
		} else {
			return azblob.NewPipeline(
				azblob.NewTokenCredential(freshToken.AccessToken),
				azblob.PipelineOptions{
					Retry: azblob.RetryOptions{
						Policy:        azblob.RetryPolicyExponential,
						MaxTries:      ste.UploadMaxTries,
						TryTimeout:    ste.UploadTryTimeout,
						RetryDelay:    ste.UploadRetryDelay,
						MaxRetryDelay: ste.UploadMaxRetryDelay,
					},
				}), nil
		}
	default:
		panic(fmt.Errorf("illegal credentialType: %v", credentialType))
	}
}
