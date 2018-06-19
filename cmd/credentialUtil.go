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
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
)

var once sync.Once

// only one UserOAuthTokenManager should exists in azcopy-v2 process in cmd(FE) module for current user.
// (given appAppPathFolder is mapped to current user)
var currentUserOAuthTokenManager *common.UserOAuthTokenManager

// GetUserOAuthTokenManagerInstance gets or creates OAuthTokenManager for current user.
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

// getBlobCredentialType is used to get Blob's credential type when user wishes to use OAuth session mode.
// The verification logic follows following rules:
// 1. For source or dest url, if the url contains SAS, indicating using anonymous credential(SAS).
// 2. If it's source blob url, and it's a public resource, indicating using anonymous credential(public resource).
// 3. Otherwise, if there is cached session OAuth token, indicating using token credential.
// 4. Otherwise use anonymous credential.
// The implementaion logic follows above rule, and adjusts sequence to save web request(for verifying public resource).
func getBlobCredentialType(ctx context.Context, blobResourceURL string, isSource bool) (common.CredentialType, error) {
	resourceURL, err := url.Parse(blobResourceURL)

	// TODO: Clean up user messages in errors.
	// If error is due to a user error, make the error message user friendly.
	// If error is due to a program bug, error should be logged in the general azcopy log (if the bug is not related to a job), or in the job-specific log if it is related to a job.
	// and terminate the application.
	if err != nil {
		return common.ECredentialType.Unknown(), errors.New("provided blob resource string is not in URL format")
	}

	sas := azblob.NewBlobURLParts(*resourceURL).SAS

	// If SAS existed, return anonymous credential type.
	if isSASExisted := sas.Signature() != ""; isSASExisted {
		return common.ECredentialType.Anonymous(), nil
	}

	uotm := GetUserOAuthTokenManagerInstance()

	if !uotm.HasCachedToken() {
		return common.ECredentialType.Anonymous(), nil
	} else if !isSource {
		return common.ECredentialType.OAuthToken(), nil
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

// getBlobFSCredentialType is used to get BlobFS's credential type when user wishes to use OAuth session mode.
// The verification logic follows following rules:
// 1. If there is cached session OAuth token, indicating using token credential.
// 2. Otherwise use anonymous credential. TODO: ensure if blob FS supports any kind of anonymous credential.
func getBlobFSCredentialType() (common.CredentialType, error) {
	uotm := GetUserOAuthTokenManagerInstance()

	if uotm.HasCachedToken() {
		return common.ECredentialType.OAuthToken(), nil
	} else {
		return common.ECredentialType.SharedKey(), nil // For internal testing, SharedKey is not supported from commandline
	}
}

// ==============================================================================================
// pipeline factory methods
// ==============================================================================================
func createBlobPipeline(ctx context.Context, credInfo common.CredentialInfo) (pipeline.Pipeline, error) {
	credential := createBlobCredential(ctx, credInfo)

	return azblob.NewPipeline(
		credential,
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

func createBlobCredential(ctx context.Context, credInfo common.CredentialInfo) azblob.Credential {
	credential := azblob.NewAnonymousCredential()

	if credInfo.CredentialType == common.ECredentialType.OAuthToken() {
		if credInfo.OAuthTokenInfo.IsEmpty() {
			panic(fmt.Errorf("invalid state, cannot get valid token info for OAuthToken credential"))
		}

		// Create TokenCredential and set access token into it.
		tokenCredential := azblob.NewTokenCredential(credInfo.OAuthTokenInfo.AccessToken)

		if credInfo.OAuthTokenInfo.IsExpired() || credInfo.OAuthTokenInfo.WillExpireIn(common.DefaultTokenExpiryWithinThreshold) {
			// If token is near expire, or already expired, refresh immediately before return token.
			refreshBlobToken(ctx, credInfo.OAuthTokenInfo, tokenCredential)
		} else {
			// Otherwise, calculate the next refresh time, and schedule the refresh.
			waitDuration := credInfo.OAuthTokenInfo.Expires().Sub(time.Now().UTC()) - common.DefaultTokenExpiryWithinThreshold

			//waitDuration = time.Second * 2 // TODO: Add mock testing
			if waitDuration < time.Second {
				waitDuration = time.Nanosecond
			}

			_ = time.AfterFunc(waitDuration, func() {
				refreshBlobToken(ctx, credInfo.OAuthTokenInfo, tokenCredential)
			})
		}

		credential = tokenCredential
	}

	return credential
}

func refreshBlobToken(ctx context.Context, tokenInfo common.OAuthTokenInfo, tokenCredential *azblob.TokenCredential) {
	oauthConfig, err := adal.NewOAuthConfig(tokenInfo.ActiveDirectoryEndpoint, tokenInfo.Tenant)
	if err != nil {
		fmt.Printf("fail to refresh token, due to error: %v\n", err)
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		common.ApplicationID,
		common.Resource,
		tokenInfo.Token)
	if err != nil {
		fmt.Printf("fail to refresh token, due to error: %v\n", err)
	}

	err = spt.RefreshWithContext(ctx)
	if err != nil {
		fmt.Printf("fail to refresh token, due to error: %v\n", err)
	}

	newToken := spt.Token()
	tokenCredential.SetToken(newToken.AccessToken)

	// For FE(commandline module), refreshing token until process exit.

	// Calculate wait duration, and schedule next refresh.
	waitDuration := newToken.Expires().Sub(time.Now().UTC()) - common.DefaultTokenExpiryWithinThreshold
	if waitDuration < time.Second {
		waitDuration = time.Nanosecond
	}

	_ = time.AfterFunc(waitDuration, func() {
		refreshBlobToken(ctx, common.OAuthTokenInfo{
			Token:                   newToken,
			Tenant:                  tokenInfo.Tenant,
			ActiveDirectoryEndpoint: tokenInfo.ActiveDirectoryEndpoint,
		}, tokenCredential)
	})
}

func createBlobFSPipeline(ctx context.Context, credInfo common.CredentialInfo) (pipeline.Pipeline, error) {
	credential := createBlobFSCredential(ctx, credInfo)

	return azbfs.NewPipeline(
		credential,
		azbfs.PipelineOptions{
			Retry: azbfs.RetryOptions{
				Policy:        azbfs.RetryPolicyExponential,
				MaxTries:      ste.UploadMaxTries,
				TryTimeout:    ste.UploadTryTimeout,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
		}), nil
}

func createBlobFSCredential(ctx context.Context, credInfo common.CredentialInfo) azbfs.Credential {
	switch credInfo.CredentialType {
	case common.ECredentialType.OAuthToken():
		if credInfo.OAuthTokenInfo.IsEmpty() {
			panic(fmt.Errorf("invalid state, cannot get valid token info for OAuthToken credential"))
		}

		// Create TokenCredential and set access token into it.
		tokenCredential := azbfs.NewTokenCredential(credInfo.OAuthTokenInfo.AccessToken)

		if credInfo.OAuthTokenInfo.IsExpired() || credInfo.OAuthTokenInfo.WillExpireIn(common.DefaultTokenExpiryWithinThreshold) {
			// If token is near expire, or already expired, refresh immediately before return token.
			refreshBlobFSToken(ctx, credInfo.OAuthTokenInfo, tokenCredential)
		} else {
			// Otherwise, calculate the next refresh time, and schedule the refresh.
			waitDuration := credInfo.OAuthTokenInfo.Expires().Sub(time.Now().UTC()) - common.DefaultTokenExpiryWithinThreshold

			//waitDuration = time.Second * 2 // TODO: Add mock testing
			if waitDuration < time.Second {
				waitDuration = time.Nanosecond
			}

			_ = time.AfterFunc(waitDuration, func() {
				refreshBlobFSToken(ctx, credInfo.OAuthTokenInfo, tokenCredential)
			})
		}

		return tokenCredential

	case common.ECredentialType.SharedKey():
		// Get the Account Name and Key variables from environment
		name := os.Getenv("ACCOUNT_NAME")
		key := os.Getenv("ACCOUNT_KEY")
		// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
		if name == "" || key == "" {
			panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before creating the blobfs SharedKey credential")
		}
		// create the shared key credentials
		return azbfs.NewSharedKeyCredential(name, key)

	default:
		panic(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}
}

func refreshBlobFSToken(ctx context.Context, tokenInfo common.OAuthTokenInfo, tokenCredential *azbfs.TokenCredential) {
	oauthConfig, err := adal.NewOAuthConfig(tokenInfo.ActiveDirectoryEndpoint, tokenInfo.Tenant)
	if err != nil {
		fmt.Printf("fail to refresh token, due to error: %v\n", err)
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		common.ApplicationID,
		common.Resource,
		tokenInfo.Token)
	if err != nil {
		fmt.Printf("fail to refresh token, due to error: %v\n", err)
	}

	err = spt.RefreshWithContext(ctx)
	if err != nil {
		fmt.Printf("fail to refresh token, due to error: %v\n", err)
	}

	newToken := spt.Token()
	tokenCredential.SetToken(newToken.AccessToken)

	// For FE(commandline module), refreshing token until process exit.

	// Calculate wait duration, and schedule next refresh.
	waitDuration := newToken.Expires().Sub(time.Now().UTC()) - common.DefaultTokenExpiryWithinThreshold
	if waitDuration < time.Second {
		waitDuration = time.Nanosecond
	}

	_ = time.AfterFunc(waitDuration, func() {
		refreshBlobFSToken(ctx, common.OAuthTokenInfo{
			Token:                   newToken,
			Tenant:                  tokenInfo.Tenant,
			ActiveDirectoryEndpoint: tokenInfo.ActiveDirectoryEndpoint,
		}, tokenCredential)
	})
}
