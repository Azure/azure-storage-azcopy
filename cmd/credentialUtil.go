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
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
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
// 3. If there is cached OAuth token, indicating using token credential.
// 4. If there is OAuth token info passed from env var, indicating using token credential. (Note: this is only for testing)
// 5. Otherwise use anonymous credential.
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

	// Following are the cases: Use oauth token, public source blob or default anonymous credential.
	uotm := GetUserOAuthTokenManagerInstance()
	hasCachedToken, err := uotm.HasCachedToken()
	if err != nil {
		// Log the error if fail to get cached token, as these are unhandled errors, and should not influence the logic flow.
		glcm.Info(fmt.Sprintf("No cached token found, %v", err))
	}

	// Note: Environment variable for OAuth token should only be used in testing, or the case user clearly now how to protect
	// the tokens.
	hasEnvVarOAuthTokenInfo := common.EnvVarOAuthTokenInfoExists()
	if hasEnvVarOAuthTokenInfo {
		glcm.Info(fmt.Sprintf("%v is set.", common.EnvVarOAuthTokenInfo)) // Log the case when env var is set, as it's rare case.
	}

	if !hasCachedToken && !hasEnvVarOAuthTokenInfo { // no oauth token found, then directly return anonymous credential
		return common.ECredentialType.Anonymous(), nil
	} else if !isSource { // oauth token found, if it's destination, then it should not be public resource, return token credential
		return common.ECredentialType.OAuthToken(), nil
	} else { // check if it's public resource, and return credential type correspondingly
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
// 2. If there is OAuth token info passed from env var, indicating using token credential. (Note: this is only for testing)
// 3. Otherwise use shared key.
func getBlobFSCredentialType() (common.CredentialType, error) {
	// Note: Environment variable for OAuth token should only be used in testing, or the case user clearly now how to protect
	// the tokens
	if common.EnvVarOAuthTokenInfoExists() {
		glcm.Info(fmt.Sprintf("%v is set.", common.EnvVarOAuthTokenInfo)) // Log the case when env var is set, as it's rare case.
		return common.ECredentialType.OAuthToken(), nil
	}

	uotm := GetUserOAuthTokenManagerInstance()
	hasCachedToken, err := uotm.HasCachedToken()
	if err != nil {
		// Log the error if fail to get cached token, as these are unhandled errors, and should not influence the logic flow.
		glcm.Info(fmt.Sprintf("No cached token found, %v", err))
	}

	if hasCachedToken {
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

	return ste.NewBlobPipeline(
		credential,
		azblob.PipelineOptions{
			Telemetry: azblob.TelemetryOptions{
				Value: common.UserAgent,
			},
		},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
		nil), nil
}

func createBlobCredential(ctx context.Context, credInfo common.CredentialInfo) azblob.Credential {
	credential := azblob.NewAnonymousCredential()

	if credInfo.CredentialType == common.ECredentialType.OAuthToken() {
		if credInfo.OAuthTokenInfo.IsEmpty() {
			panic(fmt.Errorf("invalid state, cannot get valid token info for OAuthToken credential"))
		}

		// Create TokenCredential with refresher.
		return azblob.NewTokenCredential(
			credInfo.OAuthTokenInfo.AccessToken,
			func(credential azblob.TokenCredential) time.Duration {
				return refreshBlobToken(ctx, credInfo.OAuthTokenInfo, credential)
			})
	}

	return credential
}

func refreshBlobToken(ctx context.Context, tokenInfo common.OAuthTokenInfo, tokenCredential azblob.TokenCredential) time.Duration {
	oauthConfig, err := adal.NewOAuthConfig(tokenInfo.ActiveDirectoryEndpoint, tokenInfo.Tenant)
	if err != nil {
		glcm.Info(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		common.ApplicationID,
		common.Resource,
		tokenInfo.Token)
	if err != nil {
		glcm.Info(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}

	err = spt.RefreshWithContext(ctx)
	if err != nil {
		glcm.Info(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}

	newToken := spt.Token()
	tokenCredential.SetToken(newToken.AccessToken)

	// Calculate wait duration, and schedule next refresh.
	waitDuration := newToken.Expires().Sub(time.Now().UTC()) - common.DefaultTokenExpiryWithinThreshold
	if waitDuration < time.Second {
		waitDuration = time.Nanosecond
	}

	if common.GlobalTestOAuthInjection.DoTokenRefreshInjection {
		waitDuration = common.GlobalTestOAuthInjection.TokenRefreshDuration
	}

	return waitDuration
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
			Telemetry: azbfs.TelemetryOptions{
				Value: common.UserAgent,
			},
		}), nil
}

func createBlobFSCredential(ctx context.Context, credInfo common.CredentialInfo) azbfs.Credential {
	switch credInfo.CredentialType {
	case common.ECredentialType.OAuthToken():
		if credInfo.OAuthTokenInfo.IsEmpty() {
			panic(fmt.Errorf("invalid state, cannot get valid token info for OAuthToken credential"))
		}

		// Create TokenCredential with refresher.
		return azbfs.NewTokenCredential(
			credInfo.OAuthTokenInfo.AccessToken,
			func(credential azbfs.TokenCredential) time.Duration {
				return refreshBlobFSToken(ctx, credInfo.OAuthTokenInfo, credential)
			})

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

func createFilePipeline(ctx context.Context, credInfo common.CredentialInfo) (pipeline.Pipeline, error) {
	return azfile.NewPipeline(
		azfile.NewAnonymousCredential(),
		azfile.PipelineOptions{
			Retry: azfile.RetryOptions{
				Policy:        azfile.RetryPolicyExponential,
				MaxTries:      ste.UploadMaxTries,
				TryTimeout:    ste.UploadTryTimeout,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
			Telemetry: azfile.TelemetryOptions{
				Value: common.UserAgent,
			},
		}), nil
}

func refreshBlobFSToken(ctx context.Context, tokenInfo common.OAuthTokenInfo, tokenCredential azbfs.TokenCredential) time.Duration {
	oauthConfig, err := adal.NewOAuthConfig(tokenInfo.ActiveDirectoryEndpoint, tokenInfo.Tenant)
	if err != nil {
		glcm.Info(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(
		*oauthConfig,
		common.ApplicationID,
		common.Resource,
		tokenInfo.Token)
	if err != nil {
		glcm.Info(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}

	err = spt.RefreshWithContext(ctx)
	if err != nil {
		glcm.Info(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}

	newToken := spt.Token()
	tokenCredential.SetToken(newToken.AccessToken)

	// Calculate wait duration, and schedule next refresh.
	waitDuration := newToken.Expires().Sub(time.Now().UTC()) - common.DefaultTokenExpiryWithinThreshold
	if waitDuration < time.Second {
		waitDuration = time.Nanosecond
	}
	if common.GlobalTestOAuthInjection.DoTokenRefreshInjection {
		waitDuration = common.GlobalTestOAuthInjection.TokenRefreshDuration
	}

	return waitDuration
}
