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

// This file contains credential utils used only in cmd module.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

var once sync.Once

// only one UserOAuthTokenManager should exists in azcopy-v2 process in cmd(FE) module for current user.
// (given appAppPathFolder is mapped to current user)
var currentUserOAuthTokenManager *common.UserOAuthTokenManager

const oauthLoginSessionCacheKeyName = "AzCopyOAuthTokenCache"
const oauthLoginSessionCacheServiceName = "AzCopyV10"
const oauthLoginSessionCacheAccountName = "AzCopyOAuthTokenCache"

// GetUserOAuthTokenManagerInstance gets or creates OAuthTokenManager for current user.
// Note: Currently, only support to have TokenManager for one user mapping to one tenantID.
func GetUserOAuthTokenManagerInstance() *common.UserOAuthTokenManager {
	once.Do(func() {
		if azcopyAppPathFolder == "" {
			panic("invalid state, azcopyAppPathFolder should be initialized by root")
		}
		currentUserOAuthTokenManager = common.NewUserOAuthTokenManagerInstance(common.CredCacheOptions{
			DPAPIFilePath: azcopyAppPathFolder,
			KeyName:       oauthLoginSessionCacheKeyName,
			ServiceName:   oauthLoginSessionCacheServiceName,
			AccountName:   oauthLoginSessionCacheAccountName,
		})
	})

	return currentUserOAuthTokenManager
}

// ==============================================================================================
// Get credential type methods
// ==============================================================================================

// getBlobCredentialType is used to get Blob's credential type when user wishes to use OAuth session mode.
// The verification logic follows following rules:
// 1. For source or dest url, if the url contains SAS or SAS is provided standalone, indicating using anonymous credential(SAS).
// 2. If the blob URL can be public access resource, and validated as public resource, indicating using anonymous credential(public resource).
// 3. If there is cached OAuth token, indicating using token credential.
// 4. If there is OAuth token info passed from env var, indicating using token credential. (Note: this is only for testing)
// 5. Otherwise use anonymous credential.
// The implementaion logic follows above rule, and adjusts sequence to save web request(for verifying public resource).
func getBlobCredentialType(ctx context.Context, blobResourceURL string, canBePublic bool, standaloneSAS bool) (common.CredentialType, error) {
	resourceURL, err := url.Parse(blobResourceURL)

	if err != nil {
		return common.ECredentialType.Unknown(), errors.New("provided blob resource string is not in URL format")
	}

	sas := azblob.NewBlobURLParts(*resourceURL).SAS

	// If SAS existed, return anonymous credential type.
	if isSASExisted := sas.Signature() != ""; isSASExisted || standaloneSAS {
		return common.ECredentialType.Anonymous(), nil
	}

	// If SAS token doesn't exist, it could be using OAuth token or the resource is public.
	if !oAuthTokenExists() { // no oauth token found, then directly return anonymous credential
		return common.ECredentialType.Anonymous(), nil
	} else if !canBePublic { // oauth token found, if it can not be public resource, return token credential
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
	if oAuthTokenExists() {
		return common.ECredentialType.OAuthToken(), nil
	}

	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name != "" && key != "" { // TODO: To remove, use for internal testing, SharedKey should not be supported from commandline
		return common.ECredentialType.SharedKey(), nil
	} else {
		return common.ECredentialType.Unknown(), errors.New("OAuth token or shared key should be provided for Blob FS")
	}
}

func oAuthTokenExists() (oauthTokenExists bool) {
	// Note: Environment variable for OAuth token should only be used in testing, or the case user clearly now how to protect
	// the tokens
	if common.EnvVarOAuthTokenInfoExists() {
		glcm.Info(fmt.Sprintf("%v is set.", common.EnvVarOAuthTokenInfo)) // Log the case when env var is set, as it's rare case.
		oauthTokenExists = true
	}

	uotm := GetUserOAuthTokenManagerInstance()
	if hasCachedToken, err := uotm.HasCachedToken(); hasCachedToken {
		oauthTokenExists = true
	} else if err != nil {
		// Log the error if fail to get cached token, as these are unhandled errors, and should not influence the logic flow.
		// Uncomment for debugging.
		// glcm.Info(fmt.Sprintf("No cached token found, %v", err))
	}

	return
}

// getAzureFileCredentialType is used to get Azure file's credential type
func getAzureFileCredentialType() (common.CredentialType, error) {
	// Azure file only support anonymous credential currently.
	return common.ECredentialType.Anonymous(), nil
}

// envVarCredentialType used for passing credential type into AzCopy through environment variable.
// Note: This is only used for internal integration, and not encouraged to be used directly.
const envVarCredentialType = "AZCOPY_CRED_TYPE"

// GetCredTypeFromEnvVar tries to get credential type from environment variable defined by envVarCredentialType.
func GetCredTypeFromEnvVar() common.CredentialType {
	rawVal := os.Getenv(envVarCredentialType)
	if rawVal == "" {
		return common.ECredentialType.Unknown()
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectly.
	os.Setenv(envVarCredentialType, "")

	// Try to get the value set.
	var credType common.CredentialType
	if err := credType.Parse(rawVal); err != nil {
		return common.ECredentialType.Unknown()
	}

	return credType
}

type rawFromToInfo struct {
	fromTo                    common.FromTo
	source, destination       string
	sourceSAS, destinationSAS string // Standalone SAS which might be provided
}

// getCredentialType checks user provided info, and gets the proper credential type
// for current command.
func getCredentialType(ctx context.Context, raw rawFromToInfo) (credentialType common.CredentialType, err error) {
	// In the integration case, AzCopy directly use caller provided credential type if specified and not Unknown.
	if credType := GetCredTypeFromEnvVar(); credType != common.ECredentialType.Unknown() {
		return credType, nil
	}

	// Could be using oauth session mode or non-oauth scenario which uses SAS authentication or public endpoint,
	// verify credential type with cached token info, src or dest resource URL.
	switch raw.fromTo {
	case common.EFromTo.BlobBlob(), common.EFromTo.FileBlob(), common.EFromTo.S3Blob():
		// For blob/file to blob copy, calculate credential type for destination (currently only support StageBlockFromURL)
		// If the traditional approach(download+upload) need be supported, credential type should be calculated for both src and dest.
		fallthrough
	case common.EFromTo.LocalBlob(), common.EFromTo.PipeBlob():
		if credentialType, err = getBlobCredentialType(ctx, raw.destination, false, raw.destinationSAS != ""); err != nil {
			return common.ECredentialType.Unknown(), err
		}
	case common.EFromTo.BlobTrash():
		// For BlobTrash direction, use source as resource URL, and it should not be public access resource.
		if credentialType, err = getBlobCredentialType(ctx, raw.source, false, raw.sourceSAS != ""); err != nil {
			return common.ECredentialType.Unknown(), err
		}
	case common.EFromTo.BlobLocal(), common.EFromTo.BlobPipe():
		if credentialType, err = getBlobCredentialType(ctx, raw.source, true, raw.sourceSAS != ""); err != nil {
			return common.ECredentialType.Unknown(), err
		}
	case common.EFromTo.LocalBlobFS(), common.EFromTo.BlobFSLocal():
		if credentialType, err = getBlobFSCredentialType(); err != nil {
			return common.ECredentialType.Unknown(), err
		}
	case common.EFromTo.LocalFile(), common.EFromTo.FileLocal(), common.EFromTo.FileTrash(), common.EFromTo.FilePipe(), common.EFromTo.PipeFile():
		if credentialType, err = getAzureFileCredentialType(); err != nil {
			return common.ECredentialType.Unknown(), err
		}
	default:
		credentialType = common.ECredentialType.Anonymous()
		// Log the FromTo types which getCredentialType hasn't solved, in case of miss-use.
		glcm.Info(fmt.Sprintf("Use anonymous credential by default for from-to '%v'", raw.fromTo))
	}

	return credentialType, nil
}

// ==============================================================================================
// pipeline factory methods
// ==============================================================================================
func createBlobPipeline(ctx context.Context, credInfo common.CredentialInfo) (pipeline.Pipeline, error) {
	credential := common.CreateBlobCredential(ctx, credInfo, common.CredentialOpOptions{
		//LogInfo:  glcm.Info, //Comment out for debugging
		LogError: glcm.Info,
	})

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
		nil,
		ste.NewAzcopyHTTPClient()), nil
}

func createBlobFSPipeline(ctx context.Context, credInfo common.CredentialInfo) (pipeline.Pipeline, error) {
	credential := common.CreateBlobFSCredential(ctx, credInfo, common.CredentialOpOptions{
		//LogInfo:  glcm.Info, //Comment out for debugging
		LogError: glcm.Info,
	})

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

// TODO note: ctx and credInfo are ignored at the moment because we only support SAS for Azure File
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
