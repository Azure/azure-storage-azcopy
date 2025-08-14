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
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const (
	oauthLoginSessionCacheKeyName     = "AzCopyOAuthTokenCache"
	oauthLoginSessionCacheServiceName = "AzCopyV10"
	oauthLoginSessionCacheAccountName = "AzCopyOAuthTokenCache"
)

// TODO: (gapra) Should we make currentUserOAuthTokenManager a variable in the client?

// only one UserOAuthTokenManager should exist in azcopy process for current user.
// (a given AzcopyJobPlanFolder is mapped to current user)
var oauthTokenManagerOnce sync.Once
var currentUserOAuthTokenManager *common.UserOAuthTokenManager

// GetUserOAuthTokenManagerInstance gets or creates OAuthTokenManager for current user.
// Note: Currently, only support to have TokenManager for one user mapping to one tenantID.
func GetUserOAuthTokenManagerInstance() *common.UserOAuthTokenManager {
	oauthTokenManagerOnce.Do(func() {
		if common.AzcopyJobPlanFolder == "" {
			panic("invalid state, AzcopyJobPlanFolder should not be an empty string")
		}
		cacheName := common.GetEnvironmentVariable(common.EEnvironmentVariable.LoginCacheName())

		currentUserOAuthTokenManager = common.NewUserOAuthTokenManagerInstance(common.CredCacheOptions{
			DPAPIFilePath: common.AzcopyJobPlanFolder,
			KeyName:       common.Iff(cacheName != "", cacheName, oauthLoginSessionCacheKeyName),
			ServiceName:   oauthLoginSessionCacheServiceName,
			AccountName:   common.Iff(cacheName != "", cacheName, oauthLoginSessionCacheAccountName),
		})
	})

	return currentUserOAuthTokenManager
}

var stashedEnvCredType = ""

// GetCredTypeFromEnvVar tries to get credential type from environment variable defined by envVarCredentialType.
func GetCredTypeFromEnvVar() common.CredentialType {
	rawVal := stashedEnvCredType
	if stashedEnvCredType == "" {
		rawVal = common.GetEnvironmentVariable(common.EEnvironmentVariable.CredentialType())
		if rawVal == "" {
			return common.ECredentialType.Unknown()
		}
		stashedEnvCredType = rawVal
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectedly.
	common.ClearEnvironmentVariable(common.EEnvironmentVariable.CredentialType())

	// Try to get the value set.
	var credType common.CredentialType
	if err := credType.Parse(rawVal); err != nil {
		return common.ECredentialType.Unknown()
	}

	return credType
}

func getCredentialTypeForLocation(ctx context.Context, location common.Location, resource common.ResourceString, isSource bool, cpkOptions common.CpkOptions) (credType common.CredentialType, isPublic bool, err error) {
	return doGetCredentialTypeForLocation(ctx, location, resource, isSource, GetCredTypeFromEnvVar, cpkOptions)
}

func doGetCredentialTypeForLocation(ctx context.Context, location common.Location, resource common.ResourceString, isSource bool, getForcedCredType func() common.CredentialType, cpkOptions common.CpkOptions) (credType common.CredentialType, public bool, err error) {
	public = false
	err = nil

	switch location {
	case common.ELocation.Local(), common.ELocation.Benchmark(), common.ELocation.None(), common.ELocation.Pipe():
		return common.ECredentialType.Anonymous(), false, nil
	}

	// caution: If auth-type is unsafe, below defer statement will change the return value credType
	defer func() {
		if err != nil {
			return
		}

		if err = checkAuthSafeForTarget(credType, resource.Value, TrustedSuffixes, location); err != nil {
			credType = common.ECredentialType.Unknown()
			public = false
		}
	}()

	if getForcedCredType() != common.ECredentialType.Unknown() &&
		location != common.ELocation.S3() && location != common.ELocation.GCP() {
		credType = getForcedCredType()
		return
	}

	if location == common.ELocation.S3() {
		accessKeyID := common.GetEnvironmentVariable(common.EEnvironmentVariable.AWSAccessKeyID())
		secretAccessKey := common.GetEnvironmentVariable(common.EEnvironmentVariable.AWSSecretAccessKey())
		if accessKeyID == "" || secretAccessKey == "" {
			credType = common.ECredentialType.S3PublicBucket()
			public = true
			return
		}

		credType = common.ECredentialType.S3AccessKey()
		return
	}

	if location == common.ELocation.GCP() {
		googleAppCredentials := common.GetEnvironmentVariable(common.EEnvironmentVariable.GoogleAppCredentials())
		if googleAppCredentials == "" {
			return common.ECredentialType.Unknown(), false, errors.New("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set before using GCP transfer feature")
		}
		credType = common.ECredentialType.GoogleAppCredentials()
		return
	}

	// Special blob destinations - public and MD account needing oAuth
	if location == common.ELocation.Blob() {
		uri, _ := resource.FullURL()
		if isSource && resource.SAS == "" && isPublic(ctx, uri.String(), cpkOptions) {
			credType = common.ECredentialType.Anonymous()
			public = true
			return
		}

		if strings.HasPrefix(uri.Host, "md-") && mdAccountNeedsOAuth(ctx, uri.String(), cpkOptions) {
			if !oAuthTokenExists() {
				return common.ECredentialType.Unknown(), false,
					common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
			}

			credType = common.ECredentialType.MDOAuthToken()
			return
		}
	}

	if resource.SAS != "" {
		credType = common.ECredentialType.Anonymous()
		return
	}

	if oAuthTokenExists() {
		credType = common.ECredentialType.OAuthToken()
		return
	}
	return
}

var announceOAuthTokenOnce sync.Once

func oAuthTokenExists() (oauthTokenExists bool) {
	// Note: Environment variable for OAuth token should only be used in testing, or the case user clearly now how to protect
	// the tokens
	if common.EnvVarOAuthTokenInfoExists() {
		announceOAuthTokenOnce.Do(
			func() {
				glcm.Info(fmt.Sprintf("%v is set.", common.EnvVarOAuthTokenInfo)) // Log the case when env var is set, as it's rare case.
			},
		)
		oauthTokenExists = true
	}

	uotm, err := GetOAuthTokenManagerInstance()
	if err != nil {
		oauthTokenExists = false
		return
	}

	if hasCachedToken, err := uotm.HasCachedToken(); hasCachedToken {
		oauthTokenExists = true
	} else if err != nil { //nolint:staticcheck
		// Log the error if fail to get cached token, as these are unhandled errors, and should not influence the logic flow.
		// Uncomment for debugging.
		// glcm.Info(fmt.Sprintf("No cached token found, %v", err))
	}

	return
}

type rawFromToInfo struct {
	fromTo              common.FromTo
	source, destination common.ResourceString
}

// getCredentialType checks user provided info, and gets the proper credential type
// for current command.
// TODO: consider replace with calls to getCredentialInfoForLocation
// (right now, we have tweaked this to be a wrapper for that function, but really should remove this one totally)
func getCredentialType(ctx context.Context, raw rawFromToInfo, cpkOptions common.CpkOptions) (credType common.CredentialType, err error) {

	switch {
	case raw.fromTo.To().IsRemote():
		// we authenticate to the destination. Source is assumed to be SAS, or public, or a local resource
		credType, _, err = getCredentialTypeForLocation(ctx, raw.fromTo.To(), raw.destination, false, common.CpkOptions{})
	case raw.fromTo == common.EFromTo.BlobTrash() ||
		raw.fromTo == common.EFromTo.BlobFSTrash() ||
		raw.fromTo == common.EFromTo.FileTrash():
		// For to Trash direction, use source as resource URL
		// Also, by setting isSource=false we inform getCredentialTypeForLocation() that resource
		// being deleted cannot be public.
		credType, _, err = getCredentialTypeForLocation(ctx, raw.fromTo.From(), raw.source, false, cpkOptions)
	case raw.fromTo.From().IsRemote() && raw.fromTo.To().IsLocal():
		// we authenticate to the source.
		credType, _, err = getCredentialTypeForLocation(ctx, raw.fromTo.From(), raw.source, true, cpkOptions)
	default:
		credType = common.ECredentialType.Anonymous()
		// Log the FromTo types which getCredentialType hasn't solved, in case of miss-use.
		glcm.Info(fmt.Sprintf("Use anonymous credential by default for from-to '%v'", raw.fromTo))
	}

	return
}
