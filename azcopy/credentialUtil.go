package azcopy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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
