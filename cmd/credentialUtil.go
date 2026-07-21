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
	"net/http"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// GetCredentialManager is a variable for two reasons:
// 1) Mover may want to replace it to inject credentials of their own
// 2) To create a closure to hide "global" variables
var GetCredentialManager = func() func() cred.Manager {
	// Contain the "globals" within a closure so that we don't have access to them in the outside world.
	var (
		credManagerInstance cred.Manager
		credManagerOnce     sync.Once
	)

	return func() cred.Manager {
		credManagerOnce.Do(func() {
			if common.AzcopyJobPlanFolder == "" {
				panic("invalid state, AzcopyJobPlanFolder should not be an empty string")
			}

			keyrings := make([]cred.Keyring, 1, 4)
			keyrings[0] = cred.NewAutoLoginKeyring()

			if integration, err := cred.GetIntegrationKeyring(); err != nil {
				glcm.Warn(fmt.Sprintf("could not get integration keyring: %s", err))
			} else {
				keyrings = append(keyrings, integration)
			}
			if env, err := cred.GetEnvironmentKeyring(); err != nil {
				glcm.Warn(fmt.Sprintf("could not get environment keyring: %s", err))
			} else {
				keyrings = append(keyrings, env)
			}

			cacheName, ok := enum.EEnvironmentVariable.LoginCacheName().Lookup()

			if osKeyring, err := cred.GetOSKeyring(cred.GetOSKeyringOptions{
				// for DPAPI file path and RootKey, rely upon default values.
				OSKeyringCacheName: ternary.Iff(ok, &cacheName, nil),
			}); err != nil {
				glcm.Warn(fmt.Sprintf("could not get OS keyring: %s", err))
			} else {
				keyrings = append(keyrings, osKeyring)
			}

			credManagerInstance = cred.NewManager(keyrings...)
		})
		return credManagerInstance
	}
}()

type GetTargetCredInfoOptions struct {
	Context context.Context

	CanBePublic      bool
	SharedKeyAllowed bool

	PreferredTokenName string

	CpkOptions   common.CpkOptions
	TokenManager cred.Manager
}

type credInfoOptions struct {
	TokenCredential  azcore.TokenCredential
	S3CredentialInfo cred.S3CredentialInfo
}

func NewCredInfoRaw(credType enum.CredentialType, opts ...credInfoOptions) cred.CredentialInfo {
	info := cred.CredentialInfo{CredentialType: credType}
	if len(opts) > 0 {
		info.TokenCredential = cred.NewScopedToken(opts[0].TokenCredential, credType) // wrap our credential as a scoped token, so we have the appropriate scopes, and reauth powers ltaer
		info.S3CredentialInfo = opts[0].S3CredentialInfo
	}
	return info
}

func GetTargetCredInfo(resourceString common.ResourceString, location common.Location, opts GetTargetCredInfoOptions) (cred.CredentialInfo, error) {
	if forced := GetCredTypeFromEnvVar(); forced != enum.ECredentialType.Unknown() &&
		location != common.ELocation.S3() && location != common.ELocation.GCP() {
		return NewCredInfoRaw(forced), nil
	}

	if opts.Context == nil {
		opts.Context = context.TODO()
	}

	switch location {
	case common.ELocation.Blob():
		return getBlobCredInfo(resourceString, opts)
	case common.ELocation.BlobFS():
		return getBlobFSCredInfo(resourceString, opts)
	case common.ELocation.File():
		return getFileCredInfo(resourceString, opts)
	case common.ELocation.S3():
		return getS3CredInfo()
	case common.ELocation.GCP():
		return getGCPCredInfo()
	case common.ELocation.Local(), common.ELocation.Benchmark(), common.ELocation.None(), common.ELocation.Pipe():
		return getLocalCredInfo()
	}

	return cred.CredentialInfo{}, errors.New("unknown location: " + location.String())
}

func getBlobCredInfo(resourceString common.ResourceString, opts GetTargetCredInfoOptions) (cred.CredentialInfo, error) {
	return getBlobBasedCredInfo(resourceString, common.ELocation.Blob(), opts)
}

func getBlobFSCredInfo(resourceString common.ResourceString, opts GetTargetCredInfoOptions) (cred.CredentialInfo, error) {
	return getBlobBasedCredInfo(resourceString, common.ELocation.BlobFS(), opts)
}

func getBlobBasedCredInfo(resourceString common.ResourceString, location common.Location, opts GetTargetCredInfoOptions) (cred.CredentialInfo, error) {
	uri, _ := resourceString.FullURL()
	// normal accounts can't be prefixed like this (at least under normal blob endpoints!)
	// and it isn't allowed for storage accounts to have this naming scheme typically, anywho.
	// if someone is developing a service against an emulator or whatnot, and naming storage accounts this way, they are footgunning.
	isMdAccount := strings.HasPrefix(uri.Host, "md-")

	// Managed disk requires SAS bare minimum. No SAS, no managed disk.
	if isMdAccount && resourceString.SAS == "" {
		return NewCredInfoRaw(enum.ECredentialType.Unknown()), nil
	}

	// Handle all managed disk cases, to become DRY.
	if isMdAccount && mdAccountNeedsOAuth(opts.Context, uri.String(), opts.CpkOptions) {
		if opts.TokenManager == nil {
			return cred.CredentialInfo{}, common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
		}
		if _, err := opts.TokenManager.GetCredentials(opts.PreferredTokenName, nil); err != nil {
			return cred.CredentialInfo{}, common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
		}
		return NewCredInfoRaw(enum.ECredentialType.MDOAuthToken()), nil
	} else if isMdAccount {
		//
		return NewCredInfoRaw(enum.ECredentialType.Anonymous()), nil
	}

	// Managed disk, if it has a SAS, isn't always *just* SAS. it could need OAuth too.
	if resourceString.SAS != "" {
		return NewCredInfoRaw(enum.ECredentialType.Anonymous()), nil
	}

	// Test public access, if it's an option...
	if opts.CanBePublic {
		if isPublic(opts.Context, uri.String(), opts.CpkOptions) {
			return NewCredInfoRaw(enum.ECredentialType.Anonymous()), nil
		}
	}

	// If we have a token manager, see if we can fetch the token. If we can, we know what we're using!
	if opts.TokenManager != nil {
		if tc, err := opts.TokenManager.GetCredentials(opts.PreferredTokenName, nil); err == nil {
			return NewCredInfoRaw(enum.ECredentialType.OAuthToken(), credInfoOptions{TokenCredential: tc}), nil
		}
	}

	// BlobFS currently supports Shared key. Remove this piece of code once we deprecate that.
	if opts.SharedKeyAllowed && location == common.ELocation.BlobFS() {
		name := enum.EEnvironmentVariable.AccountName().Get()
		key := enum.EEnvironmentVariable.AccountKey().Get()
		if name != "" && key != "" {
			warnIfSharedKeyAuthForDatalake()
			return NewCredInfoRaw(enum.ECredentialType.SharedKey()), nil
		}
	}

	return NewCredInfoRaw(enum.ECredentialType.Unknown()), nil
}

func getFileCredInfo(resourceString common.ResourceString, opts GetTargetCredInfoOptions) (cred.CredentialInfo, error) {
	// Short-circuit for SAS
	if resourceString.SAS != "" {
		return NewCredInfoRaw(enum.ECredentialType.Anonymous()), nil
	}

	// Try to fetch OAuth if we can.
	if opts.TokenManager != nil {
		if tokenCred, err := opts.TokenManager.GetCredentials(opts.PreferredTokenName, nil); err == nil {
			return NewCredInfoRaw(enum.ECredentialType.OAuthToken(), credInfoOptions{
				TokenCredential: tokenCred,
			}), nil
		}
	}

	return NewCredInfoRaw(enum.ECredentialType.Unknown()), nil
}

func getS3CredInfo() (cred.CredentialInfo, error) {
	if !buildmode.IsMover {
		accessKeyID := enum.EEnvironmentVariable.AWSAccessKeyID().Get()
		secretAccessKey := enum.EEnvironmentVariable.AWSSecretAccessKey().Get()
		if accessKeyID == "" || secretAccessKey == "" {
			return NewCredInfoRaw(enum.ECredentialType.S3PublicBucket()), nil
		}
	}

	return NewCredInfoRaw(enum.ECredentialType.S3AccessKey()), nil
}

func getGCPCredInfo() (cred.CredentialInfo, error) {
	googleAppCredentials := enum.EEnvironmentVariable.GoogleAppCredentials().Get()
	if googleAppCredentials == "" {
		return cred.CredentialInfo{}, errors.New("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set before using GCP transfer feature")
	}
	return NewCredInfoRaw(enum.ECredentialType.GoogleAppCredentials()), nil
}

func getLocalCredInfo() (cred.CredentialInfo, error) {
	return NewCredInfoRaw(enum.ECredentialType.Anonymous()), nil
}

var sharedKeyDeprecation sync.Once
var sharedKeyDeprecationMessage = "*** WARNING *** shared key authentication for datalake is deprecated and will be removed in a future release. Please use shared access signature (SAS) or OAuth for authentication."

func warnIfSharedKeyAuthForDatalake() {
	sharedKeyDeprecation.Do(func() {
		glcm.Warn(sharedKeyDeprecationMessage)
		jobsAdmin.JobsAdmin.LogToJobLog(sharedKeyDeprecationMessage, common.LogWarning)
	})
}

var stashedEnvCredType = ""

// GetCredTypeFromEnvVar tries to get credential type from environment variable defined by envVarCredentialType.
func GetCredTypeFromEnvVar() enum.CredentialType {
	rawVal := stashedEnvCredType
	if stashedEnvCredType == "" {
		rawVal = enum.EEnvironmentVariable.CredentialType().Get()
		if rawVal == "" {
			return enum.ECredentialType.Unknown()
		}
		stashedEnvCredType = rawVal
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectedly.
	enum.EEnvironmentVariable.CredentialType().Get()

	// Try to get the value set.
	credType, ok := enum.ECredentialType.Parse(rawVal)
	if !ok {
		return enum.ECredentialType.Unknown()
	}

	return credType
}

// isPublic reports true if the Blob URL passed can be read without auth.
func isPublic(ctx context.Context, blobResourceURL string, cpkOptions common.CpkOptions) (isPublicResource bool) {
	bURLParts, err := blob.ParseURL(blobResourceURL)
	if err != nil {
		return false
	}

	if bURLParts.ContainerName == "" || strings.Contains(bURLParts.ContainerName, "*") {
		// Service level searches can't possibly be public.
		return false
	}

	// This request will not be logged. This can fail, and too many Cx do not like this.
	clientOptions := ste.NewClientOptions(policy.RetryOptions{
		MaxRetries:    ste.UploadMaxTries,
		TryTimeout:    ste.UploadTryTimeout,
		RetryDelay:    ste.UploadRetryDelay,
		MaxRetryDelay: ste.UploadMaxRetryDelay,
	}, policy.TelemetryOptions{
		ApplicationID: common.AddUserAgentPrefix(common.UserAgent),
	}, nil, ste.LogOptions{}, nil, nil)

	blobClient, _ := blob.NewClientWithNoCredential(bURLParts.String(), &blob.ClientOptions{ClientOptions: clientOptions})
	bURLParts.BlobName = ""
	bURLParts.Snapshot = ""
	bURLParts.VersionID = ""

	// Scenario 1: When resourceURL points to a container or a virtual directory
	// Check if the virtual directory is accessible by doing GetProperties on container.
	// Virtual directory can be public only when its parent container is public.
	containerClient, _ := container.NewClientWithNoCredential(bURLParts.String(), &container.ClientOptions{ClientOptions: clientOptions})
	if _, err := containerClient.GetProperties(ctx, nil); err == nil {
		return true
	}

	// Scenario 2: When resourceURL points to a blob
	if _, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{CPKInfo: cpkOptions.GetCPKInfo()}); err == nil {
		return true
	}

	return false
}

// mdAccountNeedsOAuth pings the passed in md account, and checks if we need additional token with Disk-socpe
func mdAccountNeedsOAuth(ctx context.Context, blobResourceURL string, cpkOptions common.CpkOptions) bool {
	// This request will not be logged. This can fail, and too many Cx do not like this.
	clientOptions := ste.NewClientOptions(policy.RetryOptions{
		MaxRetries:    ste.UploadMaxTries,
		TryTimeout:    ste.UploadTryTimeout,
		RetryDelay:    ste.UploadRetryDelay,
		MaxRetryDelay: ste.UploadMaxRetryDelay,
	}, policy.TelemetryOptions{
		ApplicationID: common.AddUserAgentPrefix(common.UserAgent),
	}, nil, ste.LogOptions{}, nil, nil)

	blobClient, _ := blob.NewClientWithNoCredential(blobResourceURL, &blob.ClientOptions{ClientOptions: clientOptions})
	_, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{CPKInfo: cpkOptions.GetCPKInfo()})
	if err == nil {
		return false
	}

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		if respErr.StatusCode == 401 || respErr.StatusCode == 403 { // *sometimes* the service can return 403s.
			challenge := respErr.RawResponse.Header.Get("WWW-Authenticate")
			if strings.Contains(challenge, cred.MDResource) {
				return true
			}
		}
	}
	return false
}

// ==============================================================================================
// pipeline factory methods
// ==============================================================================================
// createClientOptions creates generic client options which are required to create any
// client to interact with storage service. Default options are modified to suit azcopy.
// srcCred is required in cases where source is authenticated via oAuth for S2S transfers
func createClientOptions(logger common.ILoggerResetable, srcCred, targetCred azcore.TokenCredential) azcore.ClientOptions {
	logOptions := ste.LogOptions{}

	if logger != nil {
		logOptions.RequestLogOptions.SyslogDisabled = common.IsForceLoggingDisabled()
		logOptions.Log = logger.Log
		logOptions.ShouldLog = logger.ShouldLog
	}
	// Job-level/global client if available so we reuse connections and transports.
	client := common.GetGlobalHTTPClient(logger)

	return ste.NewClientOptions(
		policy.RetryOptions{
			MaxRetries:    ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
		policy.TelemetryOptions{
			ApplicationID: common.AddUserAgentPrefix(common.UserAgent),
		},
		client, /*Use common.NewTracingTransport(client, "createClientOptions", logger) for http.Trace*/
		logOptions,
		srcCred,
		targetCred)
}

const frontEndMaxIdleConnectionsPerHost = http.DefaultMaxIdleConnsPerHost
