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
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/minio/minio-go/v7/pkg/s3utils"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
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
	case common.ELocation.File(), common.ELocation.FileNFS():
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
		common.LogToJobLogWithPrefix(sharedKeyDeprecationMessage, common.LogWarning)
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

type rawFromToInfo struct {
	fromTo              common.FromTo
	source, destination common.ResourceString
}

// checkAuthSafeForTarget checks our "implicit" auth types (those that pick up creds from the environment
// or a prior login) to make sure they are only being used in places where we know those auth types are safe.
// This prevents, for example, us accidentally sending OAuth creds to some place they don't belong
func checkAuthSafeForTarget(ct enum.CredentialType, resource, extraSuffixesAAD string, resourceType common.Location) error {

	getSuffixes := func(list string, extras string) []string {
		extras = strings.Trim(extras, " ")
		if extras != "" {
			list += ";" + extras
		}
		return strings.Split(list, ";")
	}

	isResourceInSuffixList := func(suffixes []string) (string, bool) {
		u, err := url.Parse(resource)
		if err != nil {
			return "<unparsable>", false
		}
		host := strings.ToLower(u.Host)

		for _, s := range suffixes {
			s = strings.Trim(s, " *") // trim *.foo to .foo
			s = strings.ToLower(s)
			if strings.HasSuffix(host, s) {
				return host, true
			}
		}
		return host, false
	}

	switch ct {
	case enum.ECredentialType.Unknown(),
		enum.ECredentialType.Anonymous():
		// these auth types don't pick up anything from environment vars, so they are not the focus of this routine
		return nil
	case enum.ECredentialType.OAuthToken(),
		enum.ECredentialType.MDOAuthToken(),
		enum.ECredentialType.SharedKey():
		// Files doesn't currently support OAuth, but it's a valid azure endpoint anyway, so it'll pass the check.
		if resourceType != common.ELocation.Blob() && resourceType != common.ELocation.BlobFS() && !resourceType.IsFile() {
			// There may be a reason for files->blob to specify this.
			if resourceType == common.ELocation.Local() {
				return nil
			}

			return fmt.Errorf("azure OAuth authentication to %s is not enabled in AzCopy", resourceType.String())
		}

		// these are Azure auth types, so make sure the resource is known to be in Azure
		domainSuffixes := getSuffixes(trustedSuffixesAAD, extraSuffixesAAD)
		if host, ok := isResourceInSuffixList(domainSuffixes); !ok {
			return fmt.Errorf(
				"the URL requires authentication. If this URL is in fact an Azure service, you can enable Azure authentication to %s. "+
					"To enable, view the documentation for "+
					"the parameter --%s, by running 'AzCopy copy --help'. BUT if this URL is not an Azure service, do NOT enable Azure authentication to it. "+
					"Instead, see if the URL host supports authentication by way of a token that can be included in the URL's query string",
				// E.g. CDN apparently supports a non-SAS type of token as noted here: https://docs.microsoft.com/en-us/azure/cdn/cdn-token-auth#setting-up-token-authentication
				// Including such a token in the URL will cause AzCopy to see it as a "public" URL (since the URL on its own will pass
				// our "isPublic" access tests, which run before this routine).
				host, trustedSuffixesNameAAD)
		}

	case enum.ECredentialType.S3AccessKey():
		if resourceType != common.ELocation.S3() {
			//noinspection ALL
			return fmt.Errorf("S3 access key authentication to %s is not enabled in AzCopy", resourceType.String())
		}

		// just check with minio. No need to have our own list of S3 domains, since minio effectively
		// has that list already, we can't talk to anything outside that list because minio won't let us,
		// and the parsing of s3 URL is non-trivial.  E.g. can't just look for the ending since
		// something like https://someApi.execute-api.someRegion.amazonaws.com is AWS but is a customer-
		// written code, not S3.
		ok := false
		host := "<unparsable url>"
		u, err := url.Parse(resource)
		if err == nil {
			host = u.Host
			parts, err := common.NewS3URLParts(*u) // strip any leading bucket name from URL, to get an endpoint we can pass to s3utils
			if err == nil {
				u, err := url.Parse("https://" + parts.Endpoint)
				ok = err == nil && (s3utils.IsAmazonEndpoint(*u) || strings.HasSuffix(u.Host, common.GetS3CompatibleSuffix()))
			}
		}

		if !ok {
			return fmt.Errorf(
				"s3 authentication to %s is not currently supported in AzCopy", host)
		}
	case enum.ECredentialType.GoogleAppCredentials():
		if resourceType != common.ELocation.GCP() {
			return fmt.Errorf("google application credentials to %s is not valid", resourceType.String())
		}

		u, err := url.Parse(resource)
		if err == nil {
			host := u.Host
			_, err := common.NewGCPURLParts(*u)
			if err != nil {
				return fmt.Errorf("GCP authentication to %s is not currently supported", host)
			}
		}
	default:
		panic("unknown credential type")
	}

	return nil
}

func logAuthType(ct enum.CredentialType, location common.Location, isSource bool) {
	if location == common.ELocation.Unknown() {
		return // nothing to log
	} else if location.IsLocal() {
		return // don't log local ones, no point
	} else if ct == enum.ECredentialType.Anonymous() {
		return // don't log these either (too cluttered and auth type is obvious from the URL)
	}

	resource := "destination"
	if isSource {
		resource = "source"
	}
	name := ct.String()
	if ct == enum.ECredentialType.OAuthToken() {
		name = "Azure AD" // clarify the name to something users will recognize
	} else if ct == enum.ECredentialType.MDOAuthToken() {
		name = "Azure AD (Managed Disk)"
	}
	message := fmt.Sprintf("Authenticating to %s using %s", resource, name)
	if ct == enum.ECredentialType.Unknown() && location.IsAzure() {
		message += ", Please authenticate using Microsoft Entra ID (https://aka.ms/AzCopy/AuthZ), use AzCopy login, or append a SAS token to your Azure URL."
	}
	if _, exists := authMessagesAlreadyLogged.Load(message); !exists {
		authMessagesAlreadyLogged.Store(message, struct{}{}) // dedup because source is auth'd by both enumerator and STE
		common.LogToJobLogWithPrefix(message, common.LogInfo)
		glcm.Info(message)
	}
}

var authMessagesAlreadyLogged = &sync.Map{}

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
