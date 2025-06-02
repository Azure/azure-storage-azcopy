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
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/minio/minio-go/pkg/s3utils"
)

var once sync.Once
var autoOAuth sync.Once

var sharedKeyDeprecation sync.Once
var sharedKeyDeprecationMessage = "*** WARNING *** shared key authentication for datalake is deprecated and will be removed in a future release. Please use shared access signature (SAS) or OAuth for authentication."

func warnIfSharedKeyAuthForDatalake() {
	sharedKeyDeprecation.Do(func() {
		glcm.Warn(sharedKeyDeprecationMessage)
		jobsAdmin.JobsAdmin.LogToJobLog(sharedKeyDeprecationMessage, common.LogWarning)
	})
}

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

/*
 * GetInstanceOAuthTokenInfo returns OAuth token, obtained by auto-login,
 * for current instance of AzCopy.
 */
func GetOAuthTokenManagerInstance() (*common.UserOAuthTokenManager, error) {
	var err error
	autoOAuth.Do(func() {
		var lca loginCmdArgs
		autoLoginType := strings.ToLower(common.GetEnvironmentVariable(common.EEnvironmentVariable.AutoLoginType()))
		if autoLoginType == "" {
			glcm.Info("Autologin not specified.")
			return
		}

		if tenantID := common.GetEnvironmentVariable(common.EEnvironmentVariable.TenantID()); tenantID != "" {
			lca.tenantID = tenantID
		}

		if endpoint := common.GetEnvironmentVariable(common.EEnvironmentVariable.AADEndpoint()); endpoint != "" {
			lca.aadEndpoint = endpoint
		}

		// Fill up lca
		lca.loginType = autoLoginType
		switch autoLoginType {
		case common.EAutoLoginType.SPN().String():
			lca.applicationID = common.GetEnvironmentVariable(common.EEnvironmentVariable.ApplicationID())
			lca.certPath = common.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePath())
			lca.certPass = common.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePassword())
			lca.clientSecret = common.GetEnvironmentVariable(common.EEnvironmentVariable.ClientSecret())
		case common.EAutoLoginType.MSI().String():
			lca.identityClientID = common.GetEnvironmentVariable(common.EEnvironmentVariable.ManagedIdentityClientID())
			lca.identityObjectID = common.GetEnvironmentVariable(common.EEnvironmentVariable.ManagedIdentityObjectID())
			lca.identityResourceID = common.GetEnvironmentVariable(common.EEnvironmentVariable.ManagedIdentityResourceString())
		case common.EAutoLoginType.Device().String():
		case common.EAutoLoginType.AzCLI().String():
		case common.EAutoLoginType.PsCred().String():
		case common.EAutoLoginType.Workload().String():
		default:
			glcm.Error("Invalid Auto-login type specified: " + autoLoginType)
			return
		}

		lca.persistToken = false
		if err = lca.process(); err != nil {
			glcm.Error(fmt.Sprintf("Failed to perform Auto-login: %v.", err.Error()))
		}
	})

	if err != nil {
		return nil, err
	}

	return GetUserOAuthTokenManagerInstance(), nil
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

type rawFromToInfo struct {
	fromTo              common.FromTo
	source, destination common.ResourceString
}

const trustedSuffixesNameAAD = "trusted-microsoft-suffixes"
const trustedSuffixesAAD = "*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net;*.storage.azure.net"

// checkAuthSafeForTarget checks our "implicit" auth types (those that pick up creds from the environment
// or a prior login) to make sure they are only being used in places where we know those auth types are safe.
// This prevents, for example, us accidentally sending OAuth creds to some place they don't belong
func checkAuthSafeForTarget(ct common.CredentialType, resource, extraSuffixesAAD string, resourceType common.Location) error {

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
		
		// Strip port from host for suffix comparison (fixes issue #2792)
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}

		for _, s := range suffixes {
			s = strings.Trim(s, " *") // trim *.foo to .foo
			s = strings.ToLower(s)
			if strings.HasSuffix(host, s) {
				return u.Host, true // Return original host for error messages
			}
		}
		return u.Host, false
	}

	switch ct {
	case common.ECredentialType.Unknown(),
		common.ECredentialType.Anonymous():
		// these auth types don't pick up anything from environment vars, so they are not the focus of this routine
		return nil
	case common.ECredentialType.OAuthToken(),
		common.ECredentialType.MDOAuthToken(),
		common.ECredentialType.SharedKey():
		// Files doesn't currently support OAuth, but it's a valid azure endpoint anyway, so it'll pass the check.
		if resourceType != common.ELocation.Blob() && resourceType != common.ELocation.BlobFS() && resourceType != common.ELocation.File() {
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

	case common.ECredentialType.S3AccessKey():
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
				ok = err == nil && s3utils.IsAmazonEndpoint(*u)
			}
		}

		if !ok {
			return fmt.Errorf(
				"s3 authentication to %s is not currently supported in AzCopy", host)
		}
	case common.ECredentialType.GoogleAppCredentials():
		if resourceType != common.ELocation.GCP() {
			return fmt.Errorf("Google Application Credentials to %s is not valid", resourceType.String())
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

func logAuthType(ct common.CredentialType, location common.Location, isSource bool) {
	if location == common.ELocation.Unknown() {
		return // nothing to log
	} else if location.IsLocal() {
		return // don't log local ones, no point
	} else if ct == common.ECredentialType.Anonymous() {
		return // don't log these either (too cluttered and auth type is obvious from the URL)
	}

	resource := "destination"
	if isSource {
		resource = "source"
	}
	name := ct.String()
	if ct == common.ECredentialType.OAuthToken() {
		name = "Azure AD" // clarify the name to something users will recognize
	} else if ct == common.ECredentialType.MDOAuthToken() {
		name = "Azure AD (Managed Disk)"
	}
	message := fmt.Sprintf("Authenticating to %s using %s", resource, name)
	if ct == common.ECredentialType.Unknown() && location.IsAzure() {
		message += ", Please authenticate using Microsoft Entra ID (https://aka.ms/AzCopy/AuthZ), use AzCopy login, or append a SAS token to your Azure URL."
	}
	if _, exists := authMessagesAlreadyLogged.Load(message); !exists {
		authMessagesAlreadyLogged.Store(message, struct{}{}) // dedup because source is auth'd by both enumerator and STE
		if jobsAdmin.JobsAdmin != nil {
			jobsAdmin.JobsAdmin.LogToJobLog(message, common.LogInfo)
		}
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
			if strings.Contains(challenge, common.MDResource) {
				return true
			}
		}
	}
	return false
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

	defer func() {
		logAuthType(credType, location, isSource)
	}()

	// caution: If auth-type is unsafe, below defer statement will change the return value credType
	defer func() {
		if err != nil {
			return
		}

		if err = checkAuthSafeForTarget(credType, resource.Value, cmdLineExtraSuffixesAAD, location); err != nil {
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

	// BlobFS currently supports Shared key. Remove this piece of code, once
	// we deprecate that.
	if location == common.ELocation.BlobFS() {
		name := common.GetEnvironmentVariable(common.EEnvironmentVariable.AccountName())
		key := common.GetEnvironmentVariable(common.EEnvironmentVariable.AccountKey())
		if name != "" && key != "" { // TODO: To remove, use for internal testing, SharedKey should not be supported from commandline
			credType = common.ECredentialType.SharedKey()
			warnIfSharedKeyAuthForDatalake()
		}
	}

	// We may not always use the OAuth token on Managed Disks. As such, we should change to the type indicating the potential for use.
	// if mdAccount && credType == common.ECredentialType.OAuthToken() {
	// 	credType = common.ECredentialType.MDOAuthToken()
	// }
	return
}

func GetCredentialInfoForLocation(ctx context.Context, location common.Location, resource common.ResourceString, isSource bool, cpkOptions common.CpkOptions) (credInfo common.CredentialInfo, isPublic bool, err error) {

	// get the type
	credInfo.CredentialType, isPublic, err = getCredentialTypeForLocation(ctx, location, resource, isSource, cpkOptions)

	// flesh out the rest of the fields, for those types that require it
	if credInfo.CredentialType.IsAzureOAuth() {
		uotm := GetUserOAuthTokenManagerInstance()

		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return credInfo, false, err
		} else {
			credInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	return
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

// ==============================================================================================
// pipeline factory methods
// ==============================================================================================
// createClientOptions creates generic client options which are required to create any
// client to interact with storage service. Default options are modified to suit azcopy.
// srcCred is required in cases where source is authenticated via oAuth for S2S transfers
func createClientOptions(logger common.ILoggerResetable, srcCred *common.ScopedToken, reauthCred *common.ScopedAuthenticator) azcore.ClientOptions {
	logOptions := ste.LogOptions{}

	if logger != nil {
		logOptions.RequestLogOptions.SyslogDisabled = common.IsForceLoggingDisabled()
		logOptions.Log = logger.Log
		logOptions.ShouldLog = logger.ShouldLog
	}
	return ste.NewClientOptions(policy.RetryOptions{
		MaxRetries:    ste.UploadMaxTries,
		TryTimeout:    ste.UploadTryTimeout,
		RetryDelay:    ste.UploadRetryDelay,
		MaxRetryDelay: ste.UploadMaxRetryDelay,
	}, policy.TelemetryOptions{
		ApplicationID: common.AddUserAgentPrefix(common.UserAgent),
	}, ste.NewAzcopyHTTPClient(frontEndMaxIdleConnectionsPerHost), logOptions, srcCred, reauthCred)
}

const frontEndMaxIdleConnectionsPerHost = http.DefaultMaxIdleConnsPerHost
