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
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/minio/minio-go/pkg/s3utils"
)

const (
	TrustedSuffixesNameAAD      = "trusted-microsoft-suffixes"
	TrustedSuffixesAAD          = "*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net;*.storage.azure.net"
	sharedKeyDeprecationMessage = "*** WARNING *** shared key authentication for datalake is deprecated and will be removed in a future release. Please use shared access signature (SAS) or OAuth for authentication."
)

// TODO : reset per job
var sharedKeyDeprecation sync.Once
var autoOAuth sync.Once
var announceOAuthTokenOnce sync.Once
var authMessagesAlreadyLogged = &sync.Map{}
var stashedEnvCredType string

func warnIfSharedKeyAuthForDatalake() {
	sharedKeyDeprecation.Do(func() {
		common.GetLifecycleMgr().Warn(sharedKeyDeprecationMessage)
		common.LogToJobLogWithPrefix(sharedKeyDeprecationMessage, common.LogWarning)
	})
}

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
	case common.ECredentialType.Unknown(),
		common.ECredentialType.Anonymous():
		// these auth types don't pick up anything from environment vars, so they are not the focus of this routine
		return nil
	case common.ECredentialType.OAuthToken(),
		common.ECredentialType.MDOAuthToken(),
		common.ECredentialType.SharedKey():
		if resourceType == common.ELocation.Local() {
			return nil
		}
		// Files doesn't currently support OAuth, but it's a valid azure endpoint anyway, so it'll pass the check.
		if resourceType != common.ELocation.Blob() && resourceType != common.ELocation.BlobFS() && resourceType != common.ELocation.File() && resourceType != common.ELocation.FileNFS() {
			// There may be a reason for files->blob to specify this.\
			return fmt.Errorf("azure OAuth authentication to %s is not enabled in AzCopy", resourceType.String())
		}

		// these are Azure auth types, so make sure the resource is known to be in Azure
		domainSuffixes := getSuffixes(TrustedSuffixesAAD, extraSuffixesAAD)
		if host, ok := isResourceInSuffixList(domainSuffixes); !ok {
			return fmt.Errorf(
				"the URL requires authentication. If this URL is in fact an Azure service, you can enable Azure authentication to %s. "+
					"To enable, view the documentation for "+
					"the parameter --%s, by running 'AzCopy copy --help'. BUT if this URL is not an Azure service, do NOT enable Azure authentication to it. "+
					"Instead, see if the URL host supports authentication by way of a token that can be included in the URL's query string",
				// E.g. CDN apparently supports a non-SAS type of token as noted here: https://docs.microsoft.com/en-us/azure/cdn/cdn-token-auth#setting-up-token-authentication
				// Including such a token in the URL will cause AzCopy to see it as a "public" URL (since the URL on its own will pass
				// our "isPublic" access tests, which run before this routine).
				host, TrustedSuffixesNameAAD)
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
		common.LogToJobLogWithPrefix(message, common.LogInfo)
		common.GetLifecycleMgr().Info(message)
	}
}

// isPublic reports true if the Blob URL passed can be read without auth.
func isPublic(ctx context.Context, blobResourceURL string, cpkOptions common.CpkOptions) (isPublicResource bool, err error) {
	bURLParts, err := blob.ParseURL(blobResourceURL)
	if err != nil {
		return false, nil
	}

	if bURLParts.ContainerName == "" || strings.Contains(bURLParts.ContainerName, "*") {
		// Service level searches can't possibly be public.
		return false, nil
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
		return true, nil
	}

	// Scenario 2: When resourceURL points to a blob
	cpkInfo, err := cpkOptions.GetCPKInfo()
	if err != nil {
		return false, err
	}
	if _, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{CPKInfo: cpkInfo}); err == nil {
		return true, nil
	}

	return false, nil
}

// mdAccountNeedsOAuth pings the passed in md account, and checks if we need additional token with Disk-socpe
func mdAccountNeedsOAuth(ctx context.Context, blobResourceURL string, cpkOptions common.CpkOptions) (bool, error) {
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

	cpkInfo, err := cpkOptions.GetCPKInfo()
	if err == nil {
		_, err := blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{CPKInfo: cpkInfo})
		if err == nil {
			return false, nil
		}

		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			if respErr.StatusCode == 401 || respErr.StatusCode == 403 { // *sometimes* the service can return 403s.
				challenge := respErr.RawResponse.Header.Get("WWW-Authenticate")
				if strings.Contains(challenge, common.MDResource) {
					return true, nil
				}
			}
		}
		return false, nil
	} else {
		return false, err
	}
}

func GetCredentialTypeForLocation(ctx context.Context, location common.Location, resource common.ResourceString, isSource bool, uotm *common.UserOAuthTokenManager, cpkOptions common.CpkOptions) (credType common.CredentialType, isPublic bool, err error) {
	return doGetCredentialTypeForLocation(ctx, location, resource, isSource, GetCredTypeFromEnvVar, uotm, cpkOptions)
}

func doGetCredentialTypeForLocation(ctx context.Context, location common.Location, resource common.ResourceString, isSource bool, getForcedCredType func() common.CredentialType, uotm *common.UserOAuthTokenManager, cpkOptions common.CpkOptions) (credType common.CredentialType, public bool, err error) {
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
		if isSource && resource.SAS == "" {
			public, err = isPublic(ctx, uri.String(), cpkOptions)
			if err != nil {
				return common.ECredentialType.Unknown(), false, err
			}
			if public {
				credType = common.ECredentialType.Anonymous()
				return
			}
		}

		if strings.HasPrefix(uri.Host, "md-") {
			needsOauth, cpkError := mdAccountNeedsOAuth(ctx, uri.String(), cpkOptions)
			if cpkError != nil {
				return common.ECredentialType.Unknown(), false, cpkError
			}
			if needsOauth {
				var oAuthTokenExists bool
				oAuthTokenExists, err = uotm.OAuthTokenExists(&announceOAuthTokenOnce, &autoOAuth)
				if err != nil {
					return common.ECredentialType.Unknown(), false, err
				}
				if !oAuthTokenExists {
					return common.ECredentialType.Unknown(), false,
						common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
				}

				credType = common.ECredentialType.MDOAuthToken()
				return
			}
		}
	}

	if resource.SAS != "" {
		credType = common.ECredentialType.Anonymous()
		return
	}

	var oAuthTokenExists bool
	oAuthTokenExists, err = uotm.OAuthTokenExists(&announceOAuthTokenOnce, &autoOAuth)
	if err != nil {
		return common.ECredentialType.Unknown(), false, err
	}
	if oAuthTokenExists {
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
