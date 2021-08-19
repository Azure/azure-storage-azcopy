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

	"github.com/minio/minio-go/pkg/s3utils"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

var once sync.Once
var autoOAuth sync.Once

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

/*
 * GetInstanceOAuthTokenInfo returns OAuth token, obtained by auto-login,
 * for current instance of AzCopy.
 */
func GetOAuthTokenManagerInstance() (*common.UserOAuthTokenManager, error) {
	var err error
	autoOAuth.Do(func() {
		var lca loginCmdArgs
		if glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AutoLoginType()) == "" {
			err = errors.New("no login type specified")
			return
		}

		if tenantID := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.TenantID()); tenantID != "" {
			lca.tenantID = tenantID
		}

		if endpoint := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AADEndpoint()); endpoint != "" {
			lca.aadEndpoint = endpoint
		}

		// Fill up lca
		switch glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AutoLoginType()) {
		case "SPN":
			lca.applicationID = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ApplicationID())
			lca.certPath = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePath())
			lca.certPass = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.CertificatePassword())
			lca.clientSecret = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ClientSecret())
			lca.servicePrincipal = true

		case "MSI":
			lca.identityClientID = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ManagedIdentityClientID())
			lca.identityObjectID = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ManagedIdentityObjectID())
			lca.identityResourceID = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ManagedIdentityResourceString())
			lca.identity = true

		case "DEVICE":
			lca.identity = false
		}

		lca.persistToken = false
		err = lca.process()
	})

	if err != nil {
		return nil, err
	}

	return GetUserOAuthTokenManagerInstance(), nil
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
// The implementation logic follows above rule, and adjusts sequence to save web request(for verifying public resource).
func getBlobCredentialType(ctx context.Context, blobResourceURL string, canBePublic bool, standaloneSAS bool, cpkOptions common.CpkOptions) (common.CredentialType, bool, error) {
	resourceURL, err := url.Parse(blobResourceURL)

	if err != nil {
		return common.ECredentialType.Unknown(), false, errors.New("provided blob resource string is not in URL format")
	}

	sas := azblob.NewBlobURLParts(*resourceURL).SAS

	// If SAS existed, return anonymous credential type.
	if isSASExisted := sas.Signature() != ""; isSASExisted || standaloneSAS {
		return common.ECredentialType.Anonymous(), false, nil
	}

	checkPublic := func() (isPublicResource bool) {
		if !canBePublic { // Cannot possibly be public - like say a destination EP
			return false
		}
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
				RequestLog: azblob.RequestLogOptions{
					SyslogDisabled: common.IsForceLoggingDisabled(),
				},
			})

		isContainer := copyHandlerUtil{}.urlIsContainerOrVirtualDirectory(resourceURL)
		isPublicResource = false

		// Scenario 1: When resourceURL points to a container
		// Scenario 2: When resourceURL points to a virtual directory.
		// Check if the virtual directory is accessible by doing GetProperties on container.
		// Virtual directory can be accessed/scanned only when its parent container is public.
		bURLParts := azblob.NewBlobURLParts(*resourceURL)
		bURLParts.BlobName = ""
		containerURL := azblob.NewContainerURL(bURLParts.URL(), p)

		if bURLParts.ContainerName == "" || strings.Contains(bURLParts.ContainerName, "*") {
			// Service level searches can't possibly be public.
			return false
		}

		if _, err := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{}); err == nil {
			return true
		}

		if !isContainer {
			clientProvidedKey := azblob.ClientProvidedKeyOptions{}
			if cpkOptions.IsSourceEncrypted {
				clientProvidedKey = common.GetClientProvidedKey(cpkOptions)
			}
			// Scenario 3: When resourceURL points to a blob
			blobURL := azblob.NewBlobURL(*resourceURL, p)
			if _, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, clientProvidedKey); err == nil {
				return true
			}
		}

		return
	}

	// If SAS token doesn't exist, it could be using OAuth token or the resource is public.
	if !oAuthTokenExists() { // no oauth token found, then directly return anonymous credential
		isPublicResource := checkPublic()

		// No forms of auth are present.no SAS token or OAuth token is present and the resource is not public
		if !isPublicResource {
			return common.ECredentialType.Unknown(), isPublicResource,
				common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
		}

		return common.ECredentialType.Anonymous(), isPublicResource, nil
	} else if !canBePublic { // oauth token found, if it can not be public resource, return token credential
		return common.ECredentialType.OAuthToken(), false, nil
	} else { // check if it's public resource, and return credential type correspondingly
		// If has cached token, and no SAS token provided, it could be a public blob resource.
		isPublicResource := checkPublic()

		if isPublicResource {
			return common.ECredentialType.Anonymous(), true, nil
		} else {
			return common.ECredentialType.OAuthToken(), false, nil
		}
	}
}

// getBlobFSCredentialType is used to get BlobFS's credential type when user wishes to use OAuth session mode.
// The verification logic follows following rules:
// 1. Check if there is a SAS query appended to the URL
// 2. If there is cached session OAuth token, indicating using token credential.
// 3. If there is OAuth token info passed from env var, indicating using token credential. (Note: this is only for testing)
// 4. Otherwise use shared key.
func getBlobFSCredentialType(ctx context.Context, blobResourceURL string, standaloneSAS bool) (common.CredentialType, error) {
	resourceURL, err := url.Parse(blobResourceURL)
	if err != nil {
		return common.ECredentialType.Unknown(), err
	}

	//Give preference to explicitly supplied SAS tokens
	sas := azbfs.NewBfsURLParts(*resourceURL).SAS

	if isSASExisted := sas.Signature() != ""; isSASExisted || standaloneSAS {
		return common.ECredentialType.Anonymous(), nil
	}

	if oAuthTokenExists() {
		return common.ECredentialType.OAuthToken(), nil
	}

	name := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AccountName())
	key := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AccountKey())
	if name != "" && key != "" { // TODO: To remove, use for internal testing, SharedKey should not be supported from commandline
		return common.ECredentialType.SharedKey(), nil
	} else {
		return common.ECredentialType.Unknown(),
			common.NewAzError(common.EAzError.LoginCredMissing(), "OAuth token, SAS token, or shared key should be provided for Blob FS")
	}
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

var stashedEnvCredType = ""

// GetCredTypeFromEnvVar tries to get credential type from environment variable defined by envVarCredentialType.
func GetCredTypeFromEnvVar() common.CredentialType {
	rawVal := stashedEnvCredType
	if stashedEnvCredType == "" {
		rawVal = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.CredentialType())
		if rawVal == "" {
			return common.ECredentialType.Unknown()
		}
		stashedEnvCredType = rawVal
	}

	// Remove the env var after successfully fetching once,
	// in case of env var is further spreading into child processes unexpectedly.
	glcm.ClearEnvironmentVariable(common.EEnvironmentVariable.CredentialType())

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
		host := "<unparseable url>"
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
				"s3 authentication to %s is not currently suported in AzCopy", host)
		}
	case common.ECredentialType.GoogleAppCredentials():
		if resourceType != common.ELocation.GCP() {
			return fmt.Errorf("Google Application Credentials to %s is not valid", resourceType.String())
		}

		host := "<unparseable url>"
		u, err := url.Parse(resource)
		if err == nil {
			host = u.Host
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
	}
	message := fmt.Sprintf("Authenticating to %s using %s", resource, name)
	if _, exists := authMessagesAlreadyLogged.Load(message); !exists {
		authMessagesAlreadyLogged.Store(message, struct{}{}) // dedup because source is auth'd by both enumerator and STE
		if ste.JobsAdmin != nil {
			ste.JobsAdmin.LogToJobLog(message, pipeline.LogInfo)
		}
		glcm.Info(message)
	}
}

var authMessagesAlreadyLogged = &sync.Map{}

func getCredentialTypeForLocation(ctx context.Context, location common.Location, resource, resourceSAS string, isSource bool, cpkOptions common.CpkOptions) (credType common.CredentialType, isPublic bool, err error) {
	return doGetCredentialTypeForLocation(ctx, location, resource, resourceSAS, isSource, GetCredTypeFromEnvVar, cpkOptions)
}

func doGetCredentialTypeForLocation(ctx context.Context, location common.Location, resource, resourceSAS string, isSource bool, getForcedCredType func() common.CredentialType, cpkOptions common.CpkOptions) (credType common.CredentialType, isPublic bool, err error) {
	if resourceSAS != "" {
		credType = common.ECredentialType.Anonymous()
	} else if credType = getForcedCredType(); credType == common.ECredentialType.Unknown() || location == common.ELocation.S3() || location == common.ELocation.GCP() {
		switch location {
		case common.ELocation.Local(), common.ELocation.Benchmark():
			credType = common.ECredentialType.Anonymous()
		case common.ELocation.Blob():
			credType, isPublic, err = getBlobCredentialType(ctx, resource, isSource, resourceSAS != "", cpkOptions)
			if azErr, ok := err.(common.AzError); ok && azErr.Equals(common.EAzError.LoginCredMissing()) {
				_, autoLoginErr := GetOAuthTokenManagerInstance()
				if autoLoginErr == nil {
					err = nil // Autologin succeeded, reset original error
					credType, isPublic = common.ECredentialType.OAuthToken(), false
				}
			}
			if err != nil {
				return common.ECredentialType.Unknown(), false, err
			}
		case common.ELocation.File():
			if credType, err = getAzureFileCredentialType(); err != nil {
				return common.ECredentialType.Unknown(), false, err
			}
		case common.ELocation.BlobFS():
			credType, err = getBlobFSCredentialType(ctx, resource, resourceSAS != "")
			if azErr, ok := err.(common.AzError); ok && azErr.Equals(common.EAzError.LoginCredMissing()) {
				_, autoLoginErr := GetOAuthTokenManagerInstance()
				if autoLoginErr == nil {
					err = nil // Autologin succeeded, reset original error
					credType, isPublic = common.ECredentialType.OAuthToken(), false
				}
			}
			if err != nil {
				return common.ECredentialType.Unknown(), false, err
			}
		case common.ELocation.S3():
			accessKeyID := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AWSAccessKeyID())
			secretAccessKey := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.AWSSecretAccessKey())
			if accessKeyID == "" || secretAccessKey == "" {
				return common.ECredentialType.Unknown(), false, errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set before creating the S3 AccessKey credential")
			}
			credType = common.ECredentialType.S3AccessKey()
		case common.ELocation.GCP():
			googleAppCredentials := glcm.GetEnvironmentVariable(common.EEnvironmentVariable.GoogleAppCredentials())
			if googleAppCredentials == "" {
				return common.ECredentialType.Unknown(), false, errors.New("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set before using GCP transfer feature")
			}
			credType = common.ECredentialType.GoogleAppCredentials()
		}
	}

	if err = checkAuthSafeForTarget(credType, resource, cmdLineExtraSuffixesAAD, location); err != nil {
		return common.ECredentialType.Unknown(), false, err
	}

	logAuthType(credType, location, isSource)
	return
}

func GetCredentialInfoForLocation(ctx context.Context, location common.Location, resource, resourceSAS string, isSource bool, cpkOptions common.CpkOptions) (credInfo common.CredentialInfo, isPublic bool, err error) {

	// get the type
	credInfo.CredentialType, isPublic, err = getCredentialTypeForLocation(ctx, location, resource, resourceSAS, isSource, cpkOptions)

	// flesh out the rest of the fields, for those types that require it
	if credInfo.CredentialType == common.ECredentialType.OAuthToken() {
		uotm := GetUserOAuthTokenManagerInstance()

		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return credInfo, false, err
		} else {
			credInfo.OAuthTokenInfo = *tokenInfo
		}
	} else if credInfo.CredentialType == common.ECredentialType.S3AccessKey() {
		// nothing to do here. The extra fields for S3 are fleshed out at the time
		// we make the S3Client
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
		credType, _, err = getCredentialTypeForLocation(ctx, raw.fromTo.To(), raw.destination, raw.destinationSAS, false, common.CpkOptions{})
	case raw.fromTo == common.EFromTo.BlobTrash() ||
		raw.fromTo == common.EFromTo.BlobFSTrash() ||
		raw.fromTo == common.EFromTo.FileTrash():
		// For to Trash direction, use source as resource URL
		// Also, by setting isSource=false we inform getCredentialTypeForLocation() that resource
		// being deleted cannot be public.
		credType, _, err = getCredentialTypeForLocation(ctx, raw.fromTo.From(), raw.source, raw.sourceSAS, false, cpkOptions)
	case raw.fromTo.From().IsRemote() && raw.fromTo.To().IsLocal():
		// we authenticate to the source.
		credType, _, err = getCredentialTypeForLocation(ctx, raw.fromTo.From(), raw.source, raw.sourceSAS, true, cpkOptions)
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
func createBlobPipeline(ctx context.Context, credInfo common.CredentialInfo, logLevel pipeline.LogLevel) (pipeline.Pipeline, error) {
	credential := common.CreateBlobCredential(ctx, credInfo, common.CredentialOpOptions{
		//LogInfo:  glcm.Info, //Comment out for debugging
		LogError: glcm.Info,
	})

	logOption := pipeline.LogOptions{}
	if azcopyScanningLogger != nil {
		logOption = pipeline.LogOptions{
			Log:       azcopyScanningLogger.Log,
			ShouldLog: func(level pipeline.LogLevel) bool { return level <= logLevel },
		}
	}

	return ste.NewBlobPipeline(
		credential,
		azblob.PipelineOptions{
			Telemetry: azblob.TelemetryOptions{
				Value: glcm.AddUserAgentPrefix(common.UserAgent),
			},
			Log: logOption,
		},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
		nil,
		ste.NewAzcopyHTTPClient(frontEndMaxIdleConnectionsPerHost),
		nil, // we don't gather network stats on the credential pipeline
	), nil
}

const frontEndMaxIdleConnectionsPerHost = http.DefaultMaxIdleConnsPerHost

func createBlobFSPipeline(ctx context.Context, credInfo common.CredentialInfo, logLevel pipeline.LogLevel) (pipeline.Pipeline, error) {
	credential := common.CreateBlobFSCredential(ctx, credInfo, common.CredentialOpOptions{
		//LogInfo:  glcm.Info, //Comment out for debugging
		LogError: glcm.Info,
	})

	logOption := pipeline.LogOptions{}
	if azcopyScanningLogger != nil {
		logOption = pipeline.LogOptions{
			Log:       azcopyScanningLogger.Log,
			ShouldLog: func(level pipeline.LogLevel) bool { return level <= logLevel },
		}
	}

	return ste.NewBlobFSPipeline(
		credential,
		azbfs.PipelineOptions{
			Telemetry: azbfs.TelemetryOptions{
				Value: glcm.AddUserAgentPrefix(common.UserAgent),
			},
			Log: logOption,
		},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
		nil,
		ste.NewAzcopyHTTPClient(frontEndMaxIdleConnectionsPerHost),
		nil, // we don't gather network stats on the credential pipeline
	), nil
}

// TODO note: ctx and credInfo are ignored at the moment because we only support SAS for Azure File
func createFilePipeline(ctx context.Context, credInfo common.CredentialInfo, logLevel pipeline.LogLevel) (pipeline.Pipeline, error) {
	logOption := pipeline.LogOptions{}
	if azcopyScanningLogger != nil {
		logOption = pipeline.LogOptions{
			Log:       azcopyScanningLogger.Log,
			ShouldLog: func(level pipeline.LogLevel) bool { return level <= logLevel },
		}
	}

	return ste.NewFilePipeline(
		azfile.NewAnonymousCredential(),
		azfile.PipelineOptions{
			Telemetry: azfile.TelemetryOptions{
				Value: glcm.AddUserAgentPrefix(common.UserAgent),
			},
			Log: logOption,
		},
		azfile.RetryOptions{
			Policy:        azfile.RetryPolicyExponential,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
		nil,
		ste.NewAzcopyHTTPClient(frontEndMaxIdleConnectionsPerHost),
		nil, // we don't gather network stats on the credential pipeline
	), nil
}
