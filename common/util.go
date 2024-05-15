package common

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalake "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
)

var AzcopyJobPlanFolder string
var AzcopyCurrentJobLogger ILoggerResetable

// isIPEndpointStyle checks if URL's host is IP, in this case the storage account endpoint will be composed as:
// http(s)://IP(:port)/storageaccount/container/...
// As url's Host property, host could be both host or host:port
func isIPEndpointStyle(host string) bool {
	if host == "" {
		return false
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// For IPv6, there could be case where SplitHostPort fails for cannot finding port.
	// In this case, eliminate the '[' and ']' in the URL.
	// For details about IPv6 URL, please refer to https://tools.ietf.org/html/rfc2732
	if host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	return net.ParseIP(host) != nil
}

// SplitContainerNameFromPath returns blob/file/dir path excluding container.
// Ex. For input https://account1.blob.core.windows.net/container1/a/b/c/d
// a/b/c/d is returned.
func SplitContainerNameFromPath(u string) (container string, filepath string, err error) {
	uri, err := url.Parse(u)
	if err != nil {
		return "", "", err
	}

	if uri.Path == "" {
		return "", "", nil
	}

	path := uri.Path
	if path[0] == '/' {
		path = path[1:]
	}

	if isIPEndpointStyle(uri.Host) {
		if accountEndIndex := strings.Index(path, "/"); accountEndIndex == -1 {
			// Slash not found; path has account name & no container name or blob
			return "", "", nil
		} else {
			path = path[accountEndIndex+1:] // path refers to portion after the account name now (container & blob names)
		}
	}

	containerEndIndex := strings.Index(path, "/") // Find the next slash (if it exists)
	if containerEndIndex == -1 {                  // Slash not found; path has container name & no blob name
		return path, "", nil
	}

	return path[:containerEndIndex], path[containerEndIndex+1:], nil
}

func VerifyIsURLResolvable(url_string string) error {
	/* This function is disabled. But we should still fix this after fixing the below stuff.
	 * We can take this up after migration to new SDK. The pipeline infra may not be same then.
	 * 1. At someplaces we use Blob SDK directly to create pipeline - ex getBlobCredentialType()
	 *    We should create pipeline through helper functions create<Blob/File/blobfs>pipeline, where we
	 *    handle errors appropriately.
	 * 2. We should either do a http.Get or net.Dial instead of lookIP. If we are behind a proxy, we may
	 *    not resolve this IP. #2144
	 * 3. DNS errors may by temporary, we should try for a minute before we give up.
	 */
	return nil
	/*
		url, err := url.Parse(url_string)
		if (err != nil) {
			return err
		}

		_, err = net.LookupIP(url.Host)
		return err
	*/
}

type FileClientOptions struct {
	AllowTrailingDot       bool
	AllowSourceTrailingDot bool
}

// GetServiceClientForLocation returns service client for the resourceURL. It strips the
// container and file related details before creating the client. locationSpecificOptions
// are required currently only for files.
func GetServiceClientForLocation(loc Location,
	resource ResourceString,
	credType CredentialType,
	cred azcore.TokenCredential,
	policyOptions *azcore.ClientOptions,
	locationSpecificOptions any,
) (*ServiceClient, error) {
	ret := &ServiceClient{}
	resourceURL, err := resource.String()
	if err != nil {
		return nil, fmt.Errorf("failed to get resource string: %w", err)
	}

	switch loc {
	case ELocation.BlobFS(), ELocation.Blob(): // Since we always may need to interact with DFS while working with Blob, we should just attach both.
		datalakeURLParts, err := azdatalake.ParseURL(resourceURL)
		if err != nil {
			return nil, err
		}
		datalakeURLParts.FileSystemName = ""
		datalakeURLParts.PathName = ""
		resourceURL = datalakeURLParts.String()

		var o *datalake.ClientOptions
		var dsc *datalake.Client
		if policyOptions != nil {
			o = &datalake.ClientOptions{ClientOptions: *policyOptions}
		}

		if credType.IsAzureOAuth() {
			dsc, err = datalake.NewClient(resourceURL, cred, o)
		} else if credType.IsSharedKey() {
			var sharedKeyCred *azdatalake.SharedKeyCredential
			sharedKeyCred, err = GetDatalakeSharedKeyCredential()
			if err != nil {
				return nil, err
			}
			dsc, err = datalake.NewClientWithSharedKeyCredential(resourceURL, sharedKeyCred, o)
		} else {
			dsc, err = datalake.NewClientWithNoCredential(resourceURL, o)
		}

		if err != nil {
			return nil, err
		}

		ret.dsc = dsc

		blobURLParts, err := blob.ParseURL(resourceURL)
		if err != nil {
			return nil, err
		}
		blobURLParts.ContainerName = ""
		blobURLParts.BlobName = ""
		// In case we are creating a blob client for a datalake target, correct the endpoint
		blobURLParts.Host = strings.Replace(blobURLParts.Host, ".dfs", ".blob", 1)
		resourceURL = blobURLParts.String()
		var bso *blobservice.ClientOptions
		var bsc *blobservice.Client
		if policyOptions != nil {
			bso = &blobservice.ClientOptions{ClientOptions: *policyOptions}
		}

		if credType.IsAzureOAuth() {
			bsc, err = blobservice.NewClient(resourceURL, cred, bso)
		} else if credType.IsSharedKey() {
			var sharedKeyCred *blob.SharedKeyCredential
			sharedKeyCred, err = GetBlobSharedKeyCredential()
			if err != nil {
				return nil, err
			}
			bsc, err = blobservice.NewClientWithSharedKeyCredential(resourceURL, sharedKeyCred, bso)
		} else {
			bsc, err = blobservice.NewClientWithNoCredential(resourceURL, bso)
		}

		if err != nil {
			return nil, err
		}

		ret.bsc = bsc
		return ret, nil

	case ELocation.File():
		fileURLParts, err := file.ParseURL(resourceURL)
		if err != nil {
			return nil, err
		}
		fileURLParts.ShareName = ""
		fileURLParts.DirectoryOrFilePath = ""
		resourceURL = fileURLParts.String()
		var o *fileservice.ClientOptions
		var fsc *fileservice.Client
		if policyOptions != nil {
			o = &fileservice.ClientOptions{ClientOptions: *policyOptions}
		}
		if locationSpecificOptions != nil {
			o.AllowTrailingDot = &locationSpecificOptions.(*FileClientOptions).AllowTrailingDot
			o.AllowSourceTrailingDot = &locationSpecificOptions.(*FileClientOptions).AllowSourceTrailingDot
		}

		if cred != nil {
			o.FileRequestIntent = to.Ptr(fileservice.ShareTokenIntentBackup)
			fsc, err = fileservice.NewClient(resourceURL, cred, o)
		} else {
			fsc, err = fileservice.NewClientWithNoCredential(resourceURL, o)
		}

		if err != nil {
			return nil, err
		}

		ret.fsc = fsc
		return ret, nil

	default:
		return nil, nil
	}
}

// NewScopedCredential takes in a credInfo object and returns ScopedCredential
// if credentialType is either MDOAuth or oAuth. For anything else,
// nil is returned
func NewScopedCredential(cred azcore.TokenCredential, credType CredentialType) *ScopedCredential {
	var scope string
	if !credType.IsAzureOAuth() {
		return nil
	} else if credType == ECredentialType.MDOAuthToken() {
		scope = ManagedDiskScope
	} else if credType == ECredentialType.OAuthToken() {
		scope = StorageScope
	}
	return &ScopedCredential{cred: cred, scopes: []string{scope}}
}

type ScopedCredential struct {
	cred   azcore.TokenCredential
	scopes []string
}

func (s *ScopedCredential) GetToken(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return s.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: s.scopes})
}

type ServiceClient struct {
	fsc *fileservice.Client
	bsc *blobservice.Client
	dsc *datalake.Client
}

func (s *ServiceClient) BlobServiceClient() (*blobservice.Client, error) {
	if s.bsc == nil {
		return nil, ErrInvalidClient("Blob Service")
	}
	return s.bsc, nil
}

func (s *ServiceClient) FileServiceClient() (*fileservice.Client, error) {
	if s.fsc == nil {
		return nil, ErrInvalidClient("File Service")
	}
	return s.fsc, nil
}

func (s *ServiceClient) DatalakeServiceClient() (*datalake.Client, error) {
	if s.dsc == nil {
		return nil, ErrInvalidClient("Datalake Service")
	}
	return s.dsc, nil
}

// This is currently used only in testcases
func NewServiceClient(bsc *blobservice.Client,
	fsc *fileservice.Client,
	dsc *datalake.Client) *ServiceClient {
	return &ServiceClient{
		bsc: bsc,
		fsc: fsc,
		dsc: dsc,
	}
}

// Metadata utility functions to work around GoLang's metadata capitalization

func TryAddMetadata(metadata Metadata, key, value string) {
	if _, ok := metadata[key]; ok {
		return // Don't overwrite the user's metadata
	}

	if key != "" {
		capitalizedKey := strings.ToUpper(string(key[0])) + key[1:]
		if _, ok := metadata[capitalizedKey]; ok {
			return
		}
	}

	v := value
	metadata[key] = &v
}

func TryReadMetadata(metadata Metadata, key string) (*string, bool) {
	if v, ok := metadata[key]; ok {
		return v, true
	}

	if key != "" {
		capitalizedKey := strings.ToUpper(string(key[0])) + key[1:]
		if v, ok := metadata[capitalizedKey]; ok {
			return v, true
		}
	}

	return nil, false
}

type FileClientStub interface {
	URL() string
}

// DoWithOverrideReadOnlyOnAzureFiles performs the given action,
// and forces it to happen even if the target is read only.
// NOTE that all SMB attributes (and other headers?) on the target will be lost,
// so only use this if you don't need them any more
// (e.g. you are about to delete the resource, or you are going to reset the attributes/headers)
func DoWithOverrideReadOnlyOnAzureFiles(ctx context.Context, action func() (interface{}, error), targetFileOrDir FileClientStub, enableForcing bool) error {
	// try the action
	_, err := action()

	if fileerror.HasCode(err, fileerror.ParentNotFound, fileerror.ShareNotFound) {
		return err
	}
	failedAsReadOnly := false
	if fileerror.HasCode(err, fileerror.ReadOnlyAttribute) {
		failedAsReadOnly = true
	}
	if !failedAsReadOnly {
		return err
	}

	// did fail as readonly, but forcing is not enabled
	if !enableForcing {
		return errors.New("target is readonly. To force the action to proceed, add --force-if-read-only to the command line")
	}

	// did fail as readonly, and forcing is enabled
	if f, ok := targetFileOrDir.(*file.Client); ok {
		h := file.HTTPHeaders{}
		_, err = f.SetHTTPHeaders(ctx, &file.SetHTTPHeadersOptions{
			HTTPHeaders: &h,
			SMBProperties: &file.SMBProperties{
				// clear the attributes
				Attributes: &file.NTFSFileAttributes{None: true},
			},
		})
	} else if d, ok := targetFileOrDir.(*directory.Client); ok {
		// this code path probably isn't used, since ReadOnly (in Windows file systems at least)
		// only applies to the files in a folder, not to the folder itself. But we'll leave the code here, for now.
		_, err = d.SetProperties(ctx, &directory.SetPropertiesOptions{
			FileSMBProperties: &file.SMBProperties{
				// clear the attributes
				Attributes: &file.NTFSFileAttributes{None: true},
			},
		})
	} else {
		err = errors.New("cannot remove read-only attribute from unknown target type")
	}
	if err != nil {
		return err
	}

	// retry the action
	_, err = action()
	return err
}
