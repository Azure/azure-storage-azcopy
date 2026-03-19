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
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"

	"errors"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

// holds raw input from user
type rawMakeCmdArgs struct {
	resourceToCreate string
	quota            uint32
}

// parse raw input
func (raw rawMakeCmdArgs) cook() (cookedMakeCmdArgs, error) {
	parsedURL, err := url.Parse(raw.resourceToCreate)
	if err != nil {
		return cookedMakeCmdArgs{}, err
	}

	if strings.Count(parsedURL.Path, "/") > 1 {
		return cookedMakeCmdArgs{}, fmt.Errorf("please provide a valid top-level(ex: File System or Container) resource URL")
	}

	// resourceLocation could be unknown at this stage, it will be handled by the caller
	return cookedMakeCmdArgs{
		resourceURL:      *parsedURL,
		resourceLocation: azcopy.InferArgumentLocation(raw.resourceToCreate),
		quota:            int32(raw.quota),
	}, nil
}

// holds processed/actionable args
type cookedMakeCmdArgs struct {
	resourceURL      url.URL
	resourceLocation common.Location
	quota            int32 // quota is in GB
}

func (cookedArgs cookedMakeCmdArgs) process() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	resourceStringParts, err := traverser.SplitResourceString(cookedArgs.resourceURL.String(), cookedArgs.resourceLocation)
	if err != nil {
		return err
	}

	if err := common.VerifyIsURLResolvable(resourceStringParts.Value); cookedArgs.resourceLocation.IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve target: %w", err)
	}

	credentialInfo, _, err := GetCredentialInfoForLocation(ctx, cookedArgs.resourceLocation, resourceStringParts, false, common.CpkOptions{})
	if err != nil {
		return err
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := credentialInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	// Note : trailing dot is only applicable to file operations anyway, so setting this to false
	options := traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, nil, reauthTok)
	resourceURL := cookedArgs.resourceURL.String()
	cred := credentialInfo.OAuthTokenInfo.TokenCredential

	switch cookedArgs.resourceLocation {
	case common.ELocation.BlobFS():
		var filesystemClient *filesystem.Client
		if credentialInfo.CredentialType.IsAzureOAuth() {
			filesystemClient, err = filesystem.NewClient(resourceURL, cred, &filesystem.ClientOptions{ClientOptions: options})
		} else if credentialInfo.CredentialType.IsSharedKey() {
			var sharedKeyCred *azdatalake.SharedKeyCredential
			sharedKeyCred, err = common.GetDatalakeSharedKeyCredential()
			if err != nil {
				return err
			}
			filesystemClient, err = filesystem.NewClientWithSharedKeyCredential(resourceURL, sharedKeyCred, &filesystem.ClientOptions{ClientOptions: options})
		} else {
			filesystemClient, err = filesystem.NewClientWithNoCredential(resourceURL, &filesystem.ClientOptions{ClientOptions: options})
		}
		if err != nil {
			return err
		}

		if _, err = filesystemClient.Create(ctx, nil); err != nil {
			// print a nicer error message if container already exists
			if datalakeerror.HasCode(err, datalakeerror.FileSystemAlreadyExists) {
				return fmt.Errorf("the filesystem already exists")
			} else if datalakeerror.HasCode(err, datalakeerror.ResourceNotFound) {
				return fmt.Errorf("please specify a valid filesystem URL with corresponding credentials")
			}
			// print the ugly error if unexpected
			return err
		}
	case common.ELocation.Blob():
		// TODO : Ensure it is a container URL here and fail early?
		var containerClient *container.Client
		if credentialInfo.CredentialType.IsAzureOAuth() {
			containerClient, err = container.NewClient(resourceURL, cred, &container.ClientOptions{ClientOptions: options})
		} else {
			containerClient, err = container.NewClientWithNoCredential(resourceURL, &container.ClientOptions{ClientOptions: options})
		}
		if err != nil {
			return err
		}
		if _, err = containerClient.Create(ctx, nil); err != nil {
			// print a nicer error message if container already exists
			if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
				return fmt.Errorf("the container already exists")
			} else if bloberror.HasCode(err, bloberror.ResourceNotFound) {
				return fmt.Errorf("please specify a valid container URL with corresponding credentials")
			}
			// print the ugly error if unexpected
			return err
		}
	case common.ELocation.File(), common.ELocation.FileNFS():
		var shareClient *share.Client
		shareClient, err = share.NewClientWithNoCredential(resourceURL, &share.ClientOptions{ClientOptions: options})
		if err != nil {
			return err
		}
		quota := &cookedArgs.quota
		if quota != nil && *quota == 0 {
			quota = nil
		}
		if _, err = shareClient.Create(ctx, &share.CreateOptions{Quota: quota}); err != nil {
			// print a nicer error message if share already exists
			if fileerror.HasCode(err, fileerror.ShareAlreadyExists) {
				return fmt.Errorf("the file share already exists")
			} else if fileerror.HasCode(err, fileerror.ResourceNotFound) {
				return fmt.Errorf("please specify a valid share URL with corresponding credentials")
			}
			// print the ugly error if unexpected
			return err
		}
	default:
		return fmt.Errorf("operation not supported, cannot create resource %s type at the moment", cookedArgs.resourceURL.String())
	}
	return nil
}

func init() {
	rawArgs := rawMakeCmdArgs{}

	// makeCmd represents the mkdir command, but targets the service side
	makeCmd := &cobra.Command{
		Use:        "make [resourceURL]",
		Aliases:    []string{"mk", "mkdir"},
		SuggestFor: []string{"mak", "makeCmd"},
		Short:      makeCmdShortDescription,
		Long:       makeCmdLongDescription,
		Example:    makeCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			// verify that there is exactly one argument
			if len(args) != 1 {
				return errors.New("please provide the resource URL as the only argument")
			}

			rawArgs.resourceToCreate = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cookedArgs, err := rawArgs.cook()
			if err != nil {
				glcm.Error(err.Error())
			}

			err = cookedArgs.process()
			if err != nil {
				glcm.Error(err.Error())
			}

			glcm.Exit(func(format OutputFormat) string {
				return "Successfully created the resource."
			}, EExitCode.Success())
		},
	}

	makeCmd.PersistentFlags().Uint32Var(&rawArgs.quota, "quota-gb", 0, "Specifies the maximum size of the share in gigabytes (GiB), "+
		"\n 0 means you accept the file service's default quota.")
	rootCmd.AddCommand(makeCmd)
}
