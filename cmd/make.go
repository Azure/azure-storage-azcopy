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

	"errors"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
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
		resourceLocation: inferArgumentLocation(raw.resourceToCreate),
		quota:            int32(raw.quota),
	}, nil
}

// holds processed/actionable args
type cookedMakeCmdArgs struct {
	resourceURL      url.URL
	resourceLocation common.Location
	quota            int32 // quota is in GB
}

// getCredentialType gets the proper credential type for make command.
func (cma cookedMakeCmdArgs) getCredentialType(ctx context.Context) (credentialType common.CredentialType, err error) {
	credentialType = common.ECredentialType.Unknown()

	switch cma.resourceLocation {
	case common.ELocation.BlobFS():
		if credentialType, err = getBlobFSCredentialType(); err != nil {
			return common.ECredentialType.Unknown(), err
		}
	case common.ELocation.Blob():
		// The resource URL cannot be public access URL, as it need delete permission.
		credentialType, err = getBlobCredentialType(ctx, cma.resourceURL.String(), false, false)
		if err != nil {
			return common.ECredentialType.Unknown(), err
		}
	case common.ELocation.File():
		return common.ECredentialType.Anonymous(), nil
	default:
		credentialType = common.ECredentialType.Anonymous()
		glcm.Info(fmt.Sprintf("Use anonymous credential by default for location '%v'", cma.resourceLocation))
	}

	return credentialType, nil
}

func (cookedArgs cookedMakeCmdArgs) process() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	credentialInfo := common.CredentialInfo{}
	if credentialInfo.CredentialType, err = cookedArgs.getCredentialType(ctx); err != nil {
		return err
	} else if credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// Message user that they are using Oauth token for authentication,
		// in case of silently using cached token without consciousness。
		glcm.Info("Make is using OAuth token for authentication.")

		uotm := GetUserOAuthTokenManagerInstance()
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	switch cookedArgs.resourceLocation {
	case common.ELocation.BlobFS():
		p, err := createBlobFSPipeline(ctx, credentialInfo)
		if err != nil {
			return err
		}
		// here we assume the resourceURL is a proper file system URL
		fsURL := azbfs.NewFileSystemURL(cookedArgs.resourceURL, p)
		if _, err = fsURL.Create(ctx); err != nil {
			// print a nicer error message if file system already exists
			if storageErr, ok := err.(azbfs.StorageError); ok {
				if storageErr.ServiceCode() == azbfs.ServiceCodeFileSystemAlreadyExists {
					return fmt.Errorf("the file system already exists")
				} else if storageErr.ServiceCode() == azbfs.ServiceCodeResourceNotFound {
					return fmt.Errorf("please specify a valid file system URL with corresponding credentials")
				}
			}

			// print the ugly error if unexpected
			return err
		}
	case common.ELocation.Blob():
		p, err := createBlobPipeline(ctx, credentialInfo)
		if err != nil {
			return err
		}
		containerURL := azblob.NewContainerURL(cookedArgs.resourceURL, p)
		if _, err = containerURL.Create(ctx, nil, azblob.PublicAccessNone); err != nil {
			// print a nicer error message if container already exists
			if storageErr, ok := err.(azblob.StorageError); ok {
				if storageErr.ServiceCode() == azblob.ServiceCodeContainerAlreadyExists {
					return fmt.Errorf("the container already exists")
				} else if storageErr.ServiceCode() == azblob.ServiceCodeResourceNotFound {
					return fmt.Errorf("please specify a valid container URL with account SAS")
				}
			}

			// print the ugly error if unexpected
			return err
		}
	case common.ELocation.File():
		p, err := createFilePipeline(ctx, credentialInfo)
		if err != nil {
			return err
		}
		shareURL := azfile.NewShareURL(cookedArgs.resourceURL, p)
		if _, err = shareURL.Create(ctx, nil, cookedArgs.quota); err != nil {
			// print a nicer error message if share already exists
			if storageErr, ok := err.(azfile.StorageError); ok {
				if storageErr.ServiceCode() == azfile.ServiceCodeShareAlreadyExists {
					return fmt.Errorf("the file share already exists")
				} else if storageErr.ServiceCode() == azfile.ServiceCodeResourceNotFound {
					return fmt.Errorf("please specify a valid share URL with account SAS")
				}
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
				glcm.Exit(err.Error(), common.EExitCode.Error())
			}

			err = cookedArgs.process()
			if err != nil {
				glcm.Exit(err.Error(), common.EExitCode.Error())
			}

			glcm.Exit("Successfully created the resource.", common.EExitCode.Success())
		},
	}

	makeCmd.PersistentFlags().Uint32Var(&rawArgs.quota, "quota", 0, "specifies the maximum size of the share in gigabytes, 0 means you accept the file service's default quota.")
	rootCmd.AddCommand(makeCmd)
}
