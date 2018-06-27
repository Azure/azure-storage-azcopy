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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/spf13/cobra"
	"os"
	"net/url"
	"time"
	"context"
	"strings"
)

// make related pipeline params
const makeMaxTries = 5
const makeTryTimeout = time.Minute * 1
const makeRetryDelay = time.Second * 1
const makeMaxRetryDelay = time.Second * 3

// holds raw input from user
type rawMakeCmdArgs struct {
	resourceToCreate string
}

// parse raw input
func (raw rawMakeCmdArgs) cook() (cookedMakeCmdArgs, error) {
	parsedURL, err := url.Parse(raw.resourceToCreate)
	if err != nil {
		return cookedMakeCmdArgs{}, err
	}

	if strings.Count(parsedURL.Path, "/") > 1 {
		return cookedMakeCmdArgs{}, fmt.Errorf("please provide a valid top-level(ex: File System) resource URL")
	}

	// resourceLocation could be unknown at this stage, it will be handled by the caller
	return cookedMakeCmdArgs{
		resourceURL: *parsedURL,
		resourceLocation: inferArgumentLocation(raw.resourceToCreate),
	}, nil
}

// holds processed/actionable args
type cookedMakeCmdArgs struct {
	resourceURL url.URL
	resourceLocation Location
}

func (cookedArgs cookedMakeCmdArgs) process() error {
	switch cookedArgs.resourceLocation {
	case ELocation.BlobFS():
		// get the Account Name and Key variables from environment
		name := os.Getenv("ACCOUNT_NAME")
		key := os.Getenv("ACCOUNT_KEY")
		if name == "" || key == "" {
			return fmt.Errorf("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before creating the file system")
		}

		// here we assume the resourceURL is a proper file system URL
		fsURL := azbfs.NewFileSystemURL(cookedArgs.resourceURL, azbfs.NewPipeline(azbfs.NewSharedKeyCredential(name, key),
			azbfs.PipelineOptions{
				Retry: azbfs.RetryOptions{
					Policy:        azbfs.RetryPolicyExponential,
					MaxTries:      makeMaxTries,
					TryTimeout:    makeTryTimeout,
					RetryDelay:    makeRetryDelay,
					MaxRetryDelay: makeMaxRetryDelay,
				},
				Telemetry: azbfs.TelemetryOptions{
					Value: common.UserAgent,
				},
			}))

		_, err := fsURL.Create(context.Background())
		if err != nil {
			// print a nicer error message if file system already exists
			if storageErr,ok := err.(azbfs.StorageError); ok {
				if storageErr.ServiceCode() == azbfs.ServiceCodeFileSystemAlreadyExists {
					return fmt.Errorf("the file system already exists")
				}
			}

			// print the ugly error if unexpected
			return err
		}

		return nil
	default:
		return fmt.Errorf("operation not supported, cannot create resource %s type at the moment", cookedArgs.resourceURL.String())
	}
}

func init() {
	rawArgs := rawMakeCmdArgs{}

	// makeCmd represents the mkdir command, but targets the service side
	makeCmd := &cobra.Command{
		Use:        "make [resourceURL]",
		Aliases:    []string{"mk", "mkdir"},
		SuggestFor: []string{"mak", "makeCmd"},
		Short:      "Create a File System on the Azure Data Lake Storage Gen2 service",
		Long: `
Create the File System represented by the given resource URL.
`,
		Example: `
  - azcopy make "https://[account-name].dfs.core.windows.net/[filesystem-name]"
`,
		Args: func(cmd *cobra.Command, args []string) error {
			// verify that there is exactly one argument
			if len(args) != 1 {
				fmt.Println("please provide the resource URL as the only argument")
				os.Exit(1)
			}

			rawArgs.resourceToCreate = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string)  {
			// TODO update after output mechanism is in place
			cookedArgs, err := rawArgs.cook()
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}

			// TODO update after output mechanism is in place
			err = cookedArgs.process()
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}

			// TODO update after output mechanism is in place
			fmt.Printf("Successfully created the File System %s.\n", cookedArgs.resourceURL.String())
		},
	}

	rootCmd.AddCommand(makeCmd)
}
