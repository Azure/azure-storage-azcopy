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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/spf13/cobra"
	"net/url"
	"strings"
)

func init() {
	var sourcePath = ""
	var jsonOutput = false
	// listContainerCmd represents the list container command
	// listContainer list the blobs inside the container or virtual directory inside the container
	listContainerCmd := &cobra.Command{
		Use:        "listContainer",
		Aliases:    []string{"lsc"},
		SuggestFor: []string{"lstcontainer", "listcntainer", "licontaier"},
		Short:      "resume resumes the existing job for given JobId.",
		Long:       `resume resumes the existing job for given JobId.`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the listContainer command requires necessarily to have an argument

			// If no argument is passed then it is not valid
			// lsc expects the container path / virtual directory
			if len(args) == 0 || len(args) > 2 {
				return errors.New("this command only requires container destination")
			}
			sourcePath = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// the expected argument in input is the container sas / or path of virtual directory in the container.
			// verifying the location type
			location := inferArgumentLocation(sourcePath)
			if location != location.Blob() {
				glcm.ExitWithError("invalid path passed for listing. given source is of type "+location.String()+" while expect is container / container path ", common.EExitCode.Error())
			}

			err := HandleListContainerCommand(sourcePath, jsonOutput)
			if err == nil {
				glcm.ExitWithSuccess("", common.EExitCode.Success())
			} else {
				glcm.ExitWithError(err.Error(), common.EExitCode.Error())
			}

		},
		// hide features not relevant to BFS
		// TODO remove after preview release
		Hidden: true,
	}
	rootCmd.AddCommand(listContainerCmd)
	listContainerCmd.PersistentFlags().BoolVar(&jsonOutput, "output-json", false, "true if user wants the output in Json format")
}

// handles the list container command
func HandleListContainerCommand(source string, jsonOutput bool) error {

	util := copyHandlerUtil{}
	// Create Pipeline which will be used further in the blob operations.
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil)

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// attempt to parse the source url
	sourceUrl, err := url.Parse(source)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// get the container url to be used for listing
	literalContainerUrl := util.getContainerURLFromString(*sourceUrl)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	// get the search prefix to query the service
	searchPrefix := ""
	// if the source is container url, then searchPrefix is empty
	if !util.urlIsContainerOrShare(sourceUrl) {
		searchPrefix = util.getBlobNameFromURL(sourceUrl.Path)
	}
	if len(searchPrefix) > 0 {
		// if the user did not specify / at the end of the virtual directory, add it before doing the prefix search
		if strings.LastIndex(searchPrefix, "/") != len(searchPrefix)-1 {
			searchPrefix += "/"
		}
	}

	summary := common.ListContainerResponse{}

	// perform a list blob
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerUrl.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			blobName := blobInfo.Name
			if len(searchPrefix) > 0 {
				// strip away search prefix from the blob name.
				blobName = strings.Replace(blobName, searchPrefix, "", 1)
			}
			summary.Blobs = append(summary.Blobs, blobName)
		}
		marker = listBlob.NextMarker
		printListContainerResponse(&summary, jsonOutput)
	}
	return nil
}

// printListContainerResponse prints the list container response
// If the output-json flag is set to true, it prints the output in json format.
func printListContainerResponse(lsResponse *common.ListContainerResponse, jsonOutput bool) {
	if len(lsResponse.Blobs) == 0 {
		return
	}
	if jsonOutput {
		marshalledData, err := json.MarshalIndent(lsResponse, "", " ")
		if err != nil {
			panic(fmt.Errorf("error listing the source. Failed with error %s", err))
		}
		glcm.Info(string(marshalledData))
	} else {
		for index := 0; index < len(lsResponse.Blobs); index++ {
			glcm.Info(lsResponse.Blobs[index])
		}
	}
	lsResponse.Blobs = nil
}
