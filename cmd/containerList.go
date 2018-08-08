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
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/spf13/cobra"
)

func init() {
	var sourcePath = ""
	var outputRaw = ""
	// listContainerCmd represents the list container command
	// listContainer list the blobs inside the container or virtual directory inside the container
	listContainerCmd := &cobra.Command{
		Use:     "list [containerURL]",
		Aliases: []string{"ls"},
		Short:   "List the blobs in a given container.",
		Long:    `List the blobs in a given container.`,
		Example: "azcopy container list [containerURL]",
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
				glcm.Exit("invalid path passed for listing. given source is of type "+location.String()+" while expect is container / container path ", common.EExitCode.Error())
			}

			var output common.OutputFormat
			output.Parse(outputRaw)
			err := HandleListContainerCommand(sourcePath, output)
			if err == nil {
				glcm.Exit("", common.EExitCode.Success())
			} else {
				glcm.Exit(err.Error(), common.EExitCode.Error())
			}

		},
	}
	containerCmd.AddCommand(listContainerCmd)
	listContainerCmd.PersistentFlags().StringVar(&outputRaw, "outputRaw", "text", "format of the command's outputRaw, the choices include: text, json")
}

// handles the list container command
func HandleListContainerCommand(source string, outputFormat common.OutputFormat) error {

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
		for _, blobInfo := range listBlob.Segment.BlobItems {
			blobName := blobInfo.Name
			if len(searchPrefix) > 0 {
				// strip away search prefix from the blob name.
				blobName = strings.Replace(blobName, searchPrefix, "", 1)
			}
			summary.Blobs = append(summary.Blobs, blobName)
		}
		marker = listBlob.NextMarker
		printListContainerResponse(&summary, outputFormat)
	}
	return nil
}

// printListContainerResponse prints the list container response
func printListContainerResponse(lsResponse *common.ListContainerResponse, outputFormat common.OutputFormat) {
	if len(lsResponse.Blobs) == 0 {
		return
	}
	if outputFormat == common.EOutputFormat.Json() {
		marshaledData, err := json.MarshalIndent(lsResponse, "", " ")
		if err != nil {
			panic(fmt.Errorf("error listing the source. Failed with error %s", err))
		}
		glcm.Info(string(marshaledData))
	} else {
		for index := 0; index < len(lsResponse.Blobs); index++ {
			glcm.Info(lsResponse.Blobs[index])
		}
	}
	lsResponse.Blobs = nil
}
