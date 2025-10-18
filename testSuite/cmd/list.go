package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

// initializes the clean command, its aliases and description.
func init() {
	resourceUrl := ""
	numberOfResource := int64(0)

	cleanCmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"list"},
		Short:   "lists list everything inside the container / virtual directory",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for clean command")
			}
			resourceUrl = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			listContainer(resourceUrl, numberOfResource)
		},
	}
	rootCmd.AddCommand(cleanCmd)

	cleanCmd.PersistentFlags().Int64Var(&numberOfResource, "resource-num", 0, "number of resource inside the container")
}

func getContainerURLFromString(url url.URL) string {
	containerName := strings.SplitAfterN(url.Path[1:], "/", 2)[0]
	url.Path = "/" + containerName
	return url.String()
}

// checks if a given url points to a container, as opposed to a blob or prefix match
func urlIsContainerOrShare(url *url.URL) bool {
	// if the path contains more than one "/", then it means it points to a blob, and not a container
	numOfSlashes := strings.Count(url.Path[1:], "/")

	if numOfSlashes == 0 {
		return true
	} else if numOfSlashes == 1 && url.Path[len(url.Path)-1:] == "/" { // this checks if container_name/ was given
		return true
	}
	return false
}

func getBlobNameFromURL(path string) string {
	// return everything after the second /
	return strings.SplitAfterN(path[1:], "/", 2)[1]
}

func listContainer(resourceUrl string, numberOfResources int64) {
	// attempt to parse the source url
	sourceUrl, err := url.Parse(resourceUrl)
	if err != nil {
		fmt.Println("cannot parse source URL")
		os.Exit(1)
	}

	// get the container url to be used for listing
	literalContainerUrl := getContainerURLFromString(*sourceUrl)
	cc, err := container.NewClientWithNoCredential(literalContainerUrl, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		Retry: policy.RetryOptions{
			MaxRetries:    ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
	}})
	if err != nil {
		fmt.Printf("cannot create container client. Failed with error %s\n", err.Error())
		os.Exit(1)
	}

	// get the search prefix to query the service
	searchPrefix := ""
	// if the source is container url, then searchPrefix is empty
	if !urlIsContainerOrShare(sourceUrl) {
		searchPrefix = getBlobNameFromURL(sourceUrl.Path)
	}
	if len(searchPrefix) > 0 {
		// if the user did not specify / at the end of the virtual directory, add it before doing the prefix search
		if strings.LastIndex(searchPrefix, "/") != len(searchPrefix)-1 {
			searchPrefix += "/"
		}
	}
	numberOfBlobs := int64(0)

	// perform a list blob
	// look for all blobs that start with the prefix
	pager := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &searchPrefix})

	for pager.More() {
		listBlob, err := pager.NextPage(context.TODO())
		if err != nil {
			fmt.Printf("cannot list blobs for download. Failed with error %s\n", err.Error())
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			blobName := *blobInfo.Name
			if len(searchPrefix) > 0 {
				// strip away search prefix from the blob name.
				blobName = strings.Replace(blobName, searchPrefix, "", 1) //nolint:ineffassign,staticcheck
			}
			numberOfBlobs++
		}
	}

	if numberOfBlobs != numberOfResources {
		fmt.Printf("expected number of blobs / file %d inside the resource does not match the actual %d\n", numberOfResources, numberOfBlobs)
		os.Exit(1)
	}
}
