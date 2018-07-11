package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/spf13/cobra"
)

// initializes the clean command, its aliases and description.
func init() {
	resourceUrl := ""
	resourceType := ""
	blobType := "blob"
	fileType := "file"
	isResourceABucket := true

	cleanCmd := &cobra.Command{
		Use:     "clean",
		Aliases: []string{"clean"},
		Short:   "clean deletes everything inside the container.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for clean command")
			}
			resourceUrl = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("clean get resourceType", resourceType)
			if resourceType != blobType && resourceType != fileType {
				panic(fmt.Errorf("illegal resourceType '%s'", resourceType))
			}

			switch resourceType {
			case blobType:
				if isResourceABucket {
					cleanContainer(resourceUrl)
				} else {
					cleanBlob(resourceUrl)
				}
			case fileType:
				if isResourceABucket {
					cleanShare(resourceUrl)
				} else {
					cleanFile(resourceUrl)
				}
			}

		},
	}
	rootCmd.AddCommand(cleanCmd)

	cleanCmd.PersistentFlags().StringVar(&resourceType, "resourceType", "blob", "Resource type, could be blob or file currently.")
}

func cleanContainer(container string) {

	containerSas, err := url.Parse(container)

	if err != nil {
		fmt.Println("error parsing the container sas ", container)
		os.Exit(1)
	}

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	containerUrl := azblob.NewContainerURL(*containerSas, p)

	// perform a list blob
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobs(context.Background(), marker, azblob.ListBlobsOptions{})
		if err != nil {
			fmt.Println("error listing blobs inside the container. Please check the container sas")
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			_, err := containerUrl.NewBlobURL(blobInfo.Name).Delete(context.Background(), "include", azblob.BlobAccessConditions{})
			if err != nil {
				fmt.Println("error deleting the blob from container ", blobInfo.Name)
				os.Exit(1)
			}
		}
		marker = listBlob.NextMarker
	}
}

func cleanBlob(blob string) {
	blobSas, err := url.Parse(blob)

	if err != nil {
		fmt.Println("error parsing the container sas ", blob)
		os.Exit(1)
	}

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	blobUrl := azblob.NewBlobURL(*blobSas, p)

	_, err = blobUrl.Delete(context.Background(), "include", azblob.BlobAccessConditions{})
	if err != nil {
		fmt.Println("error deleting the blob ", blob)
		os.Exit(1)
	}
}

func cleanShare(shareURLStr string) {

	u, err := url.Parse(shareURLStr)

	if err != nil {
		fmt.Println("error parsing the share URL with SAS ", shareURLStr)
		os.Exit(1)
	}

	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	shareURL := azfile.NewShareURL(*u, p)

	_, err = shareURL.Delete(context.Background(), azfile.DeleteSnapshotsOptionInclude)
	if err != nil {
		sErr := err.(azfile.StorageError)
		if sErr != nil && sErr.Response().StatusCode != http.StatusNotFound {
			fmt.Fprintf(os.Stdout, "error deleting the share for clean share '%s', error '%v'\n", shareURL, err)
			os.Exit(1)
		}
	}

	// Sleep seconds to wait the share deletion got succeeded
	time.Sleep(30 * time.Second)

	_, err = shareURL.Create(context.Background(), azfile.Metadata{}, 0)
	if err != nil {
		fmt.Fprintf(os.Stdout, "error creating the share for clean share '%s', error '%v'\n", shareURL, err)
		os.Exit(1)
	}
}

func cleanFile(fileURLStr string) {
	u, err := url.Parse(fileURLStr)

	if err != nil {
		fmt.Println("error parsing the file URL with SAS ", fileURLStr)
		os.Exit(1)
	}

	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	fileURL := azfile.NewFileURL(*u, p)

	_, err = fileURL.Delete(context.Background())
	if err != nil {
		fmt.Println("error deleting the file ", fileURL)
		os.Exit(1)
	}
}
