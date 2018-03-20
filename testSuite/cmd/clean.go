package cmd

import (
	"github.com/spf13/cobra"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"os"
	"context"
)

// initializes the clean command, its aliases and description.
func init(){
	resourceUrl := ""
	isResourceAContainer := true

	cleanCmd := &cobra.Command{
		Use:    "clean",
		Aliases: []string{"clean",},
		Short:   "clean deletes everything inside the container.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for clean command")
			}
			resourceUrl = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if isResourceAContainer{
				cleanContainer(resourceUrl)
			}else{
				cleanBlob(resourceUrl)
			}
		},
	}
	rootCmd.AddCommand(cleanCmd)
}

func cleanContainer(container string){

	containerSas, err := url.Parse(container)

	if err != nil{
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
			if err != nil{
				fmt.Println("error deleting the blob from container ", blobInfo.Name)
				os.Exit(1)
			}
		}
		marker = listBlob.NextMarker
	}
}

func cleanBlob(blob string){
	blobSas, err := url.Parse(blob)

	if err != nil{
		fmt.Println("error parsing the container sas ", blob)
		os.Exit(1)
	}

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	blobUrl := azblob.NewBlobURL(*blobSas, p)

	_, err = blobUrl.Delete(context.Background(), "include", azblob.BlobAccessConditions{})
	if err != nil{
		fmt.Println("error deleting the blob ", blob)
		os.Exit(1)
	}
}