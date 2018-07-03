package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/spf13/cobra"
)

// initializes the clean command, its aliases and description.
func init() {
	resourceURL := ""
	resourceType := ""
	blobType := "blob"
	fileType := "file"
	blobFSType := "blobFS"
	isResourceABucket := true

	cleanCmd := &cobra.Command{
		Use:     "clean",
		Aliases: []string{"clean"},
		Short:   "clean deletes everything inside the container.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for clean command")
			}
			resourceURL = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			switch resourceType {
			case blobType:
				if isResourceABucket {
					cleanContainer(resourceURL)
				} else {
					cleanBlob(resourceURL)
				}
			case fileType:
				if isResourceABucket {
					cleanShare(resourceURL)
				} else {
					cleanFile(resourceURL)
				}
			case blobFSType:
				if isResourceABucket {
					cleanFileSystem(resourceURL)
				} else {
					cleanBfsFile(resourceURL)
				}
			default:
				panic(fmt.Errorf("illegal resourceType %q", resourceType))
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
	time.Sleep(45 * time.Second)

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

func createBlobFSPipeline() pipeline.Pipeline {
	// Get the Account Name and Key variables from environment
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if name == "" || key == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set before cleaning the file system")
		os.Exit(1)
	}
	// create the blob fs pipeline
	c := azbfs.NewSharedKeyCredential(name, key)
	return azbfs.NewPipeline(c, azbfs.PipelineOptions{})
}

func cleanFileSystem(fsURLStr string) {
	ctx := context.Background()
	u, err := url.Parse(fsURLStr)

	if err != nil {
		fmt.Println("error parsing the file system URL ", fsURLStr)
		os.Exit(1)
	}

	fsURL := azbfs.NewFileSystemURL(*u, createBlobFSPipeline())
	_, err = fsURL.Delete(ctx)
	if err != nil {
		sErr := err.(azbfs.StorageError)
		if sErr != nil && sErr.Response().StatusCode != http.StatusNotFound {
			fmt.Println(fmt.Sprintf("error deleting the file system %q for cleaning, %v", fsURLStr, err))
			os.Exit(1)
		}
	}

	// Sleep seconds to wait the share deletion got succeeded
	time.Sleep(45 * time.Second)

	_, err = fsURL.Create(ctx)
	if err != nil {
		fmt.Println(fmt.Fprintf(os.Stdout, "error creating the file system %q for cleaning, %v", fsURLStr, err))
		os.Exit(1)
	}
}

func cleanBfsFile(fileURLStr string) {
	ctx := context.Background()
	u, err := url.Parse(fileURLStr)

	if err != nil {
		fmt.Println("error parsing the file system URL ", fileURLStr)
		os.Exit(1)
	}

	fileURL := azbfs.NewFileURL(*u, createBlobFSPipeline())
	_, err = fileURL.Delete(ctx)
	if err != nil {
		fmt.Println(fmt.Sprintf("error deleting the blob FS file %s, %v", fileURLStr, err))
		os.Exit(1)
	}
}
