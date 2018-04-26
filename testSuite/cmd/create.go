package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/spf13/cobra"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"io/ioutil"
	"io"
	"math/rand"
	"strings"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func createStringWithRandomChars(length int) string{
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Int() % len(charset)]
	}
	return string(b)
}

// initializes the create command, its aliases and description.
func init() {
	resourceURL := ""
	resourceType := ""
	blobType := "blob"
	fileType := "file"
	isResourceABucket := true

	createCmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"create"},
		Short:   "create deletes everything inside the container.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for create command")
			}
			resourceURL = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("create get resourceType", resourceType)
			if resourceType != blobType && resourceType != fileType {
				panic(fmt.Errorf("illegal resourceType '%s'", resourceType))
			}

			switch resourceType {
			case blobType:
				if isResourceABucket {
					createContainer(resourceURL)
				} else {
					createBlob(resourceURL)
				}
			case fileType:
				if isResourceABucket {
					createShareOrDirectory(resourceURL)
				} else {
					createFile(resourceURL)
				}
			}

		},
	}
	rootCmd.AddCommand(createCmd)

	createCmd.PersistentFlags().StringVar(&resourceType, "resourceType", "blob", "Resource type, could be blob or file currently.")
	createCmd.PersistentFlags().BoolVar(&isResourceABucket, "isResourceABucket", true, "Whether resource is a bucket, if it's bucket, for blob it's container, and for file it's share or directory.")
}

// Can be used for overwrite scenarios.
func createContainer(container string) {
	panic("todo")
}

func createBlob(blobUri string) {
	url, err := url.Parse(blobUri)
	if err != nil{
		fmt.Println("error parsing the blob sas ", blobUri)
		os.Exit(1)
	}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	blobUrl := azblob.NewBlockBlobURL(*url, p)

	randomString := createStringWithRandomChars(1024)

	putBlobResp, err := blobUrl.PutBlob(context.Background(), strings.NewReader(randomString), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil{
		fmt.Println(fmt.Sprintf("error uploading the blob %s", blobUrl))
		os.Exit(1)
	}
	if putBlobResp.Response() != nil{
		io.Copy(ioutil.Discard, putBlobResp.Response().Body)
		putBlobResp.Response().Body.Close()
	}
}

func createShareOrDirectory(shareOrDirectoryURLStr string) {
	fmt.Println("createShareOrDirectory with URL: ", shareOrDirectoryURLStr)

	u, err := url.Parse(shareOrDirectoryURLStr)

	if err != nil {
		fmt.Println("error parsing the share or directory URL with SAS ", shareOrDirectoryURLStr)
		os.Exit(1)
	}

	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})

	// Suppose it's a directory or share, try create and doesn't care if error happened.
	dirURL := azfile.NewDirectoryURL(*u, p)
	_, _ = dirURL.Create(context.Background(), azfile.Metadata{})

	shareURL := azfile.NewShareURL(*u, p)
	_, _ = shareURL.Create(context.Background(), azfile.Metadata{}, 0)

	// Finally valdiate if directory with specified URL exists, if doesn't exist, then report create failure.
	time.Sleep(1 * time.Second)

	_, err = dirURL.GetProperties(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stdout, "error createShareOrDirectory with URL '%s', error '%v'\n", shareOrDirectoryURLStr, err)
		os.Exit(1)
	}
}

func createFile(fileURLStr string) {
	panic("todo")
}
