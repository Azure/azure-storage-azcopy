package cmd

import (
	"github.com/spf13/cobra"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"os"
	"context"
	"crypto/md5"
)

type TestBlobCommand struct{
	Source string
	Destination string
	CheckProperties bool
	MetaData     string
	ContentType  string
	ContentEncoding string
}

func init(){
	cmdInput := TestBlobCommand{}
	testBlobCmd := &cobra.Command{
		Use:    "testBlob",
		Aliases: []string{"tBlob",},
		Short:   "tests the blob created using AZCopy v2",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("invalid arguments for test blob command")
			}
			cmdInput.Source = args[0]
			cmdInput.Destination = args[1]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			verifyBlob(cmdInput)
		},
	}
	rootCmd.AddCommand(testBlobCmd)
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.CheckProperties, "check-properties", false, "use this flag to test the properties of blob uploaded")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.MetaData, "metadata", "", "metadata expected from the blob in the container")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentType, "contentType", "", "content type expected from the blob in the container")
}

func verifyBlob(testBlobCmd TestBlobCommand){
	sourceURL, err := url.Parse(testBlobCmd.Source)
	if err != nil{
		fmt.Println(fmt.Sprintf("Error parsing the blob url source %s", testBlobCmd.Source))
		os.Exit(1)
	}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	blobUrl := azblob.NewBlobURL(*sourceURL, p)
	blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
	if err != nil{
		fmt.Println("unable to get blob properties ", blobProperties)
		os.Exit(1)
	}

	// check whether destination provided is valid or not.
	fileInfo, err := os.Stat(testBlobCmd.Destination)
	if err != nil{
		fmt.Println("error opening the destination blob on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(testBlobCmd.Destination)
	if err != nil{
		fmt.Println("error opening the file ", testBlobCmd.Destination)
	}
	fmt.Println("destination file size ", fileInfo.Size())
	mmap, err := Map(file, false, 0, int(fileInfo.Size()))
	if err != nil{
		fmt.Println("error mapping the destination blob file ", err.Error())
		os.Exit(1)
	}
	actualMd5 := md5.Sum(mmap)
	expectedMd5 := blobProperties.ContentMD5()
	fmt.Println("actual md5 ", (actualMd5))
	if actualMd5 != expectedMd5 {
		fmt.Println("the uploaded blob's md5 doesn't matches the actual blob's md5")
		os.Exit(1)
	}
}